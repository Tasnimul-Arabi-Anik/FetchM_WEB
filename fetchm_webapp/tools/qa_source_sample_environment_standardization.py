#!/usr/bin/env python3
"""Audit source, sample, environment, site, and disease fields on canonical data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect


DEFAULT_OUTPUT_ROOT = ROOT / "data" / "source_sample_environment_qa"
CONTROLLED_CATEGORIES = ROOT / "standardization" / "controlled_categories.csv"
APPROVED_BROAD_CATEGORIES = ROOT / "standardization" / "approved_broad_categories.csv"
MIN_STANDARDIZATION_PERCENT = 80.0
MISSING = {
    "", "-", "--", "absent", "missing", "n/a", "na", "none", "not applicable",
    "not available", "not collected", "not provided", "null", "unknown",
}
MISSING_SQL = ", ".join("'" + value.replace("'", "''") + "'" for value in sorted(MISSING))

FIELD_FILES = {
    "Isolation_Source_SD": "isolation_source_counts.csv",
    "Isolation_Source_SD_Broad": "isolation_source_broad_counts.csv",
    "Sample_Type_SD": "sample_type_counts.csv",
    "Environment_Medium_SD": "environment_medium_counts.csv",
    "Environment_Broad_Scale_SD": "environment_broad_scale_counts.csv",
    "Environment_Local_Scale_SD": "environment_local_scale_counts.csv",
    "Isolation_Site_SD": "isolation_site_counts.csv",
    "Host_Disease_SD": "host_disease_counts.csv",
    "Host_Health_State_SD": "host_health_state_counts.csv",
}

BODY_SITE_PATTERN = re.compile(
    r"\b(?:groin|nasal|nose|nasopharyn|oropharyn|throat|rectum|rectal|perianal|skin|"
    r"lung|bronch|pleur|gut|intestin|colon|ileum|stomach|rumen|oral|mouth|dental|"
    r"vagina|vaginal|cervix|urethra|bladder|wound|ear|eye|conjunctiva|canal)\b",
    re.I,
)
SAMPLE_PATTERN = re.compile(
    r"\b(?:blood|urine|sputum|stool|feces|faeces|swab|tissue|milk|pus|aspirate|"
    r"biopsy|csf|cerebrospinal|body fluid|lavage|serum|plasma|saliva|sample)\b",
    re.I,
)
ENVIRONMENT_PATTERN = re.compile(
    r"\b(?:soil|water|sediment|wastewater|sewage|seawater|sludge|biofilm|air|"
    r"river|lake|pond|groundwater|hot spring|environment)\b",
    re.I,
)
DISEASE_PATTERN = re.compile(
    r"\b(?:disease|diseased|infection|infected|pneumonia|sepsis|osteomyelitis|"
    r"leukemia|cancer|diarrhea|diarrhoea|mastitis|abscess|cystic fibrosis)\b",
    re.I,
)
HOST_PATTERN = re.compile(
    r"\b(?:human|patient|chicken|cattle|cow|swine|pig|animal|oyster|fish|plant|"
    r"mouse|rat|dog|cat|poultry|turkey)\b",
    re.I,
)
METADATA_PATTERN = re.compile(
    r"(?:^|\b)(?:sample|other|unknown|not applicable|pathogen\.?cl|metadata "
    r"descriptor|non-source|whole organism|metagenome|assembly)(?:\b|$)",
    re.I,
)
RAW_CODE_PATTERN = re.compile(
    r"(?:#REF!|^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9._-]{4,24}$|^[A-Z]{2,8}[_-]\d+$)",
    re.I,
)
FOOD_PATTERN = re.compile(
    r"\b(?:food|meat|oyster|chicken|beef|pork|swine|cattle|milk|cheese|seafood|"
    r"turkey|produce)\b",
    re.I,
)


def present_sql(expression: str) -> str:
    return (
        f"NULLIF(BTRIM(COALESCE({expression}, '')), '') IS NOT NULL "
        f"AND LOWER(BTRIM(COALESCE({expression}, ''))) NOT IN ({MISSING_SQL})"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() or "unknown"


def percent(count: int, total: int) -> float:
    return round(100.0 * count / total, 2) if total else 0.0


def write_csv(path: Path, header: list[str], rows: Iterable[Iterable[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def latest_snapshot_id() -> str:
    with connect() as connection:
        row = connection.execute(
            """
            SELECT snapshot_id
            FROM bacterial_inventory_snapshot
            WHERE status = 'completed'
            ORDER BY completed_at DESC NULLS LAST, requested_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("No completed canonical bacterial snapshot is available.")
    return str(row[0])


def grouped_values(connection: Any, snapshot_id: str, field: str) -> list[tuple[str, int]]:
    expression = f"s.standardized_payload->>'{field}'"
    return [
        (str(value or ""), int(count or 0))
        for value, count in connection.execute(
            f"""
            SELECT {expression}, COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s USING (assembly_accession)
            WHERE i.snapshot_id = %s AND {present_sql(expression)}
            GROUP BY 1 ORDER BY 2 DESC, 1
            """,
            (snapshot_id,),
        ).fetchall()
    ]


def load_approved_broad_values() -> set[str]:
    with APPROVED_BROAD_CATEGORIES.open(newline="", encoding="utf-8") as handle:
        return {
            str(row.get("approved_value") or "").strip()
            for row in csv.DictReader(handle)
            if str(row.get("field") or "").strip() == "Isolation_Source_SD_Broad"
            and str(row.get("approved_value") or "").strip()
        }


def audit_controlled_rules() -> tuple[list[list[Any]], dict[str, int]]:
    rows: list[dict[str, str]]
    with CONTROLLED_CATEGORIES.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    approved = [
        row for row in rows
        if str(row.get("status") or "approved").strip().lower() in {"approved", "active"}
    ]
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in approved:
        key = (
            str(row.get("source_column") or "").strip().lower(),
            str(row.get("normalized_value") or row.get("synonym") or "").strip().lower(),
            str(row.get("destination") or "").strip(),
        )
        groups[key].append(row)
    audit_rows: list[list[Any]] = []
    duplicate_keys = 0
    conflict_keys = 0
    for key, entries in sorted(groups.items()):
        values = {
            str(row.get("proposed_value") or row.get("category") or "").strip()
            for row in entries
        }
        duplicate = len(entries) > 1
        conflict = len(values) > 1
        duplicate_keys += int(duplicate)
        conflict_keys += int(conflict)
        audit_rows.append([
            *key,
            len(entries),
            "|".join(sorted(values)),
            "true" if duplicate else "false",
            "true" if conflict else "false",
        ])
    return audit_rows, {
        "total_rows": len(rows),
        "approved_rows": len(approved),
        "duplicate_keys": duplicate_keys,
        "conflict_keys": conflict_keys,
    }


def classify_exact(value: str) -> tuple[str, list[str]]:
    classes: list[str] = []
    if BODY_SITE_PATTERN.search(value):
        classes.append("body_site")
    if SAMPLE_PATTERN.search(value):
        classes.append("sample_type")
    if ENVIRONMENT_PATTERN.search(value):
        classes.append("environment_medium")
    if DISEASE_PATTERN.search(value):
        classes.append("disease")
    if HOST_PATTERN.search(value):
        classes.append("host_taxon_or_context")
    if METADATA_PATTERN.search(value):
        classes.append("metadata_descriptor")
    if RAW_CODE_PATTERN.search(value):
        classes.append("raw_code_or_artifact")
    if not classes:
        return "keep_as_isolation_source", []
    if "metadata_descriptor" in classes or "raw_code_or_artifact" in classes:
        action = "route_to_non_source_descriptor"
    elif "disease" in classes:
        action = "route_to_disease_or_health_state"
    elif "body_site" in classes and "sample_type" not in classes:
        action = "route_to_isolation_site"
    elif "sample_type" in classes:
        action = "route_to_sample_type"
    elif "environment_medium" in classes:
        action = "route_to_environment_medium"
    elif FOOD_PATTERN.search(value):
        action = "route_to_food_source"
    elif "host_taxon_or_context" in classes:
        action = "route_to_host_context"
    else:
        action = "manual_review"
    return action, classes


def generate_qa(
    snapshot_id: str | None = None,
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_date: datetime | None = None,
) -> dict[str, Any]:
    snapshot_id = snapshot_id or latest_snapshot_id()
    run_date = run_date or datetime.now(timezone.utc)
    output_dir = output_root / run_date.strftime("%Y%m%d")
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_expr = "s.standardized_payload->>'Isolation Source'"
    source_expr = "s.standardized_payload->>'Isolation_Source_SD'"
    broad_expr = "s.standardized_payload->>'Isolation_Source_SD_Broad'"
    sample_expr = "s.standardized_payload->>'Sample_Type_SD'"
    medium_expr = "s.standardized_payload->>'Environment_Medium_SD'"
    site_expr = "s.standardized_payload->>'Isolation_Site_SD'"
    method_expr = "s.standardized_payload->>'Isolation_Source_SD_Method'"
    raw_present = present_sql(raw_expr)
    source_present = present_sql(source_expr)

    with connect() as connection:
        counts = connection.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE {raw_present}),
                COUNT(*) FILTER (WHERE {source_present}),
                COUNT(*) FILTER (WHERE {present_sql(broad_expr)}),
                COUNT(*) FILTER (WHERE NOT ({raw_present}) AND {source_present}),
                COUNT(*) FILTER (WHERE {raw_present} AND NOT ({source_present})),
                COUNT(*) FILTER (WHERE NOT ({raw_present}) AND NOT ({source_present}))
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s USING (assembly_accession)
            WHERE i.snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()
        field_counts = {
            field: grouped_values(connection, snapshot_id, field)
            for field in FIELD_FILES
        }
        unresolved_raw = [
            (str(value or ""), int(count or 0))
            for value, count in connection.execute(
                f"""
                SELECT {raw_expr}, COUNT(*)
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s USING (assembly_accession)
                WHERE i.snapshot_id = %s
                  AND {raw_present}
                  AND NOT ({source_present})
                GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1000
                """,
                (snapshot_id,),
            ).fetchall()
        ]
        unresolved_exact_routing = {
            str(action): int(count or 0)
            for action, count in connection.execute(
                f"""
                SELECT action, COUNT(*)
                FROM (
                    SELECT CASE
                        WHEN LOWER(BTRIM(COALESCE({source_expr}, ''))) IN
                             ('metagenome', 'metagenomic assembly', 'sample',
                              'metadata descriptor/non-source', 'metadata descriptor / non-source')
                            THEN 'route_to_non_source_descriptor'
                        WHEN {present_sql(source_expr)} AND {present_sql(sample_expr)}
                             AND LOWER(BTRIM({source_expr})) = LOWER(BTRIM({sample_expr}))
                             AND COALESCE({method_expr}, '') NOT IN (
                                 'sample_context_router',
                                 'food_source_context',
                                 'host_context_router'
                             )
                            THEN 'route_to_sample_type'
                        WHEN {present_sql(source_expr)} AND {present_sql(medium_expr)}
                             AND LOWER(BTRIM({source_expr})) = LOWER(BTRIM({medium_expr}))
                             AND COALESCE({method_expr}, '') NOT IN (
                                 'environment_context_router',
                                 'food_source_context',
                                 'host_context_router'
                             )
                            THEN 'route_to_environment_medium'
                        WHEN {present_sql(source_expr)} AND {present_sql(site_expr)}
                             AND LOWER(BTRIM({source_expr})) = LOWER(BTRIM({site_expr}))
                             AND COALESCE({method_expr}, '') NOT IN (
                                 'anatomy_source_router',
                                 'sample_context_router',
                                 'food_source_context',
                                 'host_context_router'
                             )
                            THEN 'route_to_isolation_site'
                        ELSE NULL
                    END AS action
                    FROM bacterial_inventory_membership AS i
                    JOIN assembly_standardization AS s USING (assembly_accession)
                    WHERE i.snapshot_id = %s
                ) AS routed
                WHERE action IS NOT NULL
                GROUP BY action
                """,
                (snapshot_id,),
            ).fetchall()
        }
        routing_method_counts = {
            str(method): int(count or 0)
            for method, count in connection.execute(
                f"""
                SELECT COALESCE({method_expr}, ''), COUNT(*)
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s USING (assembly_accession)
                WHERE i.snapshot_id = %s
                  AND COALESCE({method_expr}, '') IN (
                      'sample_context_router',
                      'sample_material_router',
                      'environment_context_router',
                      'anatomy_source_router',
                      'non_source_descriptor_router',
                      'food_source_context',
                      'host_context_router'
                  )
                GROUP BY 1
                """,
                (snapshot_id,),
            ).fetchall()
        }

    total = int(counts[0] or 0)
    raw_count = int(counts[1] or 0)
    source_count = int(counts[2] or 0)
    broad_count = int(counts[3] or 0)
    raw_and_standardized = raw_count - int(counts[5] or 0)

    approved_broad = load_approved_broad_values()
    invalid_broad = [
        [value, count]
        for value, count in field_counts["Isolation_Source_SD_Broad"]
        if value not in approved_broad
    ]

    classified: list[list[Any]] = []
    category_rows: dict[str, list[list[Any]]] = {
        "body_site": [],
        "sample_type": [],
        "environment_medium": [],
        "disease": [],
        "host_taxon_or_context": [],
        "metadata_descriptor": [],
        "generic": [],
        "raw_code_or_artifact": [],
    }
    for value, count in field_counts["Isolation_Source_SD"]:
        action, classes = classify_exact(value)
        if classes:
            classified.append([value, count, "|".join(classes), action])
        for category in classes:
            category_rows[category].append([value, count, action])
        if value.lower() in {"sample", "other", "unknown", "not applicable", "whole organism"}:
            category_rows["generic"].append([value, count, action])

    rule_rows, rule_metrics = audit_controlled_rules()
    metrics = {
        "total_rows_scanned": total,
        "file_errors": 0,
        "raw_isolation_source_present_count": raw_count,
        "raw_isolation_source_present_percent": percent(raw_count, total),
        "isolation_source_sd_present_count": source_count,
        "isolation_source_sd_present_percent": percent(source_count, total),
        "isolation_source_sd_broad_present_count": broad_count,
        "isolation_source_sd_broad_present_percent": percent(broad_count, total),
        "raw_present_isolation_source_standardization_percent": percent(raw_and_standardized, raw_count),
        "standardized_only_rescued_isolation_source_rows": int(counts[4] or 0),
        "raw_only_unresolved_isolation_source_rows": int(counts[5] or 0),
        "neither_raw_nor_standardized_isolation_source_rows": int(counts[6] or 0),
        "suspicious_exact_source_unique_values": len(classified),
        "suspicious_exact_source_rows": sum(int(row[1]) for row in classified),
        "hard_exact_leakage_rows": sum(unresolved_exact_routing.values()),
        "hard_exact_leakage_by_action": unresolved_exact_routing,
        "review_signal_exact_cross_field_unique_values": len(classified),
        "review_signal_exact_cross_field_rows": sum(int(row[1]) for row in classified),
        "intentional_cross_field_context_rows": sum(routing_method_counts.values()),
        "material_routed_to_sample_type_rows": (
            routing_method_counts.get("sample_context_router", 0)
            + routing_method_counts.get("sample_material_router", 0)
        ),
        "environment_medium_routed_rows": routing_method_counts.get("environment_context_router", 0),
        "site_routed_rows": routing_method_counts.get("anatomy_source_router", 0),
        "metadata_descriptor_suppressed_rows": routing_method_counts.get("non_source_descriptor_router", 0),
        "food_context_preserved_rows": routing_method_counts.get("food_source_context", 0),
        "host_context_preserved_rows": routing_method_counts.get("host_context_router", 0),
        "unresolved_exact_routing_rows": sum(unresolved_exact_routing.values()),
        "unresolved_exact_routing_by_action": unresolved_exact_routing,
        "non_approved_broad_unique_values": len(invalid_broad),
        "non_approved_broad_rows": sum(int(row[1]) for row in invalid_broad),
        "controlled_category_total_rows": rule_metrics["total_rows"],
        "controlled_category_approved_rows": rule_metrics["approved_rows"],
        "controlled_category_duplicate_keys": rule_metrics["duplicate_keys"],
        "controlled_category_conflict_keys": rule_metrics["conflict_keys"],
    }
    for field, rows in field_counts.items():
        key = re.sub(r"[^a-z0-9]+", "_", field.lower()).strip("_")
        metrics[f"{key}_present_count"] = sum(count for _value, count in rows)
        metrics[f"{key}_present_percent"] = percent(metrics[f"{key}_present_count"], total)

    artifacts = [
        "source_sample_environment_qa_summary.json",
        "source_sample_environment_qa_summary.md",
        "source_sample_environment_field_coverage.csv",
        *FIELD_FILES.values(),
        "exact_isolation_source_suspicious_values.csv",
        "isolation_source_body_site_leakage.csv",
        "isolation_source_sample_type_leakage.csv",
        "isolation_source_environment_medium_leakage.csv",
        "isolation_source_disease_leakage.csv",
        "isolation_source_host_taxon_leakage.csv",
        "isolation_source_metadata_descriptor_leakage.csv",
        "isolation_source_generic_values.csv",
        "isolation_source_raw_code_leakage.csv",
        "non_approved_isolation_source_broad_values.csv",
        "top_unresolved_raw_isolation_source_values.csv",
        "controlled_categories_rule_audit.csv",
        "source_sample_environment_rule_manifest.json",
    ]
    provenance = {
        "qa_timestamp": datetime.now(timezone.utc).isoformat(),
        "qa_commit": git_commit(),
        "snapshot_id": snapshot_id,
        "controlled_categories_file": str(CONTROLLED_CATEGORIES.relative_to(ROOT)),
        "controlled_categories_sha256": sha256_file(CONTROLLED_CATEGORIES),
        "approved_broad_categories_file": str(APPROVED_BROAD_CATEGORIES.relative_to(ROOT)),
        "approved_broad_categories_sha256": sha256_file(APPROVED_BROAD_CATEGORIES),
        "generated_artifacts": artifacts,
    }
    hard_checks = {
        "canonical rows scanned": 0 if total else 1,
        "file errors": metrics["file_errors"],
        "non-approved broad rows": metrics["non_approved_broad_rows"],
        "controlled-category duplicate keys": metrics["controlled_category_duplicate_keys"],
        "controlled-category conflict keys": metrics["controlled_category_conflict_keys"],
        "rule source provenance missing": int(
            not provenance["controlled_categories_sha256"]
            or not provenance["approved_broad_categories_sha256"]
        ),
        "raw-present standardization below threshold": int(
            metrics["raw_present_isolation_source_standardization_percent"]
            < MIN_STANDARDIZATION_PERCENT
        ),
        "hard exact-source leakage": metrics["hard_exact_leakage_rows"],
    }
    hard_failures = [f"{label}: {count}" for label, count in hard_checks.items() if count]
    summary = {
        "status": "fail" if hard_failures else "pass",
        "metrics": metrics,
        "provenance": provenance,
        "hard_failures": hard_failures,
        "review_note": (
            "Review-signal classifications are vocabulary triage, not error counts. Hard exact "
            "leakage is limited to metadata descriptors or exact source values duplicated in a "
            "dedicated sample, environment-medium, or isolation-site field. Successful routing "
            "is counted separately from intentional source context."
        ),
    }

    for field, filename in FIELD_FILES.items():
        write_csv(output_dir / filename, [field.lower(), "assembly_count"], field_counts[field])
    write_csv(
        output_dir / "source_sample_environment_field_coverage.csv",
        ["field", "present_count", "present_percent"],
        [
            [
                field,
                metrics[f"{re.sub(r'[^a-z0-9]+', '_', field.lower()).strip('_')}_present_count"],
                metrics[f"{re.sub(r'[^a-z0-9]+', '_', field.lower()).strip('_')}_present_percent"],
            ]
            for field in FIELD_FILES
        ],
    )
    write_csv(
        output_dir / "exact_isolation_source_suspicious_values.csv",
        ["isolation_source_sd", "assembly_count", "signal_classes", "recommended_action"],
        classified,
    )
    category_files = {
        "body_site": "isolation_source_body_site_leakage.csv",
        "sample_type": "isolation_source_sample_type_leakage.csv",
        "environment_medium": "isolation_source_environment_medium_leakage.csv",
        "disease": "isolation_source_disease_leakage.csv",
        "host_taxon_or_context": "isolation_source_host_taxon_leakage.csv",
        "metadata_descriptor": "isolation_source_metadata_descriptor_leakage.csv",
        "generic": "isolation_source_generic_values.csv",
        "raw_code_or_artifact": "isolation_source_raw_code_leakage.csv",
    }
    for category, filename in category_files.items():
        write_csv(
            output_dir / filename,
            ["isolation_source_sd", "assembly_count", "recommended_action"],
            category_rows[category],
        )
    write_csv(
        output_dir / "non_approved_isolation_source_broad_values.csv",
        ["isolation_source_sd_broad", "assembly_count"],
        invalid_broad,
    )
    write_csv(
        output_dir / "top_unresolved_raw_isolation_source_values.csv",
        ["raw_isolation_source", "assembly_count"],
        unresolved_raw,
    )
    write_csv(
        output_dir / "controlled_categories_rule_audit.csv",
        [
            "source_column", "normalized_value", "destination", "rule_count",
            "proposed_values", "duplicate_key", "conflict_key",
        ],
        rule_rows,
    )
    manifest = {
        "controlled_categories": {
            "path": provenance["controlled_categories_file"],
            "sha256": provenance["controlled_categories_sha256"],
            "rows": rule_metrics["total_rows"],
            "approved_rows": rule_metrics["approved_rows"],
            "duplicate_keys": rule_metrics["duplicate_keys"],
            "conflict_keys": rule_metrics["conflict_keys"],
        },
        "approved_broad_categories": {
            "path": provenance["approved_broad_categories_file"],
            "sha256": provenance["approved_broad_categories_sha256"],
            "rows": sum(1 for _ in APPROVED_BROAD_CATEGORIES.open(encoding="utf-8")) - 1,
            "approved_isolation_source_broad_values": len(approved_broad),
        },
        "runtime_rule_source": "Committed CSV files are loaded by app.load_external_standardization_rules().",
    }
    (output_dir / "source_sample_environment_rule_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "source_sample_environment_qa_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Source, Sample, and Environment QA Summary",
        "",
        f"- Status: **{summary['status']}**",
        f"- Canonical snapshot: `{snapshot_id}`",
        f"- QA timestamp: `{provenance['qa_timestamp']}`",
        f"- Rows audited: {total:,}",
        f"- Raw isolation source coverage: {raw_count:,} ({metrics['raw_isolation_source_present_percent']}%)",
        f"- Isolation_Source_SD coverage: {source_count:,} ({metrics['isolation_source_sd_present_percent']}%)",
        f"- Isolation_Source_SD_Broad coverage: {broad_count:,} ({metrics['isolation_source_sd_broad_present_percent']}%)",
        f"- Raw-present standardization: {metrics['raw_present_isolation_source_standardization_percent']}%",
        f"- Raw-only unresolved isolation source rows: {metrics['raw_only_unresolved_isolation_source_rows']:,}",
        f"- Non-approved broad rows: {metrics['non_approved_broad_rows']:,}",
        f"- Controlled-category duplicate/conflict keys: {rule_metrics['duplicate_keys']}/{rule_metrics['conflict_keys']}",
        f"- Review-signal exact cross-field values: {len(classified):,} unique; {metrics['review_signal_exact_cross_field_rows']:,} rows",
        f"- Hard exact-source leakage rows: {metrics['hard_exact_leakage_rows']:,}",
        f"- Intentional cross-field context rows: {metrics['intentional_cross_field_context_rows']:,}",
        f"- Material routed to Sample_Type_SD: {metrics['material_routed_to_sample_type_rows']:,}",
        f"- Environment medium routed: {metrics['environment_medium_routed_rows']:,}",
        f"- Site routed: {metrics['site_routed_rows']:,}",
        f"- Metadata descriptor suppressed: {metrics['metadata_descriptor_suppressed_rows']:,}",
        f"- Food context preserved: {metrics['food_context_preserved_rows']:,}",
        "",
        summary["review_note"],
    ]
    (output_dir / "source_sample_environment_qa_summary.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    (output_root / "latest.json").write_text(
        json.dumps({
            "snapshot_id": snapshot_id,
            "qa_timestamp": provenance["qa_timestamp"],
            "summary": str(
                (output_dir / "source_sample_environment_qa_summary.json").relative_to(output_root)
            ),
        }, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args()
    summary = generate_qa(args.snapshot_id or None, output_root=args.output_root)
    print(json.dumps(summary, sort_keys=True))
    return 1 if args.fail_on_hard_errors and summary["hard_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
