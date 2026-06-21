#!/usr/bin/env python3
"""Audit hidden Archaea metadata standardization without changing rules."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import connect, standardized_metadata_coverage
from domain_profiles import ARCHAEA_PROFILE, domain_profile_from_snapshot_id, domain_profile_from_taxon_id

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "standardization" / "review" / "archaea_pilot"

MISSING_VALUES = {
    "", "absent", "missing", "n/a", "na", "none", "not applicable", "not available",
    "not collected", "not provided", "null", "unknown", "unreported",
}
RAW_VALUE_FIELDS = [
    "Host", "Isolation Source", "Sample Type", "Collection_Date_Evidence", "Geographic Location",
    "BioSample ENV Package", "BioSample ENV Broad Scale", "BioSample ENV Local Scale",
    "BioSample ENV Medium", "BioSample ENV Material", "BioSample Host Disease",
]
STANDARDIZED_FIELDS = [
    "Host_SD", "Host_Context_SD", "Host_Superkingdom", "Country", "Continent",
    "Subcontinent", "Collection Date", "Isolation_Source_SD", "Isolation_Source_SD_Broad",
    "Sample_Type_SD", "Environment_Medium_SD", "Environment_Broad_Scale_SD",
    "Environment_Local_Scale_SD", "Isolation_Site_SD", "Host_Disease_SD",
    "Host_Health_State_SD",
]
SOURCE_SAMPLE_ENV_FIELDS = [
    "Isolation_Source_SD", "Isolation_Source_SD_Broad", "Sample_Type_SD", "Environment_Medium_SD",
    "Environment_Broad_Scale_SD", "Environment_Local_Scale_SD", "Isolation_Site_SD",
]
DOMAIN_TERM_RE = re.compile(r"\b(archaea|archaeon|archaeal|methanogen|methanogenic)\b", re.I)
DOMAIN_LABEL_ONLY_RE = re.compile(r"^(archaea|archaeon|archaeal|methanogen|methanogenic)$", re.I)
BACTERIAL_TERM_RE = re.compile(r"\b(bacteria|bacterium|bacterial)\b", re.I)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
ACCESSION_OR_COLLECTION_RE = re.compile(r"\b(ATCC|DSM|JCM|NBRC|NCTC|KCTC|GCA_|GCF_)\s*[-A-Z0-9_.]*", re.I)
PROCESS_DESCRIPTOR_RE = re.compile(r"\b(culture|cell culture|pure culture|mixed culture|whole organism)\b", re.I)
MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def present(value: Any) -> bool:
    return clean(value).casefold() not in MISSING_VALUES


def pct(numerator: int, denominator: int) -> float:
    return round((numerator / denominator * 100.0), 2) if denominator else 0.0


def write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def load_snapshot(snapshot_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with connect() as connection:
        snapshot_row = connection.execute(
            """
            SELECT snapshot_id, status, source_database, canonical_accession_namespace,
                   taxon_id, raw_records, root_unique_assemblies, noncanonical_records,
                   duplicate_records, summary_json
            FROM bacterial_inventory_snapshot
            WHERE snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()
        if snapshot_row is None:
            raise RuntimeError(f"Snapshot {snapshot_id!r} was not found in the dataset store.")
        rows = connection.execute(
            """
            SELECT i.assembly_accession, COALESCE(s.status, '') AS standardization_status,
                   s.standardized_payload
            FROM bacterial_inventory_membership AS i
            LEFT JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            ORDER BY i.assembly_accession
            """,
            (snapshot_id,),
        ).fetchall()
    snapshot = {
        "snapshot_id": str(snapshot_row[0]),
        "status": str(snapshot_row[1]),
        "source_database": str(snapshot_row[2]),
        "canonical_accession_namespace": str(snapshot_row[3]),
        "taxon_id": int(snapshot_row[4]),
        "raw_records": int(snapshot_row[5] or 0),
        "root_unique_assemblies": int(snapshot_row[6] or 0),
        "noncanonical_records": int(snapshot_row[7] or 0),
        "duplicate_records": int(snapshot_row[8] or 0),
        "summary_json": snapshot_row[9] or {},
    }
    records: list[dict[str, Any]] = []
    for accession, status, payload in rows:
        if isinstance(payload, str):
            payload = json.loads(payload)
        records.append({
            "assembly_accession": str(accession),
            "standardization_status": str(status or ""),
            "payload": payload or {},
        })
    return snapshot, records


def coverage_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(records)
    rows: list[dict[str, Any]] = []
    for field_type, fields in (("raw", RAW_VALUE_FIELDS), ("standardized", STANDARDIZED_FIELDS)):
        for field in fields:
            count = sum(1 for record in records if present(record["payload"].get(field)))
            rows.append({
                "field_type": field_type,
                "field": field,
                "present_count": count,
                "total_rows": total,
                "present_percent": pct(count, total),
            })
    return rows


def top_raw_value_rows(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in RAW_VALUE_FIELDS:
        counter: Counter[str] = Counter()
        for record in records:
            value = clean(record["payload"].get(field))
            if present(value):
                counter[value] += 1
        for value, count in counter.most_common(limit):
            rows.append({"source_field": field, "raw_value": value, "row_count": count})
    return rows


def source_sample_environment_rows(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in SOURCE_SAMPLE_ENV_FIELDS:
        counter: Counter[str] = Counter()
        for record in records:
            value = clean(record["payload"].get(field))
            if present(value):
                counter[value] += 1
        for value, count in counter.most_common(limit):
            signal = "observed_standardized_value"
            action = "no action in audit-only phase"
            if EMAIL_RE.search(value):
                signal = "person_or_email_in_standardized_field"
                action = "review as provenance/metadata descriptor, not source"
            elif ACCESSION_OR_COLLECTION_RE.search(value):
                signal = "collection_or_accession_code_in_standardized_field"
                action = "review as strain/provenance identifier, not source"
            elif DOMAIN_TERM_RE.search(value):
                signal = "domain_term_in_source_sample_environment_field"
                action = "review before promoting Archaea rules"
            elif BACTERIAL_TERM_RE.search(value):
                signal = "bacterial_term_in_archaea_standardized_field"
                action = "review for bacteria-centric rule reuse"
            elif field in {"Isolation_Source_SD", "Isolation_Source_SD_Broad"} and PROCESS_DESCRIPTOR_RE.search(value):
                signal = "process_descriptor_in_source_field"
                action = "review culture/process wording before source promotion"
            rows.append({
                "field": field,
                "standardized_value": value,
                "row_count": count,
                "review_signal_type": signal,
                "recommended_action": action,
            })
    return rows


def rule_reuse_risk_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    risk_counter: Counter[tuple[str, str, str, str, str]] = Counter()
    for record in records:
        payload = record["payload"]
        host_superkingdom = clean(payload.get("Host_Superkingdom"))
        host_sd = clean(payload.get("Host_SD"))
        if host_sd and host_superkingdom in MICROBIAL_SUPERKINGDOMS:
            risk_counter[(
                "microbial_host_assignment_review", "Host_SD", host_sd, "medium",
                "Microbial host assignments need domain-aware review for archaeal records.",
            )] += 1
        for field in SOURCE_SAMPLE_ENV_FIELDS:
            value = clean(payload.get(field))
            if not value:
                continue
            if EMAIL_RE.search(value):
                risk_counter[(
                    "person_or_email_in_standardized_field", field, value, "medium",
                    "Person/email-like text appeared in a standardized field and should remain provenance, not source.",
                )] += 1
            if ACCESSION_OR_COLLECTION_RE.search(value) and field in {"Isolation_Source_SD", "Isolation_Source_SD_Broad"}:
                risk_counter[(
                    "collection_or_accession_code_in_source_field", field, value, "medium",
                    "Collection/accession-like text appeared in source fields and should be reviewed as identifier/provenance.",
                )] += 1
            # Plain culture source context is intentionally retained by the current
            # bacteria-compatible policy. Specific non-source culture artifacts are
            # covered by regression tests and later curation, not by this audit risk table.
            if BACTERIAL_TERM_RE.search(value):
                risk_counter[(
                    "bacteria_centric_rule_reuse", field, value, "medium",
                    "A bacteria-specific term appeared in a standardized field for an Archaea pilot record.",
                )] += 1
            if DOMAIN_LABEL_ONLY_RE.fullmatch(value.strip()) and field in {"Isolation_Source_SD", "Sample_Type_SD", "Isolation_Site_SD"}:
                risk_counter[(
                    "domain_label_as_source_or_sample", field, value, "medium",
                    "Domain labels should not be promoted to source/sample/site rules without archaeal review.",
                )] += 1
    rows = [
        {
            "risk_category": key[0],
            "field": key[1],
            "matched_value": key[2],
            "affected_rows": count,
            "severity": key[3],
            "rationale": key[4],
            "recommended_action": "document for later Archaea curation; do not change rules in this audit",
        }
        for key, count in risk_counter.items()
    ]
    return sorted(rows, key=lambda row: (-int(row["affected_rows"]), row["risk_category"], row["field"], row["matched_value"]))


def top_standardized_values(records: list[dict[str, Any]], field: str, limit: int = 10) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for record in records:
        value = clean(record["payload"].get(field))
        if present(value):
            counter[value] += 1
    total = len(records)
    return [
        {"value": value, "row_count": count, "row_percent": pct(count, total)}
        for value, count in counter.most_common(limit)
    ]


def build_hidden_insights(snapshot: dict[str, Any], records: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    total = len(records)
    coverage = {
        field: {
            "present_count": sum(1 for record in records if present(record["payload"].get(field))),
            "total_rows": total,
            "present_percent": pct(sum(1 for record in records if present(record["payload"].get(field))), total),
        }
        for field in [
            "Country", "Continent", "Subcontinent", "Collection Date", "Host_SD",
            "Isolation_Source_SD", "Isolation_Source_SD_Broad", "Sample_Type_SD",
            "Environment_Medium_SD", "Environment_Broad_Scale_SD", "Environment_Local_Scale_SD",
            "Isolation_Site_SD", "Host_Disease_SD", "Host_Health_State_SD",
        ]
    }
    bioproject_counter: Counter[str] = Counter()
    for record in records:
        payload = record["payload"]
        for field in ("BioProject Accession", "BioProject", "Assembly BioProject Accession"):
            value = clean(payload.get(field))
            if present(value):
                bioproject_counter[value] += 1
                break
    top_bioproject = [
        {"value": value, "row_count": count, "row_percent": pct(count, total)}
        for value, count in bioproject_counter.most_common(10)
    ]
    return {
        "generated_at": utc_now(),
        "snapshot_id": snapshot["snapshot_id"],
        "domain_profile": summary["domain_profile"],
        "domain_label": summary["domain_label"],
        "visibility": "hidden_staging",
        "public_ui_exposed": False,
        "global_insights_regenerated": False,
        "root_unique_assemblies": summary["root_unique_assemblies"],
        "standardized_assemblies": summary["standardized_assemblies"],
        "missing_standardized_assemblies": summary["missing_standardized_assemblies"],
        "coverage": coverage,
        "top_countries": top_standardized_values(records, "Country"),
        "top_collection_years": top_standardized_values(records, "Collection Date"),
        "top_isolation_sources": top_standardized_values(records, "Isolation_Source_SD"),
        "top_sample_types": top_standardized_values(records, "Sample_Type_SD"),
        "top_environment_media": top_standardized_values(records, "Environment_Medium_SD"),
        "top_environment_broad_scale": top_standardized_values(records, "Environment_Broad_Scale_SD"),
        "top_host_contexts": top_standardized_values(records, "Host_Context_SD"),
        "top_bioprojects": top_bioproject,
        "interpretation_boundary": "Repository-level Archaea metadata coverage and representation, not biological prevalence or public FetchM Global Insights.",
    }


def write_hidden_insights_markdown(path: Path, insights: dict[str, Any]) -> None:
    def coverage_line(field: str) -> str:
        row = insights["coverage"].get(field, {})
        return f"| {field} | {int(row.get('present_count') or 0):,} | {float(row.get('present_percent') or 0):.2f}% |"

    lines = [
        "# Hidden Archaea Metadata Insights",
        "",
        f"Snapshot ID: `{insights['snapshot_id']}`",
        f"Generated: {insights['generated_at']}",
        "",
        "## Boundary",
        "",
        "This is a hidden staging analysis for Archaea metadata standardization. It is not public Global Insights, not NAR-facing, and not a deployment artifact.",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Root-unique assemblies | {insights['root_unique_assemblies']:,} |",
        f"| Standardized assemblies | {insights['standardized_assemblies']:,} |",
        f"| Missing standardized assemblies | {insights['missing_standardized_assemblies']:,} |",
        "",
        "## Coverage",
        "",
        "| Field | Present rows | Coverage |",
        "| --- | ---: | ---: |",
    ]
    for field in [
        "Country", "Collection Date", "Host_SD", "Isolation_Source_SD", "Sample_Type_SD",
        "Environment_Medium_SD", "Environment_Broad_Scale_SD", "Environment_Local_Scale_SD",
        "Isolation_Site_SD", "Host_Disease_SD", "Host_Health_State_SD",
    ]:
        lines.append(coverage_line(field))
    lines.extend([
        "",
        "## Interpretation",
        "",
        insights["interpretation_boundary"],
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def build_summary(snapshot: dict[str, Any], records: list[dict[str, Any]], coverage: dict[str, int], risks: list[dict[str, Any]]) -> dict[str, Any]:
    profile_from_taxon = domain_profile_from_taxon_id(snapshot["taxon_id"])
    profile_from_snapshot = domain_profile_from_snapshot_id(snapshot["snapshot_id"])
    standardized_rows = sum(1 for record in records if record["payload"])
    high_risk_count = sum(1 for row in risks if row.get("severity") == "high")
    criteria = {
        "snapshot_suffix_is_archaea": profile_from_snapshot.key == ARCHAEA_PROFILE.key,
        "taxon_id_is_archaea_root": profile_from_taxon.key == ARCHAEA_PROFILE.key,
        "snapshot_has_rows": len(records) > 0,
        "all_pilot_rows_standardized": coverage.get("missing_standardized_assemblies", 1) == 0,
        "no_high_risk_rule_reuse_signals": high_risk_count == 0,
    }
    return {
        "generated_at": utc_now(),
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_status": snapshot["status"],
        "domain_profile": profile_from_taxon.key,
        "domain_label": profile_from_taxon.label,
        "taxon_id": snapshot["taxon_id"],
        "source_database": snapshot["source_database"],
        "canonical_accession_namespace": snapshot["canonical_accession_namespace"],
        "root_unique_assemblies": coverage.get("root_unique_assemblies", len(records)),
        "standardized_assemblies": coverage.get("standardized_assemblies", standardized_rows),
        "missing_standardized_assemblies": coverage.get("missing_standardized_assemblies", 0),
        "rule_reuse_risk_rows": len(risks),
        "rule_reuse_high_risk_rows": high_risk_count,
        "audit_pass": all(criteria.values()),
        "pass_criteria": criteria,
        "production_database_touched": False,
        "public_ui_exposed": False,
        "canonical_refresh_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "analysis_scope": "hidden_full" if snapshot["status"] == "completed" else "hidden_pilot",
        "next_step": "Review audit signals before any Archaea-specific rule curation or public exposure.",
    }


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Hidden Archaea Metadata Standardization Audit",
        "",
        f"Snapshot ID: `{summary['snapshot_id']}`",
        f"Generated: {summary['generated_at']}",
        "",
        "## Result",
        "",
        "This audit standardized metadata for a hidden Archaea snapshot using the existing FetchM metadata machinery. The audit run did not touch public UI, Global Insights, or deployment state.",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Domain profile | `{summary['domain_profile']}` |",
        f"| NCBI taxon root | `{summary['taxon_id']}` |",
        f"| Root-unique assemblies | {summary['root_unique_assemblies']:,} |",
        f"| Standardized assemblies | {summary['standardized_assemblies']:,} |",
        f"| Missing standardized assemblies | {summary['missing_standardized_assemblies']:,} |",
        f"| Rule-reuse review signals | {summary['rule_reuse_risk_rows']:,} |",
        f"| High-risk rule-reuse signals | {summary['rule_reuse_high_risk_rows']:,} |",
        f"| Audit pass | `{str(summary['audit_pass']).lower()}` |",
        f"| Analysis scope | `{summary['analysis_scope']}` |",
        "",
        "## Boundaries",
        "",
        "- Archaea remains hidden.",
        "- The audit command is read-only with respect to standardization rules; any curation changes are tracked in git.",
        "- Hidden Archaea outputs remain non-public and separate from the NAR-facing bacterial release.",
        "- No canonical refresh, Global Insights regeneration, public UI exposure, or deployment was run.",
        "",
        "## Outputs",
        "",
        "- `standardized_field_coverage.tsv`",
        "- `top_raw_metadata_values.tsv`",
        "- `source_sample_environment_review.tsv`",
        "- `rule_reuse_risk.tsv`",
        "",
        "## Recommended Next Step",
        "",
        summary["next_step"],
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--top-limit", type=int, default=25)
    args = parser.parse_args()
    if args.top_limit < 1:
        parser.error("--top-limit must be positive")
    snapshot, records = load_snapshot(args.snapshot_id)
    coverage = standardized_metadata_coverage(args.snapshot_id)
    field_rows = coverage_rows(records)
    top_rows = top_raw_value_rows(records, args.top_limit)
    sse_rows = source_sample_environment_rows(records, args.top_limit)
    risk_rows = rule_reuse_risk_rows(records)
    summary = build_summary(snapshot, records, coverage, risk_rows)
    insights = build_hidden_insights(snapshot, records, summary)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_tsv(output_dir / "standardized_field_coverage.tsv", ["field_type", "field", "present_count", "total_rows", "present_percent"], field_rows)
    write_tsv(output_dir / "top_raw_metadata_values.tsv", ["source_field", "raw_value", "row_count"], top_rows)
    write_tsv(output_dir / "source_sample_environment_review.tsv", ["field", "standardized_value", "row_count", "review_signal_type", "recommended_action"], sse_rows)
    write_tsv(output_dir / "rule_reuse_risk.tsv", ["risk_category", "field", "matched_value", "affected_rows", "severity", "rationale", "recommended_action"], risk_rows)
    (output_dir / "archaea_metadata_audit_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(output_dir / "archaea_metadata_audit_summary.md", summary)
    (output_dir / "hidden_archaea_metadata_insights.json").write_text(json.dumps(insights, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_hidden_insights_markdown(output_dir / "hidden_archaea_metadata_insights.md", insights)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["audit_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
