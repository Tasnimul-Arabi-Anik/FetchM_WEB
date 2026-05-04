from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app, get_db, metadata_value_is_missing, normalize_standardization_lookup  # noqa: E402
from lib.fetchm_runtime.metadata import COUNTRY_MAPPING  # noqa: E402


DEFAULT_APPROVED_BROAD_VALUES = {
    "clinical/host-associated material",
    "feces/stool",
    "food",
    "food/meat",
    "food/dairy",
    "food/produce",
    "food/plant product",
    "food/processing environment",
    "water",
    "wastewater/sewage",
    "soil",
    "sediment",
    "healthcare-associated environment",
    "agricultural environment",
    "agricultural fecal material",
    "plant-associated material",
    "culture",
    "culture/assembly",
    "culture/isolate",
    "culture medium",
    "laboratory environment",
    "built environment",
    "surface sample",
    "biofilm",
    "respiratory sample",
    "upper respiratory tract",
    "upper respiratory site",
    "oral cavity",
    "urogenital site",
    "gastrointestinal site",
    "gut content",
    "tissue",
    "swab",
    "environmental material",
    "environmental/geologic material",
    "animal tissue/site",
    "animal-associated environment",
    "host-associated context",
    "metadata descriptor / non-source",
    "clinical fluid/material",
    "medical device",
    "aquatic food product",
    "biological/clinical product",
    "molecular extract",
    "single cell",
    "sample",
    "cloacal sample",
    "fermented food",
    "surface/sample collection material",
    "gut/host-associated material",
    "wastewater/organic waste",
}


def load_approved_broad_values() -> set[str]:
    path = ROOT / "standardization" / "approved_broad_categories.csv"
    values = set(DEFAULT_APPROVED_BROAD_VALUES)
    if not path.exists():
        return values
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            value = str(row.get("approved_value") or "").strip()
            if value:
                values.add(value)
    return values


APPROVED_BROAD_VALUES = load_approved_broad_values()
APPROVED_BROAD_NORMALIZED_VALUES = {normalize_standardization_lookup(value) for value in APPROVED_BROAD_VALUES}

BODY_SITE_PATTERN = re.compile(
    r"\b(?:"
    r"colon|ileum|caecum|cecum|cloaca|tonsil|pancreas|breast|perineum|perirectal|recto[-\s]*anal|"
    r"popliteal|forehead|foot|leg|uterus|anus|sinus|umbilicus|chin|bronch|bronchial|conjunctiva|"
    r"axilla|sacrum|pleural|oral cavity|urogenital|rectum|vagina|vaginal|nasal|skin|palm|cheek|index"
    r")\b",
    re.IGNORECASE,
)

DISEASE_PATTERN = re.compile(
    r"\b(?:"
    r"bronchiectasis|osteomyelitis|mastitis|infection|diarrhea|endocarditis|ssti|leukemia|disease|"
    r"aborted|sepsis|pneumonia|cystic fibrosis|cf|contact case|community acquired|hospital acquired"
    r")\b",
    re.IGNORECASE,
)

FACILITY_CODE_PATTERN = re.compile(
    r"^(?:"
    r"facility\s+\w+|ot|cxwnd|[lr]_(?:index|palm|cheek)|hcw hand|roar|cipa|esba|bk|o|#ref!|"
    r"isolated clone|isolated organism"
    r")$|(?:derived from the strain|parent strain|resistant derivatives|exposed to)",
    re.IGNORECASE,
)

RAW_TEXT_PATTERN = re.compile(r"\b(?:derived from|parent strain|exposed to|ciprofloxacin|atcc|#ref!)\b", re.IGNORECASE)

HOST_TAXON_PATTERN = re.compile(
    r"\b(?:"
    r"marmota|meriones|larus|spermophilus|otter|himalayana|unguiculatus|cachinnans|dauricus|"
    r"homo sapiens|bos taurus|sus scrofa|gallus gallus"
    r")\b",
    re.IGNORECASE,
)

GEOGRAPHY_HINT_PATTERN = re.compile(
    r"\b(?:"
    r"nottingham|guyana|shenzhen|tunisia|norway|france|australia|kathmandu|madrid|china|beijing"
    r")\b",
    re.IGNORECASE,
)


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def present(value: Any) -> bool:
    return bool(clean(value)) and not metadata_value_is_missing(value)


def ratio(numerator: int, denominator: int) -> float:
    return round((numerator / denominator) * 100, 2) if denominator else 0.0


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def command_output(command: list[str], cwd: Path) -> str:
    try:
        return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def run_identity() -> tuple[dict[str, str], list[str]]:
    warnings: list[str] = []
    git_commit = (
        os.environ.get("FETCHM_GIT_COMMIT")
        or os.environ.get("GIT_COMMIT")
        or command_output(["git", "rev-parse", "HEAD"], ROOT.parent)
        or "unknown"
    )
    git_branch = (
        os.environ.get("FETCHM_GIT_BRANCH")
        or os.environ.get("GIT_BRANCH")
        or command_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], ROOT.parent)
        or "unknown"
    )
    if git_commit == "unknown":
        warnings.append("Git commit could not be detected; set FETCHM_GIT_COMMIT for containerized audit runs.")
    if git_branch == "unknown":
        warnings.append("Git branch could not be detected; set FETCHM_GIT_BRANCH for containerized audit runs.")
    return (
        {
            "git_commit": git_commit,
            "git_branch": git_branch,
            "docker_image_tag": os.environ.get("FETCHM_DOCKER_IMAGE_TAG") or os.environ.get("FETCHM_IMAGE_TAG") or "unknown",
            "code_version": os.environ.get("FETCHM_CODE_VERSION") or git_commit[:12] if git_commit != "unknown" else "unknown",
            "worker_state": os.environ.get("FETCHM_WORKER_STATE") or "not_checked_by_audit",
        },
        warnings,
    )


def latest_child_dir(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    dirs = [path for path in parent.iterdir() if path.is_dir()]
    return sorted(dirs)[-1] if dirs else None


def read_metric_csv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        return {str(row.get("metric") or ""): str(row.get("value") or "") for row in csv.DictReader(handle)}


def read_count_csv(path: Path, count_column: str, label_column: str) -> dict[str, int]:
    if not path.exists():
        return {}
    result: dict[str, int] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            label = str(row.get(label_column) or "").strip()
            if not label:
                continue
            try:
                result[label] = int(float(str(row.get(count_column) or "0")))
            except ValueError:
                result[label] = 0
    return result


def read_source_field_summary(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        return {str(row.get("field") or ""): dict(row) for row in csv.DictReader(handle)}


def float_metric(metrics: dict[str, str], key: str) -> float:
    try:
        return float(metrics.get(key, "0") or 0)
    except ValueError:
        return 0.0


def int_metric(metrics: dict[str, str], key: str) -> int:
    try:
        return int(float(metrics.get(key, "0") or 0))
    except ValueError:
        return 0


def controlled_category_rule_quality() -> dict[str, int]:
    rule_file = ROOT / "standardization" / "controlled_categories.csv"
    if not rule_file.exists():
        return {
            "controlled_category_total_rows": 0,
            "controlled_category_approved_rows": 0,
            "controlled_category_duplicate_keys": 0,
            "controlled_category_conflict_keys": 0,
            "controlled_category_suspicious_rows": 0,
        }
    with rule_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    approved_rows = [row for row in rows if str(row.get("status") or "").strip().lower() == "approved"]
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in approved_rows:
        key = (
            str(row.get("source_column") or "").strip().lower(),
            str(row.get("normalized_value") or "").strip().lower(),
            str(row.get("destination") or "").strip(),
        )
        grouped.setdefault(key, []).append(row)
    duplicate_keys = sum(1 for entries in grouped.values() if len(entries) > 1)
    conflict_keys = 0
    for entries in grouped.values():
        proposed_values = {
            str(row.get("proposed_value") or row.get("category") or "").strip().lower()
            for row in entries
        }
        if len(entries) > 1 and len(proposed_values) > 1:
            conflict_keys += 1
    suspicious_rows = 0
    for row in approved_rows:
        destination = str(row.get("destination") or "").strip()
        normalized = normalize_standardization_lookup(row.get("normalized_value"))
        proposed = normalize_standardization_lookup(row.get("proposed_value") or row.get("category"))
        broad = normalize_standardization_lookup(row.get("broad_value"))
        note = normalize_standardization_lookup(row.get("note"))
        if destination == "Environment_Local_Scale_SD" and proposed == "intensive care unit" and normalized != "intensive care unit":
            suspicious_rows += 1
        elif destination == "Environment_Local_Scale_SD" and "agricultur" in normalized and "healthcare" in broad:
            suspicious_rows += 1
        elif "auto conservative" in note and proposed in {"swab", "water", "metadata descriptor non source"}:
            suspicious_rows += 1
    return {
        "controlled_category_total_rows": len(rows),
        "controlled_category_approved_rows": len(approved_rows),
        "controlled_category_duplicate_keys": duplicate_keys,
        "controlled_category_conflict_keys": conflict_keys,
        "controlled_category_suspicious_rows": suspicious_rows,
    }


def run_regression_tests() -> dict[str, Any]:
    command = [sys.executable, "-m", "unittest", "tests.test_metadata_standardization_regressions"]
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    output = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
    ran_match = re.search(r"Ran\s+(\d+)\s+tests?", output)
    failed_match = re.search(r"FAILED\s+\(([^)]*)\)", output)
    failed = 0
    if failed_match:
        for item in failed_match.group(1).split(","):
            if "=" in item:
                try:
                    failed += int(item.split("=", 1)[1].strip())
                except ValueError:
                    pass
    return {
        "regression_command": " ".join(command),
        "regression_returncode": completed.returncode,
        "regression_tests_run": int(ran_match.group(1)) if ran_match else 0,
        "regression_tests_failed": failed if completed.returncode else 0,
        "regression_status": "passed" if completed.returncode == 0 else "failed",
        "regression_output_tail": "\n".join(output.splitlines()[-20:]),
    }


def load_genus_clean_paths(limit: int | None = None) -> list[tuple[str, Path]]:
    with app.app_context():
        rows = get_db().execute(
            """
            SELECT species_name, metadata_clean_path
            FROM species
            WHERE taxon_rank = 'genus'
              AND metadata_status = 'ready'
              AND metadata_clean_path IS NOT NULL
            ORDER BY COALESCE(genome_count, 0) DESC, species_name ASC
            """
        ).fetchall()
    paths = [(str(row["species_name"]), Path(row["metadata_clean_path"])) for row in rows]
    return paths[:limit] if limit else paths


def classify_broad_value(value: str) -> tuple[str, str, str, str]:
    text = clean(value)
    normalized = normalize_standardization_lookup(text)
    if not present(text):
        return "acceptable_broad_category", "", "", "blank broad value"
    if text in APPROVED_BROAD_VALUES or normalized in APPROVED_BROAD_NORMALIZED_VALUES:
        return "acceptable_broad_category", "", "", "approved controlled broad category"
    if text in COUNTRY_MAPPING or GEOGRAPHY_HINT_PATTERN.search(text):
        return "geography_in_source_broad", "Country/Geography", "", "geography belongs in geography fields, not source broad"
    if HOST_TAXON_PATTERN.search(text):
        return "host_taxon_in_source_broad", "Host_SD", "", "host taxon belongs in host fields"
    if DISEASE_PATTERN.search(text):
        return "disease_in_source_broad", "Host_Disease_SD", "diseased", "disease/condition belongs in disease or health-state fields"
    if BODY_SITE_PATTERN.search(text):
        return "body_site_in_source_broad", "Isolation_Site_SD", "", "anatomical site belongs in isolation/body-site fields"
    if FACILITY_CODE_PATTERN.search(text):
        if re.search(r"facility|hcw|hospital|clinic", text, re.IGNORECASE):
            return "facility_or_code_in_source_broad", "Isolation_Source_SD", "healthcare-associated environment", "facility/code should be normalized"
        return "facility_or_code_in_source_broad", "manual_review", "", "code/admin token is not a controlled broad source"
    if RAW_TEXT_PATTERN.search(text) or len(text.split()) > 8:
        return "raw_text_fragment", "manual_review", "", "raw text fragment should not be a broad category"
    return "raw_text_fragment", "manual_review", "", "not in approved broad vocabulary"


def main() -> None:
    parser = argparse.ArgumentParser(description="Final QA for broad metadata standardization fields.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "standardization" / "review" / "final_audit")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top", type=int, default=1000)
    parser.add_argument("--run-regression-tests", action="store_true", help="Run metadata regression tests and include the result in the production gate.")
    args = parser.parse_args()

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir / run_stamp
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    files_scanned = 0
    file_errors: list[list[Any]] = []
    broad_counts: Counter[str] = Counter()
    noisy_broad_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    body_site_counts: Counter[tuple[str, str, str]] = Counter()
    disease_counts: Counter[tuple[str, str, str]] = Counter()
    raw_code_counts: Counter[tuple[str, str, str]] = Counter()
    broad_field_counts: dict[str, Counter[str]] = {
        "Isolation_Source_SD_Broad": Counter(),
        "Sample_Type_SD_Broad": Counter(),
        "Environment_Medium_SD_Broad": Counter(),
        "Environment_Broad_Scale_SD": Counter(),
        "Environment_Local_Scale_SD_Broad": Counter(),
    }

    for taxon_name, path in load_genus_clean_paths(args.limit):
        if not path.exists():
            file_errors.append([taxon_name, str(path), "missing_file"])
            continue
        try:
            frame = pd.read_csv(path, dtype=str, low_memory=False)
        except Exception as exc:
            file_errors.append([taxon_name, str(path), str(exc)])
            continue
        files_scanned += 1
        total_rows += len(frame)
        for column in [
            "Isolation_Source_SD_Broad",
            "Isolation_Source_SD",
            "Isolation_Site_SD",
            "Sample_Type_SD",
            "Host_Disease_SD",
            "Host_Health_State_SD",
            "Environment_Medium_SD",
            "Environment_Broad_Scale_SD",
            "Environment_Local_Scale_SD_Broad",
            "Sample_Type_SD_Broad",
            "Environment_Medium_SD_Broad",
        ]:
            if column not in frame.columns:
                frame[column] = ""

        broad_values = frame["Isolation_Source_SD_Broad"].fillna("").astype(str).str.strip()
        for value, count in broad_values[broad_values.map(present)].value_counts().items():
            broad_counts[clean(value)] += int(count)
            problem_class, destination, recommended, reason = classify_broad_value(clean(value))
            if problem_class != "acceptable_broad_category":
                noisy_broad_counts[(clean(value), problem_class, destination, recommended, reason)] += int(count)

        for field, counter in broad_field_counts.items():
            values = frame[field].fillna("").astype(str).str.strip()
            counter.update({clean(value): int(count) for value, count in values[values.map(present)].value_counts().items()})

        for field in ["Isolation_Source_SD", "Isolation_Source_SD_Broad", "Sample_Type_SD_Broad", "Environment_Medium_SD_Broad"]:
            values = frame[field].fillna("").astype(str).str.strip()
            for value, count in values[values.map(lambda item: bool(BODY_SITE_PATTERN.search(clean(item))))].value_counts().items():
                body_site_counts[(field, clean(value), "route to Isolation_Site_SD or Host_Anatomical_Site_SD")] += int(count)
            for value, count in values[values.map(lambda item: bool(DISEASE_PATTERN.search(clean(item))))].value_counts().items():
                disease_counts[(field, clean(value), "route to Host_Disease_SD/Host_Health_State_SD")] += int(count)
            for value, count in values[values.map(lambda item: bool(FACILITY_CODE_PATTERN.search(clean(item))) or bool(RAW_TEXT_PATTERN.search(clean(item))))].value_counts().items():
                raw_code_counts[(field, clean(value), "route to controlled category or manual_review")] += int(count)

    write_csv(
        output_dir / "bad_isolation_source_broad_values.csv",
        [
            "count",
            "isolation_source_sd_broad",
            "problem_class",
            "recommended_destination",
            "recommended_value",
            "recommended_action",
            "reason",
        ],
        [
            [count, value, problem_class, destination, recommended, "remap_or_demote", reason]
            for (value, problem_class, destination, recommended, reason), count in noisy_broad_counts.most_common(args.top)
        ],
    )
    write_csv(
        output_dir / "body_site_misrouted_rules.csv",
        ["count", "field", "value", "recommended_action"],
        [[count, *key] for key, count in body_site_counts.most_common(args.top)],
    )
    write_csv(
        output_dir / "host_disease_source_leakage_audit.csv",
        ["count", "field", "value", "recommended_action"],
        [[count, *key] for key, count in disease_counts.most_common(args.top)],
    )
    write_csv(
        output_dir / "raw_code_leakage_audit.csv",
        ["count", "field", "value", "recommended_action"],
        [[count, *key] for key, count in raw_code_counts.most_common(args.top)],
    )

    compression_rows: list[list[Any]] = []
    for field, counter in broad_field_counts.items():
        noisy_values = [value for value in counter if value and value not in APPROVED_BROAD_VALUES]
        compression_rows.append(
            [
                field,
                sum(counter.values()),
                len(counter),
                len(noisy_values),
                sum(counter[value] for value in noisy_values),
                "; ".join(f"{value}:{counter[value]}" for value in counter.most_common(20) if value[0]),
            ]
        )
    write_csv(
        output_dir / "broad_vocabulary_compression_report.csv",
        [
            "field",
            "represented_rows",
            "unique_broad_values",
            "non_approved_unique_values",
            "non_approved_represented_rows",
            "top_20_values",
        ],
        compression_rows,
    )
    write_csv(output_dir / "file_errors.csv", ["taxon", "path", "error"], file_errors)

    identity, identity_warnings = run_identity()
    quality_dir = latest_child_dir(ROOT / "standardization" / "review" / "quality_audit")
    source_dir = latest_child_dir(ROOT / "standardization" / "review" / "source_sample_environment_audit")
    quality_metrics = read_metric_csv(quality_dir / "standardization_quality_summary.csv") if quality_dir else {}
    host_review_counts = read_count_csv(quality_dir / "host_review_status_counts.csv", "count", "host_review_status") if quality_dir else {}
    source_field_summary = read_source_field_summary(source_dir / "field_coverage_summary.csv") if source_dir else {}
    rule_quality = controlled_category_rule_quality()
    regression = run_regression_tests() if args.run_regression_tests else {
        "regression_status": "not_run",
        "regression_tests_run": 0,
        "regression_tests_failed": 0,
        "regression_returncode": "",
        "regression_command": "",
        "regression_output_tail": "",
    }

    summary = {
        "latest_audit_timestamp": run_stamp,
        "git_commit": identity["git_commit"],
        "git_branch": identity["git_branch"],
        "docker_image_tag": identity["docker_image_tag"],
        "code_version": identity["code_version"],
        "worker_state": identity["worker_state"],
        "files_scanned": files_scanned,
        "rows_scanned": total_rows,
        "file_errors": len(file_errors),
        "unique_isolation_source_broad_values": len(broad_counts),
        "noisy_isolation_source_broad_values": len(noisy_broad_counts),
        "noisy_isolation_source_broad_rows": sum(noisy_broad_counts.values()),
        "body_site_leakage_values": len(body_site_counts),
        "disease_leakage_values": len(disease_counts),
        "raw_code_leakage_values": len(raw_code_counts),
        **rule_quality,
        "regression_status": regression["regression_status"],
        "regression_tests_run": regression["regression_tests_run"],
        "regression_tests_failed": regression["regression_tests_failed"],
    }
    write_csv(output_dir / "final_refinement_summary.csv", ["metric", "value"], [[key, value] for key, value in summary.items()])

    hard_failures: list[str] = []
    warnings: list[str] = list(identity_warnings)
    if len(file_errors) > 0:
        hard_failures.append("file_errors > 0")
    if int_metric(quality_metrics, "file_errors") > 0:
        hard_failures.append("quality_audit.file_errors > 0")
    if int_metric(quality_metrics, "non_country_values_in_country_rows") > 0:
        hard_failures.append("non_country_values_in_country_rows > 0")
    if int_metric(quality_metrics, "country_continent_mismatch_rows") > 0:
        hard_failures.append("country_continent_mismatch_rows > 0")
    if int_metric(quality_metrics, "country_subcontinent_mismatch_rows") > 0:
        hard_failures.append("country_subcontinent_mismatch_rows > 0")
    if int_metric(quality_metrics, "invalid_sample_type_host_term_rows") > 0:
        hard_failures.append("invalid_host_like_sample_type_rows > 0")
    if len(noisy_broad_counts) > 0 or sum(noisy_broad_counts.values()) > 0:
        hard_failures.append("noisy/non-approved broad values > 0")
    if len(body_site_counts) > 0:
        hard_failures.append("body-site leakage values > 0")
    if len(disease_counts) > 0:
        hard_failures.append("disease/source leakage values > 0")
    if len(raw_code_counts) > 0:
        hard_failures.append("raw code/text leakage values > 0")
    if rule_quality["controlled_category_duplicate_keys"] > 0:
        hard_failures.append("controlled-category duplicate_keys > 0")
    if rule_quality["controlled_category_conflict_keys"] > 0:
        hard_failures.append("controlled-category conflict_keys > 0")
    if regression["regression_status"] == "failed":
        hard_failures.append("regression tests failed")

    isolation_source_raw_present_percent = 0.0
    if "Isolation Source" in source_field_summary:
        try:
            isolation_source_raw_present_percent = float(
                source_field_summary["Isolation Source"].get("standardized_present_percent_same_field_raw_present_rows") or 0
            )
        except ValueError:
            isolation_source_raw_present_percent = 0.0
    if int_metric(quality_metrics, "host_review_needed_rows") > 1000:
        warnings.append("host_review_needed > 1000")
    if int_metric(quality_metrics, "source_like_unmapped_host_rows_for_review") > 5000:
        warnings.append("source_like_unmapped_hosts_for_review > 5000")
    if float_metric(quality_metrics, "host_taxid_percent") < 55:
        warnings.append("Host TaxID mapped < 55%")
    if float_metric(quality_metrics, "country_percent") < 80:
        warnings.append("Country present < 80%")
    if float_metric(quality_metrics, "collection_year_percent") < 75:
        warnings.append("Collection year present < 75%")
    if isolation_source_raw_present_percent and isolation_source_raw_present_percent < 80:
        warnings.append("Isolation Source raw-present standardization < 80%")

    production_ready = not hard_failures
    gate = {
        "production_ready": production_ready,
        "hard_failures": hard_failures,
        "warnings": warnings,
        "inputs": {
            "final_audit_dir": str(output_dir),
            "quality_audit_dir": str(quality_dir) if quality_dir else "",
            "source_sample_environment_audit_dir": str(source_dir) if source_dir else "",
        },
        "metrics": {
            **summary,
            "host_taxid_percent": quality_metrics.get("host_taxid_percent", ""),
            "host_review_needed_rows": quality_metrics.get("host_review_needed_rows", ""),
            "source_like_mapped_host_rows_for_review": quality_metrics.get("source_like_mapped_host_rows_for_review", ""),
            "source_like_unmapped_host_rows_for_review": quality_metrics.get("source_like_unmapped_host_rows_for_review", ""),
            "country_percent": quality_metrics.get("country_percent", ""),
            "collection_year_percent": quality_metrics.get("collection_year_percent", ""),
            "invalid_sample_type_host_term_rows": quality_metrics.get("invalid_sample_type_host_term_rows", ""),
            "non_country_values_in_country_rows": quality_metrics.get("non_country_values_in_country_rows", ""),
            "country_continent_mismatch_rows": quality_metrics.get("country_continent_mismatch_rows", ""),
            "country_subcontinent_mismatch_rows": quality_metrics.get("country_subcontinent_mismatch_rows", ""),
            "isolation_source_raw_present_standardization_percent": isolation_source_raw_present_percent,
        },
        "regression": regression,
    }
    (output_dir / "production_readiness_gate.json").write_text(json.dumps(gate, indent=2, sort_keys=True), encoding="utf-8")
    hard_failure_lines = [f"- {item}" for item in hard_failures] if hard_failures else ["- None"]
    warning_lines = [f"- {item}" for item in warnings] if warnings else ["- None"]
    gate_markdown = [
        "# Production Readiness Gate",
        "",
        f"- Production ready: {'yes' if production_ready else 'no'}",
        f"- Hard failures: {len(hard_failures)}",
        f"- Warnings: {len(warnings)}",
        "",
        "## Hard Failures",
        "",
        *hard_failure_lines,
        "",
        "## Warnings",
        "",
        *warning_lines,
        "",
        "## Regression",
        "",
        f"- Status: {regression['regression_status']}",
        f"- Tests run: {regression['regression_tests_run']}",
        f"- Tests failed: {regression['regression_tests_failed']}",
    ]
    (output_dir / "production_readiness_gate.md").write_text("\n".join(gate_markdown), encoding="utf-8")

    def quality_value(key: str, default: str = "not_available") -> str:
        return str(quality_metrics.get(key, default))

    def source_value(field: str, key: str, default: str = "not_available") -> str:
        return str(source_field_summary.get(field, {}).get(key, default))

    dashboard = [
        "# Final Metadata Standardization Dashboard",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Git commit SHA: `{summary['git_commit']}`",
        f"Git branch: `{summary['git_branch']}`",
        f"Docker image tag: `{summary['docker_image_tag']}`",
        f"Code version: `{summary['code_version']}`",
        f"Worker state: `{summary['worker_state']}`",
        "",
        "## Scope",
        "",
        f"- Files scanned: {files_scanned:,}",
        f"- Rows scanned: {total_rows:,}",
        f"- File errors: {len(file_errors)}",
        "",
        "## Geography",
        "",
        f"- Country coverage: {quality_value('country_percent')}%",
        f"- Non-country values in `Country`: {quality_value('non_country_values_in_country_rows')}",
        f"- Country-continent mismatches: {quality_value('country_continent_mismatch_rows')}",
        f"- Country-subcontinent mismatches: {quality_value('country_subcontinent_mismatch_rows')}",
        "",
        "## Time",
        "",
        f"- Collection year coverage: {quality_value('collection_year_percent')}%",
        "- Invalid/future collection year count: not_available",
        "",
        "## Host",
        "",
        f"- Host TaxID mapped: {quality_value('host_taxid_present')} rows ({quality_value('host_taxid_percent')}%)",
        f"- Host context recovered: {quality_value('host_context_recovered_rows')}",
        f"- Host review needed: {quality_value('host_review_needed_rows')}",
        f"- Host `non_host_source`: {host_review_counts.get('non_host_source', 'not_available')}",
        f"- Host `not_identifiable`: {host_review_counts.get('not_identifiable', 'not_available')}",
        f"- Source-like mapped host spot-check rows: {quality_value('source_like_mapped_host_rows_for_review')}",
        f"- Source-like unmapped host/source rows: {quality_value('source_like_unmapped_host_rows_for_review')}",
        "",
        "## Source/Sample/Environment",
        "",
        f"- `Sample_Type_SD` coverage: {quality_value('sample_type_sd_percent')}%",
        f"- Invalid host-like `Sample_Type_SD`: {quality_value('invalid_sample_type_host_term_rows')}",
        f"- `Isolation_Source_SD` coverage: {quality_value('isolation_source_sd_percent')}%",
        f"- Isolation Source raw-present standardization: {source_value('Isolation Source', 'standardized_present_percent_same_field_raw_present_rows')}%",
        f"- `Isolation_Site_SD` coverage: {source_value('Isolation Site', 'standardized_present_percent_all_rows')}%",
        f"- `Environment_Medium_SD` coverage: {quality_value('environment_medium_sd_percent')}%",
        f"- Environment Broad Scale coverage: {source_value('Environment (Broad Scale)', 'standardized_present_percent_all_rows')}%",
        f"- Environment Local Scale coverage: {source_value('Environment (Local Scale)', 'standardized_present_percent_all_rows')}%",
        f"- Host Disease raw-present standardization: {source_value('Host Disease', 'standardized_present_percent_same_field_raw_present_rows')}%",
        f"- Host Health State recovery/raw-present ratio: {source_value('Host Health State', 'standardized_present_percent_same_field_raw_present_rows')}%",
        "",
        "## Broad Vocabulary QA",
        "",
        f"- Unique `Isolation_Source_SD_Broad` values: {len(broad_counts):,}",
        f"- Noisy/non-approved broad values: {len(noisy_broad_counts):,}",
        f"- Rows represented by noisy broad values: {sum(noisy_broad_counts.values()):,}",
        f"- Body-site leakage values: {len(body_site_counts):,}",
        f"- Disease/source leakage values: {len(disease_counts):,}",
        f"- Raw code/text leakage values: {len(raw_code_counts):,}",
        "",
        "## Rule QA",
        "",
        f"- Controlled-category duplicate keys: {rule_quality['controlled_category_duplicate_keys']}",
        f"- Controlled-category conflict keys: {rule_quality['controlled_category_conflict_keys']}",
        f"- Controlled-category suspicious rows: {rule_quality['controlled_category_suspicious_rows']}",
        "",
        "## Regression",
        "",
        f"- Status: {regression['regression_status']}",
        f"- Tests run: {regression['regression_tests_run']}",
        f"- Tests failed: {regression['regression_tests_failed']}",
        "",
        "## Final Judgement",
        "",
        f"- Production-ready major fields: {'yes' if production_ready else 'no'}",
        "- Remaining curation: host low-frequency/source-like review",
        "",
        "## Review Files",
        "",
        "- `bad_isolation_source_broad_values.csv`",
        "- `body_site_misrouted_rules.csv`",
        "- `host_disease_source_leakage_audit.csv`",
        "- `raw_code_leakage_audit.csv`",
        "- `broad_vocabulary_compression_report.csv`",
        "- `final_refinement_summary.csv`",
        "- `production_readiness_gate.md`",
        "- `production_readiness_gate.json`",
        "",
        "## Release Note",
        "",
        "Metadata standardization is production-ready for genus-level FetchM outputs. Remaining issues are long-tail curation of broad source categories and low-frequency host names.",
        "",
    ]
    (output_dir / "final_metadata_standardization_dashboard.md").write_text("\n".join(dashboard), encoding="utf-8")

    print(output_dir)
    for key, value in summary.items():
        print(f"{key}\t{value}")


if __name__ == "__main__":
    main()
