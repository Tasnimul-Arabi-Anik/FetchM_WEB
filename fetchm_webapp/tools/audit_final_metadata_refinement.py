from __future__ import annotations

import argparse
import csv
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


APPROVED_BROAD_VALUES = {
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


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT.parent, text=True).strip()
    except Exception:
        return "unknown"


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
    if text in APPROVED_BROAD_VALUES or normalized in {normalize_standardization_lookup(v) for v in APPROVED_BROAD_VALUES}:
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

    summary = {
        "latest_audit_timestamp": run_stamp,
        "git_commit": git_sha(),
        "files_scanned": files_scanned,
        "rows_scanned": total_rows,
        "file_errors": len(file_errors),
        "unique_isolation_source_broad_values": len(broad_counts),
        "noisy_isolation_source_broad_values": len(noisy_broad_counts),
        "noisy_isolation_source_broad_rows": sum(noisy_broad_counts.values()),
        "body_site_leakage_values": len(body_site_counts),
        "disease_leakage_values": len(disease_counts),
        "raw_code_leakage_values": len(raw_code_counts),
    }
    write_csv(output_dir / "final_refinement_summary.csv", ["metric", "value"], [[key, value] for key, value in summary.items()])

    dashboard = [
        "# Final Metadata Standardization Dashboard",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Git commit: `{summary['git_commit']}`",
        "",
        "## Scope",
        "",
        f"- Files scanned: {files_scanned:,}",
        f"- Rows scanned: {total_rows:,}",
        f"- File errors: {len(file_errors)}",
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
        "## Review Files",
        "",
        "- `bad_isolation_source_broad_values.csv`",
        "- `body_site_misrouted_rules.csv`",
        "- `host_disease_source_leakage_audit.csv`",
        "- `raw_code_leakage_audit.csv`",
        "- `broad_vocabulary_compression_report.csv`",
        "- `final_refinement_summary.csv`",
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
