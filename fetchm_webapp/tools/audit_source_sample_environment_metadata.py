from __future__ import annotations

import argparse
import csv
import re
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


FIELD_GROUPS = {
    "Isolation Source": ("Isolation_Source_SD", "Isolation_Source_SD_Broad"),
    "Isolation Site": ("Isolation_Site_SD", "Isolation_Site_SD_Broad"),
    "Sample Type": ("Sample_Type_SD", "Sample_Type_SD_Broad"),
    "Environment Medium": ("Environment_Medium_SD", "Environment_Medium_SD_Broad"),
    "Environment (Broad Scale)": ("Environment_Broad_Scale_SD", "Environment_Broad_Scale_SD_Broad"),
    "Environment (Local Scale)": ("Environment_Local_Scale_SD", "Environment_Local_Scale_SD_Broad"),
    "Host Disease": ("Host_Disease_SD", "Host_Disease_SD_Broad"),
    "Host Health State": ("Host_Health_State_SD", "Host_Health_State_SD_Broad"),
}

EXTRA_SOURCE_COLUMNS = {
    "Host Disease": [
        "BioSample Host Disease",
        "BioSample Disease",
        "BioSample Study Disease",
        "BioSample Disease State",
    ],
    "Host Health State": [
        "BioSample Host Health State",
        "BioSample Health State",
        "BioSample Health Status",
        "BioSample Host Health",
    ],
}

SUGGESTION_RULES: list[tuple[re.Pattern[str], str, str, str, str]] = [
    (re.compile(r"\b(?:sterile site|normally sterile site)\b"), "Isolation_Site_SD", "sterile body site", "high", "sterile body site"),
    (re.compile(r"\b(?:hospital|icu|intensive care unit|nursing home|long[-\s]*term care|healthcare facility)\b"), "Isolation_Source_SD", "healthcare facility", "high", "healthcare-associated location"),
    (re.compile(r"\b(?:screening|surveillance)\b"), "Sample_Type_SD", "surveillance sample", "medium", "screening/surveillance descriptor"),
    (re.compile(r"\b(?:pathogen\.?cl|assembly|other|diverse|not applicable|not available|not provided|missing)\b"), "Sample_Type_SD", "metadata descriptor/non-source", "high", "metadata/admin descriptor"),
    (re.compile(r"\b(?:esputum|sputum)\b"), "Sample_Type_SD", "sputum", "high", "respiratory material"),
    (re.compile(r"\b(?:balf|bronchoalveolar lavage)\b"), "Sample_Type_SD", "bronchoalveolar lavage fluid", "high", "respiratory material"),
    (re.compile(r"\b(?:blood)\b"), "Sample_Type_SD", "blood", "high", "clinical material"),
    (re.compile(r"\b(?:urine)\b"), "Sample_Type_SD", "urine", "high", "clinical material"),
    (re.compile(r"\b(?:feces|faeces|fecal|faecal|stool)\b"), "Sample_Type_SD", "feces/stool", "high", "fecal material"),
    (re.compile(r"\b(?:rectal swab)\b"), "Sample_Type_SD", "rectal swab", "high", "swab material"),
    (re.compile(r"\b(?:nasopharyngeal|np swab)\b"), "Sample_Type_SD", "nasopharyngeal swab", "high", "upper respiratory swab"),
    (re.compile(r"\b(?:nasal swab|nose swab)\b"), "Sample_Type_SD", "nasal swab", "high", "upper respiratory swab"),
    (re.compile(r"\b(?:throat swab|oropharyngeal)\b"), "Sample_Type_SD", "oropharyngeal/throat swab", "high", "upper respiratory swab"),
    (re.compile(r"\b(?:biofilm)\b"), "Environment_Medium_SD", "biofilm", "high", "environmental medium"),
    (re.compile(r"\b(?:air)\b"), "Environment_Medium_SD", "air", "high", "environmental medium"),
    (re.compile(r"\b(?:soil|rhizosphere)\b"), "Environment_Medium_SD", "soil", "high", "environmental medium"),
    (re.compile(r"\b(?:sediment)\b"), "Environment_Medium_SD", "sediment", "high", "environmental medium"),
    (re.compile(r"\b(?:sludge)\b"), "Environment_Medium_SD", "sludge", "high", "environmental medium"),
    (re.compile(r"\b(?:sewage)\b"), "Environment_Medium_SD", "sewage", "high", "environmental medium"),
    (re.compile(r"\b(?:wastewater|waste water)\b"), "Environment_Medium_SD", "wastewater", "high", "environmental medium"),
    (re.compile(r"\b(?:river water|river)\b"), "Environment_Medium_SD", "river water", "high", "water type"),
    (re.compile(r"\b(?:lake water|lake)\b"), "Environment_Medium_SD", "lake water", "high", "water type"),
    (re.compile(r"\b(?:pond water|pond)\b"), "Environment_Medium_SD", "pond water", "high", "water type"),
    (re.compile(r"\b(?:sea water|seawater|marine water)\b"), "Environment_Medium_SD", "seawater", "high", "water type"),
    (re.compile(r"\b(?:healthy|control)\b"), "Host_Health_State_SD", "healthy/no disease reported", "medium", "health state"),
    (re.compile(r"\b(?:sick|diseased|disease)\b"), "Host_Health_State_SD", "diseased", "medium", "health state"),
]


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


def suggest_rule(raw_value: str) -> tuple[str, str, str, str]:
    normalized = normalize_standardization_lookup(raw_value)
    if not normalized:
        return "", "", "", ""
    for pattern, destination, category, confidence, note in SUGGESTION_RULES:
        if pattern.search(normalized):
            return destination, category, confidence, note
    return "", "", "", ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Focused audit for source/sample/environment metadata curation.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "standardization" / "review" / "source_sample_environment_audit")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top", type=int, default=1000)
    args = parser.parse_args()

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir / run_stamp
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    files_scanned = 0
    file_errors: list[list[Any]] = []
    coverage = {field: {"raw": 0, "standardized": 0} for field in FIELD_GROUPS}
    raw_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    suggested_counts: Counter[tuple[str, str, str, str, str, str]] = Counter()

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
        for field, (sd_column, broad_column) in FIELD_GROUPS.items():
            raw_columns = [field, *EXTRA_SOURCE_COLUMNS.get(field, [])]
            for column in [*raw_columns, sd_column, broad_column]:
                if column not in frame.columns:
                    frame[column] = ""
            raw_frame = frame[raw_columns].fillna("").astype(str)
            raw_series = raw_frame.apply(lambda row: next((clean(value) for value in row if present(value)), ""), axis=1)
            sd_series = frame[sd_column].fillna("").astype(str).str.strip()
            broad_series = frame[broad_column].fillna("").astype(str).str.strip()
            coverage[field]["raw"] += int(raw_series.map(present).sum())
            coverage[field]["standardized"] += int(sd_series.map(present).sum())

            for raw_value, sd_value, broad_value in zip(raw_series, sd_series, broad_series):
                if not present(raw_value):
                    continue
                normalized = normalize_standardization_lookup(raw_value)
                status = "standardized" if present(sd_value) else "unmapped"
                raw_counts[(field, raw_value, normalized, status, sd_value or "")] += 1
                if not present(sd_value):
                    destination, category, confidence, note = suggest_rule(raw_value)
                    if destination:
                        suggested_counts[(field, raw_value, normalized, destination, category, confidence, note)] += 1

    coverage_rows = [
        [field, total_rows, values["raw"], ratio(values["raw"], total_rows), values["standardized"], ratio(values["standardized"], total_rows)]
        for field, values in coverage.items()
    ]
    write_csv(
        output_dir / "field_coverage_summary.csv",
        ["field", "rows_scanned", "raw_present", "raw_present_percent", "standardized_present", "standardized_present_percent"],
        coverage_rows,
    )
    write_csv(
        output_dir / "top_values_by_field.csv",
        ["count", "field", "raw_value", "normalized_value", "status", "current_standardized_value"],
        [[count, *key] for key, count in raw_counts.most_common(args.top)],
    )
    write_csv(
        output_dir / "suggested_high_confidence_rules.csv",
        ["count", "source_field", "raw_value", "normalized_value", "destination", "category", "confidence", "note"],
        [[count, *key] for key, count in suggested_counts.most_common(args.top)],
    )
    write_csv(output_dir / "file_errors.csv", ["taxon", "path", "error"], file_errors)

    markdown = [
        "# Source/Sample/Environment Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"- Genus metadata files scanned: {files_scanned:,}",
        f"- Rows scanned: {total_rows:,}",
        f"- File errors: {len(file_errors):,}",
        "",
        "## Coverage",
        "",
        "| Field | Raw present | Standardized present |",
        "|---|---:|---:|",
    ]
    for field, values in coverage.items():
        markdown.append(
            f"| {field} | {values['raw']:,} ({ratio(values['raw'], total_rows)}%) | "
            f"{values['standardized']:,} ({ratio(values['standardized'], total_rows)}%) |"
        )
    markdown.extend(
        [
            "",
            "## Review Files",
            "",
            "- `field_coverage_summary.csv`: standardized coverage by metadata field.",
            "- `top_values_by_field.csv`: high-count values, including unmapped values.",
            "- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.",
        ]
    )
    (output_dir / "source_sample_environment_audit.md").write_text("\n".join(markdown), encoding="utf-8")

    print(output_dir)
    print(f"files_scanned\t{files_scanned}")
    print(f"rows_scanned\t{total_rows}")
    print(f"file_errors\t{len(file_errors)}")
    for row in coverage_rows:
        print(f"{row[0]}\tstandardized\t{row[4]}\t{row[5]}%")


if __name__ == "__main__":
    main()
