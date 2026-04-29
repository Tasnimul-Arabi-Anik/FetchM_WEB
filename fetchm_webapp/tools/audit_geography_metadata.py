from __future__ import annotations

import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app, get_db, secondary_geo_country_context_blocked, secondary_geo_text_blocked  # noqa: E402
from lib.fetchm_runtime.metadata import COUNTRY_MAPPING, normalize_country_name  # noqa: E402


MISSING = {
    "",
    "-",
    "--",
    "absent",
    "missing",
    "na",
    "n/a",
    "nan",
    "none",
    "not applicable",
    "not available",
    "not collected",
    "not determined",
    "not known",
    "not provided",
    "not recorded",
    "restricted access",
    "unknown",
}

PRIMARY_GEO_COLUMNS = [
    "Geographic Location",
    "BioSample GEO LOC Name",
    "BioSample Geographic Location Country AND OR SEA",
    "BioSample Geographic Location Country AND OR SEA Region",
    "BioSample Country",
]

SEARCH_COLUMNS = [
    "Geographic Location",
    "BioSample GEO LOC Name",
    "BioSample Geographic Location Country AND OR SEA",
    "BioSample Geographic Location Country AND OR SEA Region",
    "BioSample Country",
    "BioSample Isolation Source",
    "Isolation Source",
    "BioSample Host",
    "Host",
    "BioSample Specific Host",
    "BioSample Source Name",
    "BioSample ENV Local Scale",
    "BioSample ENV Broad Scale",
    "BioSample Environment",
    "BioSample Description",
    "BioSample Title",
]


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return str(value).strip().lower() in MISSING


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def expected_geo(country: str) -> tuple[str, str]:
    mapping = COUNTRY_MAPPING.get(country) or {}
    return str(mapping.get("Continent") or ""), str(mapping.get("Subcontinent") or "")


def country_from_text(value: Any) -> str:
    text = clean_text(value)
    if not text or text.lower() in MISSING:
        return ""

    candidates = [text]
    for sep in [":", ";", "|", ",", "(", ")"]:
        candidates.extend(part.strip() for part in text.split(sep))

    for candidate in candidates:
        normalized = normalize_country_name(candidate)
        if normalized in COUNTRY_MAPPING:
            return normalized

    return ""


def build_country_patterns() -> list[tuple[str, re.Pattern[str]]]:
    countries = sorted(COUNTRY_MAPPING, key=len, reverse=True)
    patterns: list[tuple[str, re.Pattern[str]]] = []
    for country in countries:
        if len(country) < 4:
            continue
        if country in {"Marine"}:
            continue
        escaped = re.escape(country)
        patterns.append((country, re.compile(rf"(?<![A-Za-z]){escaped}(?![A-Za-z])", re.IGNORECASE)))
    return patterns


COUNTRY_PATTERNS = build_country_patterns()


def hidden_country_from_text(value: Any) -> str:
    text = clean_text(value)
    if not text or text.lower() in MISSING:
        return ""
    if secondary_geo_text_blocked(text):
        return ""

    direct = country_from_text(text)
    if direct:
        if direct in {"Turkey", "Guinea", "Norway", "Niger"}:
            return ""
        return direct

    for country, pattern in COUNTRY_PATTERNS:
        match = pattern.search(text)
        if match and not secondary_geo_country_context_blocked(country, text, match):
            return country
    return ""


def write_counter(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    output_dir = ROOT / "data" / "geography_review"
    output_dir.mkdir(parents=True, exist_ok=True)

    with app.app_context():
        db = get_db()
        paths = [
            Path(row[0])
            for row in db.execute(
                """
                SELECT metadata_path
                FROM species
                WHERE taxon_rank = 'genus'
                  AND metadata_status = 'ready'
                  AND metadata_path IS NOT NULL
                """
            ).fetchall()
        ]

    total_rows = 0
    files = 0
    country_present = 0
    country_missing = 0
    country_recoverable_primary = 0
    country_recoverable_other = 0
    continent_missing_when_expected = 0
    subcontinent_missing_when_expected = 0
    continent_mismatch = 0
    subcontinent_mismatch = 0

    raw_country_counts: Counter[str] = Counter()
    normalized_country_counts: Counter[str] = Counter()
    raw_geo_missing_counts: Counter[str] = Counter()
    recoverable_primary: Counter[tuple[str, str, str]] = Counter()
    recoverable_other: Counter[tuple[str, str, str]] = Counter()
    mismatch_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    missing_assignment_counts: Counter[tuple[str, str, str]] = Counter()
    assigned_geo_counts: Counter[tuple[str, str, str]] = Counter()
    hidden_source_examples: dict[tuple[str, str, str], str] = {}
    file_errors: list[tuple[str, str]] = []
    per_file_rows: list[list[Any]] = []
    searchable_column_hits: Counter[str] = Counter()
    country_by_column: dict[str, Counter[str]] = defaultdict(Counter)

    for path in paths:
        if not path.exists():
            file_errors.append((str(path), "missing_file"))
            continue
        try:
            df = pd.read_csv(path, sep="\t", dtype=str, low_memory=False)
        except Exception as exc:
            file_errors.append((str(path), str(exc)))
            continue

        files += 1
        rows_in_file = len(df)
        total_rows += rows_in_file
        file_missing = 0
        file_recovered = 0

        for column in PRIMARY_GEO_COLUMNS + ["Country", "Continent", "Subcontinent"]:
            if column not in df.columns:
                df[column] = ""

        present_search_columns = [column for column in SEARCH_COLUMNS if column in df.columns]

        country_values = df["Country"].fillna("").astype(str).str.strip()
        country_norm_map = {value: country_from_text(value) for value in country_values.unique()}
        country_norm = country_values.map(country_norm_map).fillna("")
        missing_mask = country_values.map(is_missing) | country_norm.eq("")
        present_mask = ~missing_mask

        for value, count in country_values.value_counts(dropna=False).items():
            value = clean_text(value)
            if value:
                raw_country_counts[value] += int(count)
                if is_missing(value) or not country_norm_map.get(value):
                    raw_geo_missing_counts[value] += int(count)

        country_missing += int(missing_mask.sum())
        file_missing += int(missing_mask.sum())
        country_present += int(present_mask.sum())
        for value, count in country_norm[present_mask].value_counts().items():
            normalized_country_counts[value] += int(count)

        grouped = (
            pd.DataFrame(
                {
                    "Country_Normalized": country_norm[present_mask],
                    "Continent": df.loc[present_mask, "Continent"].fillna("").astype(str).str.strip(),
                    "Subcontinent": df.loc[present_mask, "Subcontinent"].fillna("").astype(str).str.strip(),
                    "Geographic Location": df.loc[present_mask, "Geographic Location"].fillna("").astype(str).str.strip(),
                }
            )
            .groupby(["Country_Normalized", "Continent", "Subcontinent"], dropna=False)
            .agg(count=("Country_Normalized", "size"), example_geographic_location=("Geographic Location", "first"))
            .reset_index()
        )

        for grouped_row in grouped.itertuples(index=False):
            normalized_country = str(grouped_row[0])
            continent = str(grouped_row[1])
            subcontinent = str(grouped_row[2])
            count = int(grouped_row[3])
            geo_location = str(grouped_row[4])
            expected_continent, expected_subcontinent = expected_geo(normalized_country)
            assigned_geo_counts[(normalized_country, continent, subcontinent)] += count

            if expected_continent and is_missing(continent):
                continent_missing_when_expected += count
                missing_assignment_counts[(normalized_country, "Continent", expected_continent)] += count
            elif expected_continent and continent != expected_continent:
                continent_mismatch += count
                mismatch_counts[(normalized_country, "Continent", continent, expected_continent, geo_location[:160])] += count

            if expected_subcontinent and is_missing(subcontinent):
                subcontinent_missing_when_expected += count
                missing_assignment_counts[(normalized_country, "Subcontinent", expected_subcontinent)] += count
            elif expected_subcontinent and subcontinent != expected_subcontinent:
                subcontinent_mismatch += count
                mismatch_counts[
                    (normalized_country, "Subcontinent", subcontinent, expected_subcontinent, geo_location[:160])
                ] += count

        if missing_mask.any():
            remaining_missing = missing_mask.copy()
            for column in PRIMARY_GEO_COLUMNS:
                values = df.loc[remaining_missing, column].fillna("").astype(str).str.strip()
                value_country_map = {value: country_from_text(value) for value in values.unique()}
                recovered = values.map(value_country_map).fillna("")
                recovered_mask = recovered.ne("")
                if not recovered_mask.any():
                    continue
                for value, count in values[recovered_mask].value_counts().items():
                    suggested = value_country_map.get(value, "")
                    if not suggested:
                        continue
                    key = (column, suggested, clean_text(value)[:180])
                    recoverable_primary[key] += int(count)
                    hidden_source_examples.setdefault(key, clean_text(value))
                    country_recoverable_primary += int(count)
                    file_recovered += int(count)
                remaining_missing.loc[values[recovered_mask].index] = False
                if not remaining_missing.any():
                    break

            for column in present_search_columns:
                if column in PRIMARY_GEO_COLUMNS or not remaining_missing.any():
                    continue
                values = df.loc[remaining_missing, column].fillna("").astype(str).str.strip()
                value_country_map = {value: hidden_country_from_text(value) for value in values.unique()}
                recovered = values.map(value_country_map).fillna("")
                recovered_mask = recovered.ne("")
                if not recovered_mask.any():
                    continue
                for value, count in values[recovered_mask].value_counts().items():
                    suggested = value_country_map.get(value, "")
                    if not suggested:
                        continue
                    key = (column, suggested, clean_text(value)[:180])
                    recoverable_other[key] += int(count)
                    hidden_source_examples.setdefault(key, clean_text(value))
                    searchable_column_hits[column] += int(count)
                    country_by_column[column][suggested] += int(count)
                    country_recoverable_other += int(count)
                    file_recovered += int(count)
                remaining_missing.loc[values[recovered_mask].index] = False

        per_file_rows.append([path.name, rows_in_file, file_missing, file_recovered])

    write_counter(
        output_dir / "country_missing_recoverable_primary.csv",
        ["count", "source_column", "suggested_country", "example_value"],
        [[count, *key] for key, count in recoverable_primary.most_common()],
    )
    write_counter(
        output_dir / "country_missing_recoverable_other_columns.csv",
        ["count", "source_column", "suggested_country", "example_value"],
        [[count, *key] for key, count in recoverable_other.most_common()],
    )
    write_counter(
        output_dir / "geography_secondary_review_queue.csv",
        ["count", "source_column", "suggested_country", "example_value", "review_decision", "review_note"],
        [[count, *key, "review_needed", ""] for key, count in recoverable_other.most_common()],
    )
    write_counter(
        output_dir / "continent_subcontinent_mismatches.csv",
        ["count", "country", "field", "current_value", "expected_value", "example_geographic_location"],
        [[count, *key] for key, count in mismatch_counts.most_common()],
    )
    write_counter(
        output_dir / "continent_subcontinent_missing_assignments.csv",
        ["count", "country", "field", "expected_value"],
        [[count, *key] for key, count in missing_assignment_counts.most_common()],
    )
    write_counter(
        output_dir / "country_raw_values.csv",
        ["count", "raw_country"],
        [[count, value] for value, count in raw_country_counts.most_common()],
    )
    write_counter(
        output_dir / "country_normalized_values.csv",
        ["count", "normalized_country"],
        [[count, value] for value, count in normalized_country_counts.most_common()],
    )
    write_counter(
        output_dir / "hidden_country_source_columns.csv",
        ["count", "source_column"],
        [[count, column] for column, count in searchable_column_hits.most_common()],
    )
    write_counter(
        output_dir / "hidden_country_by_source_column.csv",
        ["source_column", "suggested_country", "count"],
        [
            [column, country, count]
            for column, counter in sorted(country_by_column.items())
            for country, count in counter.most_common()
        ],
    )
    write_counter(
        output_dir / "geography_audit_by_file.csv",
        ["file", "rows", "country_missing_or_unmapped", "country_recoverable"],
        per_file_rows,
    )
    write_counter(
        output_dir / "geography_file_errors.csv",
        ["file", "error"],
        file_errors,
    )

    summary_rows = [
        ["files_scanned", files],
        ["rows_scanned", total_rows],
        ["country_present_and_mapped", country_present],
        ["country_missing_or_unmapped", country_missing],
        ["country_recoverable_from_primary_geo_columns", country_recoverable_primary],
        ["country_recoverable_from_other_metadata_columns", country_recoverable_other],
        ["continent_missing_when_country_known", continent_missing_when_expected],
        ["subcontinent_missing_when_country_known", subcontinent_missing_when_expected],
        ["continent_mismatch_when_country_known", continent_mismatch],
        ["subcontinent_mismatch_when_country_known", subcontinent_mismatch],
        ["unique_raw_country_values", len(raw_country_counts)],
        ["unique_normalized_country_values", len(normalized_country_counts)],
        ["file_errors", len(file_errors)],
    ]
    write_counter(output_dir / "geography_audit_summary.csv", ["metric", "value"], summary_rows)

    print(output_dir)
    for metric, value in summary_rows:
        print(f"{metric}\\t{value}")


if __name__ == "__main__":
    main()
