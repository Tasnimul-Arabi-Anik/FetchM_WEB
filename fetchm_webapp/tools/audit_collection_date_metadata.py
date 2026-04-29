from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    COLLECTION_DATE_PRIMARY_COLUMNS,
    COLLECTION_DATE_SECONDARY_TEXT_COLUMNS,
    app,
    extract_year_from_collection_text,
    get_db,
    metadata_value_is_missing,
    recover_collection_date,
    standardize_collection_year_value,
)


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


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def is_missing(value: Any) -> bool:
    return clean_text(value).lower() in MISSING


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    output_dir = ROOT / "data" / "collection_date_review"
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

    files = 0
    total_rows = 0
    date_present = 0
    date_missing = 0
    recoverable_primary = 0
    recoverable_secondary = 0
    file_errors: list[tuple[str, str]] = []
    raw_date_counts: Counter[str] = Counter()
    normalized_year_counts: Counter[str] = Counter()
    primary_candidates: Counter[tuple[str, str, str]] = Counter()
    secondary_candidates: Counter[tuple[str, str, str, str]] = Counter()
    per_file_rows: list[list[Any]] = []

    for path in paths:
        if not path.exists():
            file_errors.append((str(path), "missing_file"))
            continue
        try:
            frame = pd.read_csv(path, sep="\t", dtype=str, low_memory=False)
        except Exception as exc:
            file_errors.append((str(path), str(exc)))
            continue

        files += 1
        rows_in_file = len(frame)
        total_rows += rows_in_file
        file_missing = 0
        file_recovered = 0
        if "Collection Date" not in frame.columns:
            frame["Collection Date"] = ""

        for column in set(COLLECTION_DATE_PRIMARY_COLUMNS + COLLECTION_DATE_SECONDARY_TEXT_COLUMNS):
            if column not in frame.columns:
                frame[column] = ""

        raw_values = frame["Collection Date"].fillna("").astype(str).str.strip()
        for value, count in raw_values.value_counts(dropna=False).items():
            value = clean_text(value)
            if value:
                raw_date_counts[value] += int(count)

        trusted_year = pd.Series([""] * len(frame), index=frame.index, dtype="object")
        trusted_source = pd.Series([""] * len(frame), index=frame.index, dtype="object")
        for column in COLLECTION_DATE_PRIMARY_COLUMNS:
            values = frame[column].fillna("").astype(str).str.strip()
            parsed_map = {value: standardize_collection_year_value(value) or "" for value in values.unique()}
            parsed = values.map(parsed_map).fillna("")
            parsed_mask = trusted_year.eq("") & parsed.map(lambda value: not metadata_value_is_missing(value))
            if not parsed_mask.any():
                continue
            trusted_year.loc[parsed_mask] = parsed.loc[parsed_mask]
            trusted_source.loc[parsed_mask] = column
            if column != "Collection Date":
                for value, count in values[parsed_mask & raw_values.map(is_missing)].value_counts().items():
                    year = parsed_map.get(value, "")
                    if year and not metadata_value_is_missing(year):
                        primary_candidates[(column, year, clean_text(value)[:180])] += int(count)
                        recoverable_primary += int(count)
                        file_recovered += int(count)

        trusted_mask = trusted_year.ne("")
        date_present += int(trusted_mask.sum())
        file_missing += int((~trusted_mask).sum())
        date_missing += int((~trusted_mask).sum())
        for year, count in trusted_year[trusted_mask].value_counts().items():
            normalized_year_counts[str(year)] += int(count)

        remaining_mask = ~trusted_mask
        for column in COLLECTION_DATE_SECONDARY_TEXT_COLUMNS:
            if not remaining_mask.any():
                break
            values = frame.loc[remaining_mask, column].fillna("").astype(str).str.strip()
            recovered_map = {value: extract_year_from_collection_text(value, require_context=True) for value in values.unique()}
            recovered = values.map(lambda value: recovered_map.get(value))
            recovered_mask = recovered.notna()
            if not recovered_mask.any():
                continue
            for value, count in values[recovered_mask].value_counts().items():
                candidate = recovered_map.get(value)
                if not candidate:
                    continue
                year, evidence, status = candidate
                secondary_candidates[(column, year, evidence, status)] += int(count)
                recoverable_secondary += int(count)
                file_recovered += int(count)
            recovered_indices = values[recovered_mask].index
            remaining_mask.loc[recovered_indices] = False

        per_file_rows.append([path.name, rows_in_file, file_missing, file_recovered])

    write_csv(
        output_dir / "collection_date_raw_values.csv",
        ["count", "raw_collection_date"],
        [[count, value] for value, count in raw_date_counts.most_common()],
    )
    write_csv(
        output_dir / "collection_year_values.csv",
        ["count", "collection_year"],
        [[count, value] for value, count in normalized_year_counts.most_common()],
    )
    write_csv(
        output_dir / "collection_date_recoverable_primary.csv",
        ["count", "source_column", "suggested_year", "example_value"],
        [[count, *key] for key, count in primary_candidates.most_common()],
    )
    write_csv(
        output_dir / "collection_date_recoverable_secondary.csv",
        ["count", "source_column", "suggested_year", "example_value", "recovery_status"],
        [[count, *key] for key, count in secondary_candidates.most_common()],
    )
    write_csv(
        output_dir / "collection_date_secondary_review_queue.csv",
        ["count", "source_column", "suggested_year", "example_value", "recovery_status", "review_decision", "review_note"],
        [[count, *key, "review_needed", ""] for key, count in secondary_candidates.most_common()],
    )
    write_csv(
        output_dir / "collection_date_audit_by_file.csv",
        ["file", "rows", "collection_date_missing_or_unmapped", "collection_date_recoverable"],
        per_file_rows,
    )
    write_csv(output_dir / "collection_date_file_errors.csv", ["file", "error"], file_errors)

    summary_rows = [
        ["files_scanned", files],
        ["rows_scanned", total_rows],
        ["collection_date_present_and_mapped", date_present],
        ["collection_date_missing_or_unmapped", date_missing],
        ["collection_date_recoverable_from_primary_columns", recoverable_primary],
        ["collection_date_recoverable_from_secondary_text_columns", recoverable_secondary],
        ["unique_raw_collection_date_values", len(raw_date_counts)],
        ["unique_normalized_collection_year_values", len(normalized_year_counts)],
        ["file_errors", len(file_errors)],
    ]
    write_csv(output_dir / "collection_date_audit_summary.csv", ["metric", "value"], summary_rows)

    print(output_dir)
    for metric, value in summary_rows:
        print(f"{metric}\\t{value}")


if __name__ == "__main__":
    main()
