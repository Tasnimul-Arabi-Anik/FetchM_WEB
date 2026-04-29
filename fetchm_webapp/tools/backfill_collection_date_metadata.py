from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app, get_db, recover_collection_date  # noqa: E402
from lib.fetchm_runtime.metadata import add_geo_columns, save_clean_data, save_summary  # noqa: E402


MISSING = {"", "absent", "unknown", "not available", "not provided", "not collected", "restricted access"}


def is_missing_year(value: Any) -> bool:
    text = "" if value is None else str(value).strip()
    return text.lower() in MISSING or not text.isdigit()


def standardize_collection_dates(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    updated = frame.copy()
    before = updated["Collection Date"] if "Collection Date" in updated.columns else pd.Series([""] * len(updated), index=updated.index)
    collection_date = before.fillna("").astype(str).copy()
    source = updated.get("Collection_Date_Source", pd.Series([""] * len(updated), index=updated.index)).fillna("").astype(str).copy()
    evidence = updated.get("Collection_Date_Evidence", pd.Series([""] * len(updated), index=updated.index)).fillna("").astype(str).copy()
    status = updated.get("Collection_Date_Recovery_Status", pd.Series([""] * len(updated), index=updated.index)).fillna("").astype(str).copy()

    for index, row in updated.iterrows():
        recovered = recover_collection_date(row.to_dict())
        if recovered:
            year, recovered_source, recovered_evidence, recovered_status = recovered
            collection_date.loc[index] = year
            source.loc[index] = recovered_source
            evidence.loc[index] = recovered_evidence
            status.loc[index] = recovered_status
        else:
            current = collection_date.loc[index]
            collection_date.loc[index] = "absent" if is_missing_year(current) else "unknown"
            source.loc[index] = ""
            evidence.loc[index] = ""
            status.loc[index] = collection_date.loc[index]

    updated["Collection Date"] = collection_date
    updated["Collection_Date_Source"] = source
    updated["Collection_Date_Evidence"] = evidence
    updated["Collection_Date_Recovery_Status"] = status

    stats = {
        "rows": int(len(updated)),
        "collection_date_missing_or_unmapped_before": int(before.map(is_missing_year).sum()),
        "collection_date_missing_or_unmapped_after": int(updated["Collection Date"].map(is_missing_year).sum()),
        "collection_date_changed": int(
            before.fillna("").astype(str).str.strip().ne(updated["Collection Date"].fillna("").astype(str).str.strip()).sum()
        ),
        "collection_date_trusted_primary": int(status.eq("trusted_primary").sum()),
        "collection_date_rule_secondary_recovered": int(status.eq("rule_secondary").sum()),
        "collection_date_reviewed_secondary_recovered": int(status.eq("reviewed_secondary").sum()),
    }
    return updated, stats


def write_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "species_id",
        "taxon_rank",
        "species_name",
        "metadata_path",
        "rows",
        "collection_date_missing_or_unmapped_before",
        "collection_date_missing_or_unmapped_after",
        "collection_date_changed",
        "collection_date_trusted_primary",
        "collection_date_rule_secondary_recovered",
        "collection_date_reviewed_secondary_recovered",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill standardized collection date/year fields.")
    parser.add_argument("--taxon-rank", choices=["genus", "species"], help="Restrict backfill to one taxon rank.")
    args = parser.parse_args()

    output_dir = ROOT / "data" / "collection_date_backfill"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "collection_date_backfill_summary.csv"

    with app.app_context():
        db = get_db()
        rank_filter = "AND taxon_rank = ?" if args.taxon_rank else ""
        params = (args.taxon_rank,) if args.taxon_rank else ()
        records = [
            dict(row)
            for row in db.execute(
                f"""
                SELECT id, taxon_rank, species_name, metadata_path, metadata_clean_path
                FROM species
                WHERE metadata_status = 'ready'
                  AND metadata_path IS NOT NULL
                  {rank_filter}
                ORDER BY taxon_rank, species_name
                """,
                params,
            ).fetchall()
        ]

    rows: list[dict[str, Any]] = []
    totals = Counter()
    for index, record in enumerate(records, start=1):
        metadata_path = Path(str(record["metadata_path"]))
        if not metadata_path.exists():
            continue
        try:
            frame = pd.read_csv(metadata_path, sep="\t", dtype=str, low_memory=False)
        except Exception as exc:
            rows.append(
                {
                    "species_id": record["id"],
                    "taxon_rank": record["taxon_rank"],
                    "species_name": record["species_name"],
                    "metadata_path": str(metadata_path),
                    "rows": 0,
                    "collection_date_missing_or_unmapped_before": "read_error",
                    "collection_date_missing_or_unmapped_after": str(exc),
                    "collection_date_changed": 0,
                    "collection_date_trusted_primary": 0,
                    "collection_date_rule_secondary_recovered": 0,
                    "collection_date_reviewed_secondary_recovered": 0,
                }
            )
            continue

        updated, stats = standardize_collection_dates(frame)
        save_summary(updated, str(metadata_path))

        clean_path_value = record.get("metadata_clean_path")
        clean_path = Path(str(clean_path_value)) if clean_path_value else metadata_path.with_name("ncbi_clean.csv")
        clean_frame = add_geo_columns(updated.copy())
        clean_columns = list(dict.fromkeys(list(updated.columns)))
        save_clean_data(clean_frame, [column for column in clean_columns if column in clean_frame.columns], str(clean_path))

        summary_row = {
            "species_id": record["id"],
            "taxon_rank": record["taxon_rank"],
            "species_name": record["species_name"],
            "metadata_path": str(metadata_path),
            **stats,
        }
        rows.append(summary_row)
        totals.update(stats)
        if index % 500 == 0:
            write_summary(summary_path, rows)
            print(f"processed {index}/{len(records)}")

    write_summary(summary_path, rows)
    print(summary_path)
    print(f"files_processed\\t{len(rows)}")
    for key in [
        "rows",
        "collection_date_missing_or_unmapped_before",
        "collection_date_missing_or_unmapped_after",
        "collection_date_changed",
        "collection_date_trusted_primary",
        "collection_date_rule_secondary_recovered",
        "collection_date_reviewed_secondary_recovered",
    ]:
        print(f"{key}\\t{totals[key]}")


if __name__ == "__main__":
    main()
