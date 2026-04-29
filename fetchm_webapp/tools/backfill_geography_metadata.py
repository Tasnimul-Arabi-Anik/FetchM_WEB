from __future__ import annotations

import csv
import logging
import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app, get_db, recover_secondary_geography  # noqa: E402
from lib.fetchm_runtime.metadata import (  # noqa: E402
    COUNTRY_MAPPING,
    MISSING_VALUE_TOKENS,
    add_geo_columns,
    extract_country,
    normalize_country_name,
    save_clean_data,
    save_summary,
)

logging.getLogger().setLevel(logging.WARNING)

MISSING = {str(value).lower() for value in MISSING_VALUE_TOKENS} | {
    "",
    "absent",
    "unknown",
    "not applicable",
    "not available",
    "not collected",
    "not determined",
    "not known",
    "not provided",
    "not recorded",
    "restricted access",
}


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return str(value).strip().lower() in MISSING


def normalize_existing_country(value: Any) -> str | None:
    if is_missing(value):
        return None
    normalized = normalize_country_name(str(value).split(":", 1)[0].strip())
    return normalized if normalized else None


def derive_geography_fields(frame: pd.DataFrame) -> pd.DataFrame:
    if "Geographic Location" not in frame.columns:
        frame["Geographic Location"] = "absent"
    geo_values = frame["Geographic Location"].fillna("").astype(str).str.strip()
    geo_country_map = {value: extract_country(value) for value in geo_values.unique()}
    geo_derived = geo_values.map(geo_country_map)
    derived = geo_derived.copy()
    source = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    confidence = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    evidence = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    recovery_status = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    geo_mask = geo_derived.notna() & geo_derived.astype(str).str.strip().ne("")
    source.loc[geo_mask] = "Geographic Location"
    confidence.loc[geo_mask] = "trusted"
    evidence.loc[geo_mask] = geo_values.loc[geo_mask].str.slice(0, 180)
    recovery_status.loc[geo_mask] = "trusted_primary"

    if "Country" in frame.columns:
        country_values = frame["Country"].fillna("").astype(str).str.strip()
        country_map = {value: normalize_existing_country(value) for value in country_values.unique()}
        existing = country_values.map(country_map)
        existing_mask = (derived.isna() | derived.astype(str).str.strip().eq("")) & existing.notna() & existing.astype(str).str.strip().ne("")
        derived = derived.where(derived.notna() & derived.astype(str).str.strip().ne(""), existing)
        source.loc[existing_mask] = "Country"
        confidence.loc[existing_mask] = "trusted"
        evidence.loc[existing_mask] = country_values.loc[existing_mask].str.slice(0, 180)
        recovery_status.loc[existing_mask] = "trusted_primary"

    missing_geo = geo_values.str.lower().isin(MISSING)
    derived = derived.fillna("")
    secondary_mask = derived.astype(str).str.strip().eq("")
    if secondary_mask.any():
        for index, row in frame.loc[secondary_mask].iterrows():
            recovered = recover_secondary_geography(row.to_dict())
            if not recovered:
                continue
            recovered_country, recovered_source, recovered_evidence, recovered_status = recovered
            derived.loc[index] = recovered_country
            source.loc[index] = recovered_source
            confidence.loc[index] = "high"
            evidence.loc[index] = recovered_evidence
            recovery_status.loc[index] = recovered_status

    derived = derived.mask(derived.astype(str).str.strip().eq("") & missing_geo, "absent").mask(
        derived.astype(str).str.strip().eq(""), "unknown"
    )
    missing_mask = derived.astype(str).str.lower().isin({"absent", "unknown"})
    source.loc[missing_mask] = ""
    confidence.loc[missing_mask] = ""
    evidence.loc[missing_mask] = ""
    recovery_status.loc[missing_mask] = derived.loc[missing_mask].astype(str).str.lower().map(
        {"absent": "absent", "unknown": "unknown"}
    ).fillna("")
    return pd.DataFrame(
        {
            "Country": derived,
            "Country_Source": source,
            "Country_Confidence": confidence,
            "Country_Evidence": evidence,
            "Geo_Recovery_Status": recovery_status,
        },
        index=frame.index,
    )


def count_missing_or_unmapped(country: pd.Series) -> int:
    values = country.fillna("").astype(str).str.strip()
    return int(values.map(lambda value: value.lower() in MISSING or value not in COUNTRY_MAPPING).sum())


def standardize_geography(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    updated = frame.copy()
    before_country = updated["Country"] if "Country" in updated.columns else pd.Series([""] * len(updated))
    before_continent = updated["Continent"] if "Continent" in updated.columns else pd.Series([""] * len(updated))
    before_subcontinent = (
        updated["Subcontinent"] if "Subcontinent" in updated.columns else pd.Series([""] * len(updated))
    )

    geography_fields = derive_geography_fields(updated)
    for column in geography_fields.columns:
        updated[column] = geography_fields[column]
    continent = updated["Country"].map(lambda value: COUNTRY_MAPPING.get(str(value), {}).get("Continent", "unknown"))
    subcontinent = updated["Country"].map(
        lambda value: COUNTRY_MAPPING.get(str(value), {}).get("Subcontinent", "unknown")
    )
    absent_mask = updated["Country"].astype(str).str.lower().eq("absent")
    updated["Continent"] = continent.mask(absent_mask, "absent")
    updated["Subcontinent"] = subcontinent.mask(absent_mask, "absent")

    stats = {
        "rows": int(len(updated)),
        "country_missing_or_unmapped_before": count_missing_or_unmapped(before_country),
        "country_missing_or_unmapped_after": count_missing_or_unmapped(updated["Country"]),
        "country_changed": int(
            before_country.fillna("").astype(str).str.strip().ne(updated["Country"].fillna("").astype(str).str.strip()).sum()
        ),
        "continent_changed": int(
            before_continent.fillna("").astype(str).str.strip().ne(updated["Continent"].fillna("").astype(str).str.strip()).sum()
        ),
        "subcontinent_changed": int(
            before_subcontinent.fillna("").astype(str).str.strip().ne(
                updated["Subcontinent"].fillna("").astype(str).str.strip()
            ).sum()
        ),
        "country_secondary_recovered": int(updated["Country_Confidence"].fillna("").astype(str).eq("high").sum()),
        "country_reviewed_secondary_recovered": int(
            updated["Geo_Recovery_Status"].fillna("").astype(str).eq("reviewed_secondary").sum()
        ),
        "country_rule_secondary_recovered": int(
            updated["Geo_Recovery_Status"].fillna("").astype(str).eq("rule_secondary").sum()
        ),
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
        "country_missing_or_unmapped_before",
        "country_missing_or_unmapped_after",
        "country_changed",
        "continent_changed",
        "subcontinent_changed",
        "country_secondary_recovered",
        "country_reviewed_secondary_recovered",
        "country_rule_secondary_recovered",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill standardized geography columns in managed metadata.")
    parser.add_argument(
        "--taxon-rank",
        choices=["genus", "species"],
        help="Restrict backfill to one taxon rank. Omit to process all ready metadata.",
    )
    args = parser.parse_args()

    output_dir = ROOT / "data" / "geography_backfill"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "geography_backfill_summary.csv"
    clean_columns_extra = ["Country", "Continent", "Subcontinent"]

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
                    "country_missing_or_unmapped_before": "read_error",
                    "country_missing_or_unmapped_after": str(exc),
                    "country_changed": 0,
                    "continent_changed": 0,
                    "subcontinent_changed": 0,
                    "country_secondary_recovered": 0,
                }
            )
            continue

        updated, stats = standardize_geography(frame)
        save_summary(updated, str(metadata_path))

        clean_path_value = record.get("metadata_clean_path")
        clean_path = Path(str(clean_path_value)) if clean_path_value else metadata_path.with_name("ncbi_clean.csv")
        clean_frame = add_geo_columns(updated.copy())
        clean_columns = list(dict.fromkeys(list(updated.columns) + clean_columns_extra))
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
        "country_missing_or_unmapped_before",
        "country_missing_or_unmapped_after",
        "country_changed",
        "continent_changed",
        "subcontinent_changed",
        "country_secondary_recovered",
        "country_reviewed_secondary_recovered",
        "country_rule_secondary_recovered",
    ]:
        print(f"{key}\\t{totals[key]}")


if __name__ == "__main__":
    main()
