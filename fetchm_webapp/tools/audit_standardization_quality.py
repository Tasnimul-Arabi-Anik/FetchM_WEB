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

from app import app, get_db, metadata_value_is_missing, standardize_collection_year_value  # noqa: E402
from lib.fetchm_runtime.metadata import COUNTRY_MAPPING  # noqa: E402


SOURCE_LIKE_PATTERN = re.compile(
    r"\b("
    r"blood|biopsy|broth|carcass|culture|environment|environmental|feces|faeces|fecal|faecal|"
    r"food|gut|intestinal|manure|meat|metagenome|milk|product|sample|seafood|sediment|"
    r"sewage|sludge|soil|stool|surface|swab|tissue|urine|wastewater|water"
    r")\b",
    re.IGNORECASE,
)


REQUIRED_HOST_COLUMNS = [
    "Host_Original",
    "Host_Cleaned",
    "Host_SD",
    "Host_TaxID",
    "Host_Rank",
    "Host_Superkingdom",
    "Host_Phylum",
    "Host_Class",
    "Host_Order",
    "Host_Family",
    "Host_Genus",
    "Host_Species",
    "Host_Common_Name",
    "Host_Context_SD",
    "Host_Age_Group_SD",
    "Host_Production_Context_SD",
    "Host_Anatomical_Site_SD",
    "Host_Match_Method",
    "Host_Confidence",
    "Host_Review_Status",
]

STANDARDIZED_COLUMNS = [
    *REQUIRED_HOST_COLUMNS,
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Environment_Medium_SD",
    "Environment_Medium_SD_Broad",
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Country",
    "Continent",
    "Subcontinent",
    "Collection Date",
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


def counter_rows(counter: Counter[Any], *headers: str, limit: int | None = None) -> tuple[list[str], list[list[Any]]]:
    rows: list[list[Any]] = []
    for key, count in counter.most_common(limit):
        if isinstance(key, tuple):
            rows.append([count, *key])
        else:
            rows.append([count, key])
    return ["count", *headers], rows


def expected_geo(country: str) -> tuple[str, str]:
    mapping = COUNTRY_MAPPING.get(country) or {}
    return clean(mapping.get("Continent")), clean(mapping.get("Subcontinent"))


def load_genus_clean_paths(limit: int | None = None) -> list[tuple[str, Path]]:
    with app.app_context():
        db = get_db()
        query = """
            SELECT species_name, metadata_clean_path
            FROM species
            WHERE taxon_rank = 'genus'
              AND metadata_status = 'ready'
              AND metadata_clean_path IS NOT NULL
            ORDER BY COALESCE(genome_count, 0) DESC, species_name ASC
        """
        rows = db.execute(query).fetchall()
    paths = [(str(row["species_name"]), Path(row["metadata_clean_path"])) for row in rows]
    return paths[:limit] if limit else paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit refreshed genus-level standardized metadata quality.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "standardization" / "review" / "quality_audit",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional number of genus files to scan.")
    parser.add_argument("--top", type=int, default=1000, help="Rows to keep in top-value CSVs.")
    args = parser.parse_args()

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir / run_stamp
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    files_scanned = 0
    file_errors: list[list[Any]] = []
    missing_columns: Counter[str] = Counter()

    host_sd_present = 0
    host_taxid_present = 0
    host_original_present = 0
    host_context_recovered = 0
    host_review_needed = 0
    source_like_mapped_host = 0
    source_like_unmapped_host = 0

    country_present = 0
    continent_present = 0
    subcontinent_present = 0
    country_continent_mismatch = 0
    country_subcontinent_mismatch = 0
    collection_year_present = 0
    sample_type_present = 0
    isolation_source_present = 0
    environment_medium_present = 0

    host_rank_counts: Counter[str] = Counter()
    host_method_counts: Counter[str] = Counter()
    host_confidence_counts: Counter[str] = Counter()
    host_review_status_counts: Counter[str] = Counter()
    host_sd_counts: Counter[tuple[str, str, str]] = Counter()
    host_unmapped_counts: Counter[str] = Counter()
    source_like_host_counts: Counter[tuple[str, str, str]] = Counter()
    suspicious_mapped_host_counts: Counter[tuple[str, str, str, str]] = Counter()

    sample_type_counts: Counter[str] = Counter()
    sample_type_broad_counts: Counter[str] = Counter()
    isolation_source_counts: Counter[str] = Counter()
    isolation_source_broad_counts: Counter[str] = Counter()
    environment_medium_counts: Counter[str] = Counter()
    environment_medium_broad_counts: Counter[str] = Counter()

    country_counts: Counter[str] = Counter()
    continent_counts: Counter[str] = Counter()
    subcontinent_counts: Counter[str] = Counter()
    geo_mismatch_counts: Counter[tuple[str, str, str, str]] = Counter()
    collection_year_counts: Counter[str] = Counter()
    per_file_rows: list[list[Any]] = []

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
        row_count = len(frame)
        total_rows += row_count
        for column in STANDARDIZED_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""
                missing_columns[column] += 1

        host_original = frame["Host_Original"].fillna("").astype(str).str.strip()
        host_sd = frame["Host_SD"].fillna("").astype(str).str.strip()
        host_taxid = frame["Host_TaxID"].fillna("").astype(str).str.strip()
        host_rank = frame["Host_Rank"].fillna("").astype(str).str.strip()
        host_method = frame["Host_Match_Method"].fillna("").astype(str).str.strip()
        host_confidence = frame["Host_Confidence"].fillna("").astype(str).str.strip()
        host_review_status = frame["Host_Review_Status"].fillna("").astype(str).str.strip()

        host_original_mask = host_original.map(present)
        host_sd_mask = host_sd.map(present)
        host_taxid_mask = host_taxid.map(present)
        host_context_mask = host_method.str.startswith("context_", na=False)
        review_needed_mask = host_review_status.eq("review_needed")
        source_like_mask = host_original.map(lambda value: bool(SOURCE_LIKE_PATTERN.search(value)))
        source_like_mapped_mask = source_like_mask & host_taxid_mask
        source_like_unmapped_mask = source_like_mask & ~host_taxid_mask & host_original_mask

        host_original_present += int(host_original_mask.sum())
        host_sd_present += int(host_sd_mask.sum())
        host_taxid_present += int(host_taxid_mask.sum())
        host_context_recovered += int(host_context_mask.sum())
        host_review_needed += int(review_needed_mask.sum())
        source_like_mapped_host += int(source_like_mapped_mask.sum())
        source_like_unmapped_host += int(source_like_unmapped_mask.sum())

        host_rank_counts.update(host_rank[host_rank.ne("")].value_counts().to_dict())
        host_method_counts.update(host_method[host_method.ne("")].value_counts().to_dict())
        host_confidence_counts.update(host_confidence[host_confidence.ne("")].value_counts().to_dict())
        host_review_status_counts.update(host_review_status[host_review_status.ne("")].value_counts().to_dict())

        mapped_frame = frame.loc[host_taxid_mask, ["Host_SD", "Host_TaxID", "Host_Rank"]].fillna("").astype(str)
        for row in mapped_frame.value_counts().items():
            key, count = row
            host_sd_counts[tuple(clean(item) for item in key)] += int(count)

        for value, count in host_original[review_needed_mask & host_original_mask].value_counts().items():
            host_unmapped_counts[clean(value)] += int(count)

        for idx in frame.index[source_like_unmapped_mask]:
            source_like_host_counts[(clean(host_original.loc[idx]), clean(host_sd.loc[idx]), clean(host_method.loc[idx]))] += 1
        for idx in frame.index[source_like_mapped_mask]:
            suspicious_mapped_host_counts[
                (clean(host_original.loc[idx]), clean(host_sd.loc[idx]), clean(host_taxid.loc[idx]), clean(host_method.loc[idx]))
            ] += 1

        for column, counter in [
            ("Sample_Type_SD", sample_type_counts),
            ("Sample_Type_SD_Broad", sample_type_broad_counts),
            ("Isolation_Source_SD", isolation_source_counts),
            ("Isolation_Source_SD_Broad", isolation_source_broad_counts),
            ("Environment_Medium_SD", environment_medium_counts),
            ("Environment_Medium_SD_Broad", environment_medium_broad_counts),
            ("Country", country_counts),
            ("Continent", continent_counts),
            ("Subcontinent", subcontinent_counts),
        ]:
            values = frame[column].fillna("").astype(str).str.strip()
            counter.update({value: int(count) for value, count in values[values.map(present)].value_counts().items()})

        file_sample_type_present = int(frame["Sample_Type_SD"].fillna("").astype(str).str.strip().map(present).sum())
        file_isolation_source_present = int(frame["Isolation_Source_SD"].fillna("").astype(str).str.strip().map(present).sum())
        file_environment_medium_present = int(frame["Environment_Medium_SD"].fillna("").astype(str).str.strip().map(present).sum())
        sample_type_present += file_sample_type_present
        isolation_source_present += file_isolation_source_present
        environment_medium_present += file_environment_medium_present
        country_mask = frame["Country"].fillna("").astype(str).str.strip().map(present)
        continent_mask = frame["Continent"].fillna("").astype(str).str.strip().map(present)
        subcontinent_mask = frame["Subcontinent"].fillna("").astype(str).str.strip().map(present)
        collection_date_values = frame["Collection Date"].fillna("").astype(str).str.strip()
        collection_year_values = collection_date_values.map(lambda value: standardize_collection_year_value(value) or "")
        collection_year_mask = collection_year_values.map(present)
        collection_year_counts.update(
            {value: int(count) for value, count in collection_year_values[collection_year_mask].value_counts().items()}
        )
        country_present += int(country_mask.sum())
        continent_present += int(continent_mask.sum())
        subcontinent_present += int(subcontinent_mask.sum())
        collection_year_present += int(collection_year_mask.sum())

        geo = frame.loc[country_mask, ["Country", "Continent", "Subcontinent"]].fillna("").astype(str)
        for country, continent, subcontinent in geo.itertuples(index=False, name=None):
            expected_continent, expected_subcontinent = expected_geo(clean(country))
            if expected_continent and clean(continent) != expected_continent:
                country_continent_mismatch += 1
                geo_mismatch_counts[(clean(country), "Continent", clean(continent), expected_continent)] += 1
            if expected_subcontinent and clean(subcontinent) != expected_subcontinent:
                country_subcontinent_mismatch += 1
                geo_mismatch_counts[(clean(country), "Subcontinent", clean(subcontinent), expected_subcontinent)] += 1

        per_file_rows.append(
            [
                taxon_name,
                row_count,
                int(host_taxid_mask.sum()),
                int(review_needed_mask.sum()),
                int(country_mask.sum()),
                int(collection_year_mask.sum()),
                file_sample_type_present,
                file_isolation_source_present,
                file_environment_medium_present,
            ]
        )

    summary_rows = [
        ["files_scanned", files_scanned],
        ["rows_scanned", total_rows],
        ["file_errors", len(file_errors)],
        ["missing_required_column_file_hits", sum(missing_columns.values())],
        ["host_original_present", host_original_present],
        ["host_sd_present", host_sd_present],
        ["host_taxid_present", host_taxid_present],
        ["host_taxid_percent", ratio(host_taxid_present, total_rows)],
        ["host_context_recovered_rows", host_context_recovered],
        ["host_review_needed_rows", host_review_needed],
        ["source_like_mapped_host_rows_for_review", source_like_mapped_host],
        ["source_like_unmapped_host_rows_for_review", source_like_unmapped_host],
        ["sample_type_sd_present", sample_type_present],
        ["sample_type_sd_percent", ratio(sample_type_present, total_rows)],
        ["isolation_source_sd_present", isolation_source_present],
        ["isolation_source_sd_percent", ratio(isolation_source_present, total_rows)],
        ["environment_medium_sd_present", environment_medium_present],
        ["environment_medium_sd_percent", ratio(environment_medium_present, total_rows)],
        ["country_present", country_present],
        ["country_percent", ratio(country_present, total_rows)],
        ["continent_present", continent_present],
        ["subcontinent_present", subcontinent_present],
        ["country_continent_mismatch_rows", country_continent_mismatch],
        ["country_subcontinent_mismatch_rows", country_subcontinent_mismatch],
        ["collection_year_present", collection_year_present],
        ["collection_year_percent", ratio(collection_year_present, total_rows)],
    ]

    write_csv(output_dir / "standardization_quality_summary.csv", ["metric", "value"], summary_rows)
    write_csv(output_dir / "file_errors.csv", ["taxon", "path", "error"], file_errors)
    write_csv(output_dir / "missing_required_columns.csv", ["column", "file_count"], [[k, v] for k, v in missing_columns.most_common()])
    write_csv(
        output_dir / "per_taxon_quality_summary.csv",
        [
            "taxon",
            "rows",
            "host_taxid_rows",
            "host_review_needed_rows",
            "country_rows",
            "collection_year_rows",
            "sample_type_rows",
            "isolation_source_rows",
            "environment_medium_rows",
        ],
        per_file_rows,
    )

    for filename, counter, headers in [
        ("host_rank_counts.csv", host_rank_counts, ["host_rank"]),
        ("host_method_counts.csv", host_method_counts, ["host_match_method"]),
        ("host_confidence_counts.csv", host_confidence_counts, ["host_confidence"]),
        ("host_review_status_counts.csv", host_review_status_counts, ["host_review_status"]),
        ("top_host_mappings.csv", host_sd_counts, ["host_sd", "taxid", "rank"]),
        ("top_host_review_needed.csv", host_unmapped_counts, ["host_original"]),
        ("source_like_unmapped_hosts_for_review.csv", source_like_host_counts, ["host_original", "host_sd", "host_method"]),
        ("suspicious_source_like_mapped_hosts.csv", suspicious_mapped_host_counts, ["host_original", "host_sd", "taxid", "host_method"]),
        ("sample_type_sd_counts.csv", sample_type_counts, ["sample_type_sd"]),
        ("sample_type_broad_counts.csv", sample_type_broad_counts, ["sample_type_sd_broad"]),
        ("isolation_source_sd_counts.csv", isolation_source_counts, ["isolation_source_sd"]),
        ("isolation_source_broad_counts.csv", isolation_source_broad_counts, ["isolation_source_sd_broad"]),
        ("environment_medium_sd_counts.csv", environment_medium_counts, ["environment_medium_sd"]),
        ("environment_medium_broad_counts.csv", environment_medium_broad_counts, ["environment_medium_sd_broad"]),
        ("country_counts.csv", country_counts, ["country"]),
        ("continent_counts.csv", continent_counts, ["continent"]),
        ("subcontinent_counts.csv", subcontinent_counts, ["subcontinent"]),
        ("collection_year_counts.csv", collection_year_counts, ["collection_year"]),
        ("geography_mismatches.csv", geo_mismatch_counts, ["country", "field", "current", "expected"]),
    ]:
        header, rows = counter_rows(counter, *headers, limit=args.top)
        write_csv(output_dir / filename, header, rows)

    summary = {metric: value for metric, value in summary_rows}
    markdown = [
        "# Standardization Quality Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Scope",
        "",
        f"- Genus metadata files scanned: {files_scanned:,}",
        f"- Rows scanned: {total_rows:,}",
        f"- File errors: {len(file_errors):,}",
        "",
        "## Host Standardization",
        "",
        f"- Host TaxID mapped rows: {host_taxid_present:,} ({summary['host_taxid_percent']}%).",
        f"- Host context-recovered rows: {host_context_recovered:,}.",
        f"- Host rows still requiring review: {host_review_needed:,}.",
        f"- Source-like host values mapped to a host and needing spot-check: {source_like_mapped_host:,}.",
        f"- Source-like host values still unmapped and needing routing/review: {source_like_unmapped_host:,}.",
        "",
        "## Source, Sample, And Environment Routing",
        "",
        f"- `Sample_Type_SD` present: {sample_type_present:,} ({summary['sample_type_sd_percent']}%).",
        f"- `Isolation_Source_SD` present: {isolation_source_present:,} ({summary['isolation_source_sd_percent']}%).",
        f"- `Environment_Medium_SD` present: {environment_medium_present:,} ({summary['environment_medium_sd_percent']}%).",
        "",
        "## Geography And Time",
        "",
        f"- Country present: {country_present:,} ({summary['country_percent']}%).",
        f"- Collection year present: {collection_year_present:,} ({summary['collection_year_percent']}%).",
        f"- Country-continent mismatch rows: {country_continent_mismatch:,}.",
        f"- Country-subcontinent mismatch rows: {country_subcontinent_mismatch:,}.",
        "",
        "## Priority Review Files",
        "",
        "- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.",
        "- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.",
        "- `top_host_review_needed.csv`: most frequent remaining host values needing review.",
        "- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.",
        "- `standardization_quality_summary.csv`: full metric table.",
        "",
    ]
    (output_dir / "standardization_quality_audit.md").write_text("\n".join(markdown), encoding="utf-8")

    print(output_dir)
    for metric, value in summary_rows:
        print(f"{metric}\t{value}")


if __name__ == "__main__":
    main()
