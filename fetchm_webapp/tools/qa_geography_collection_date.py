#!/usr/bin/env python3
"""Audit canonical geography and collection-date standardization."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect
from lib.fetchm_runtime.metadata import COUNTRY_MAPPING


DEFAULT_OUTPUT_ROOT = ROOT / "data" / "geography_collection_date_qa"
COUNTRY_LOOKUP_PATH = ROOT / "lib" / "fetchm_runtime" / "metadata.py"
COLLECTION_PARSER_PATH = ROOT / "app.py"
MIN_COLLECTION_YEAR = 1900
MISSING = {
    "",
    "-",
    "--",
    "absent",
    "missing",
    "n/a",
    "na",
    "none",
    "not applicable",
    "not available",
    "not collected",
    "not provided",
    "null",
    "unknown",
}
RAW_COUNTRY_SQL = """
COALESCE(
    NULLIF(BTRIM(s.standardized_payload->>'Geographic Location'), ''),
    NULLIF(BTRIM(s.standardized_payload->>'BioSample GEO LOC Name'), '')
)
"""
RAW_COLLECTION_DATE_SQL = """
COALESCE(
    NULLIF(BTRIM(s.standardized_payload->>'Collection_Date_Evidence'), ''),
    NULLIF(BTRIM(s.standardized_payload->>'BioSample Collection Date'), '')
)
"""
MISSING_SQL = ", ".join("'" + value.replace("'", "''") + "'" for value in sorted(MISSING))


def present_sql(expression: str) -> str:
    return (
        f"NULLIF(BTRIM(COALESCE({expression}, '')), '') IS NOT NULL "
        f"AND LOWER(BTRIM(COALESCE({expression}, ''))) NOT IN ({MISSING_SQL})"
    )


COUNTRY_PRESENT_SQL = present_sql("s.standardized_payload->>'Country'")
CONTINENT_PRESENT_SQL = present_sql("s.standardized_payload->>'Continent'")
SUBCONTINENT_PRESENT_SQL = present_sql("s.standardized_payload->>'Subcontinent'")
RAW_COUNTRY_PRESENT_SQL = present_sql(RAW_COUNTRY_SQL)
RAW_COLLECTION_DATE_PRESENT_SQL = present_sql(RAW_COLLECTION_DATE_SQL)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def percent(count: int, total: int) -> float:
    return round((100.0 * count / total), 2) if total else 0.0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str:
    configured_commit = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured_commit:
        return configured_commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT.parent,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


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


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[Iterable[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(fieldnames)
        writer.writerows(rows)


def grouped_values(connection: Any, snapshot_id: str, expression: str) -> list[tuple[str, int]]:
    return [
        (str(value or ""), int(count or 0))
        for value, count in connection.execute(
            f"""
            SELECT {expression} AS value, COUNT(*) AS assembly_count
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND {present_sql(expression)}
            GROUP BY 1
            ORDER BY assembly_count DESC, value
            """,
            (snapshot_id,),
        ).fetchall()
    ]


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
    current_year = run_date.year

    with connect() as connection:
        counts = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE {COUNTRY_PRESENT_SQL}) AS country_present,
                COUNT(*) FILTER (WHERE {CONTINENT_PRESENT_SQL}) AS continent_present,
                COUNT(*) FILTER (WHERE {SUBCONTINENT_PRESENT_SQL}) AS subcontinent_present,
                COUNT(*) FILTER (WHERE COALESCE(s.standardized_payload->>'Collection Date', '') ~ '^[0-9]{{4}}$') AS collection_year_present,
                COUNT(*) FILTER (WHERE {RAW_COUNTRY_PRESENT_SQL}) AS raw_country_present,
                COUNT(*) FILTER (WHERE {RAW_COLLECTION_DATE_PRESENT_SQL}) AS raw_date_present,
                COUNT(*) FILTER (
                    WHERE NOT ({RAW_COUNTRY_PRESENT_SQL})
                      AND {COUNTRY_PRESENT_SQL}
                ) AS standardized_only_country,
                COUNT(*) FILTER (
                    WHERE NOT ({RAW_COLLECTION_DATE_PRESENT_SQL})
                      AND COALESCE(s.standardized_payload->>'Collection Date', '') ~ '^[0-9]{{4}}$'
                ) AS standardized_only_year,
                COUNT(*) FILTER (
                    WHERE {RAW_COUNTRY_PRESENT_SQL}
                      AND NOT ({COUNTRY_PRESENT_SQL})
                ) AS raw_only_country,
                COUNT(*) FILTER (
                    WHERE {RAW_COLLECTION_DATE_PRESENT_SQL}
                      AND NOT (COALESCE(s.standardized_payload->>'Collection Date', '') ~ '^[0-9]{{4}}$')
                ) AS raw_only_year,
                COUNT(*) FILTER (
                    WHERE NOT ({RAW_COUNTRY_PRESENT_SQL})
                      AND NOT ({COUNTRY_PRESENT_SQL})
                ) AS neither_country,
                COUNT(*) FILTER (
                    WHERE NOT ({RAW_COLLECTION_DATE_PRESENT_SQL})
                      AND NOT (COALESCE(s.standardized_payload->>'Collection Date', '') ~ '^[0-9]{{4}}$')
                ) AS neither_year,
                COUNT(*) FILTER (
                    WHERE {COUNTRY_PRESENT_SQL}
                      AND NOT ({CONTINENT_PRESENT_SQL})
                ) AS country_without_continent,
                COUNT(*) FILTER (
                    WHERE {COUNTRY_PRESENT_SQL}
                      AND NOT ({SUBCONTINENT_PRESENT_SQL})
                ) AS country_without_subcontinent,
                COUNT(*) FILTER (
                    WHERE NOT ({COUNTRY_PRESENT_SQL})
                      AND (
                          {CONTINENT_PRESENT_SQL}
                          OR {SUBCONTINENT_PRESENT_SQL}
                      )
                ) AS geography_without_country
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()

        country_rows = grouped_values(connection, snapshot_id, "s.standardized_payload->>'Country'")
        continent_rows = grouped_values(connection, snapshot_id, "s.standardized_payload->>'Continent'")
        subcontinent_rows = grouped_values(connection, snapshot_id, "s.standardized_payload->>'Subcontinent'")
        year_rows = grouped_values(connection, snapshot_id, "s.standardized_payload->>'Collection Date'")
        geography_pairs = connection.execute(
            f"""
            SELECT COALESCE(s.standardized_payload->>'Country', ''),
                   COALESCE(s.standardized_payload->>'Continent', ''),
                   COALESCE(s.standardized_payload->>'Subcontinent', ''),
                   COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND {COUNTRY_PRESENT_SQL}
            GROUP BY 1, 2, 3
            ORDER BY 4 DESC, 1
            """,
            (snapshot_id,),
        ).fetchall()
        missing_country_geo = connection.execute(
            f"""
            SELECT COALESCE(s.standardized_payload->>'Continent', ''),
                   COALESCE(s.standardized_payload->>'Subcontinent', ''),
                   COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND NOT ({COUNTRY_PRESENT_SQL})
              AND (
                  {CONTINENT_PRESENT_SQL}
                  OR {SUBCONTINENT_PRESENT_SQL}
              )
            GROUP BY 1, 2 ORDER BY 3 DESC
            """,
            (snapshot_id,),
        ).fetchall()
        country_missing_geo = connection.execute(
            f"""
            SELECT COALESCE(s.standardized_payload->>'Country', ''),
                   COALESCE(s.standardized_payload->>'Continent', ''),
                   COALESCE(s.standardized_payload->>'Subcontinent', ''),
                   COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND {COUNTRY_PRESENT_SQL}
              AND (
                  NOT ({CONTINENT_PRESENT_SQL})
                  OR NOT ({SUBCONTINENT_PRESENT_SQL})
              )
            GROUP BY 1, 2, 3 ORDER BY 4 DESC
            """,
            (snapshot_id,),
        ).fetchall()
        unresolved_country = connection.execute(
            f"""
            SELECT {RAW_COUNTRY_SQL} AS raw_value, COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND {RAW_COUNTRY_PRESENT_SQL}
              AND NOT ({COUNTRY_PRESENT_SQL})
            GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 500
            """,
            (snapshot_id,),
        ).fetchall()
        unresolved_dates = connection.execute(
            f"""
            SELECT {RAW_COLLECTION_DATE_SQL} AS raw_value, COUNT(*)
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND {RAW_COLLECTION_DATE_PRESENT_SQL}
              AND NOT (COALESCE(s.standardized_payload->>'Collection Date', '') ~ '^[0-9]{{4}}$')
            GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 500
            """,
            (snapshot_id,),
        ).fetchall()

    non_country = [(country, count) for country, count in country_rows if country not in COUNTRY_MAPPING]
    mismatch_rows: list[list[Any]] = []
    continent_mismatch = 0
    subcontinent_mismatch = 0
    for country, continent, subcontinent, count in geography_pairs:
        expected = COUNTRY_MAPPING.get(str(country)) or {}
        if not expected:
            continue
        expected_continent = str(expected.get("Continent") or "")
        expected_subcontinent = str(expected.get("Subcontinent") or "")
        if str(continent) != expected_continent:
            continent_mismatch += int(count)
            mismatch_rows.append([country, "Continent", continent, expected_continent, count])
        if str(subcontinent) != expected_subcontinent:
            subcontinent_mismatch += int(count)
            mismatch_rows.append([country, "Subcontinent", subcontinent, expected_subcontinent, count])

    invalid_years: list[list[Any]] = []
    future_years: list[list[Any]] = []
    impossible_years: list[list[Any]] = []
    collection_year_counter: Counter[str] = Counter()
    for value, count in year_rows:
        if value.lower() in MISSING:
            continue
        if not re.fullmatch(r"\d{4}", value):
            invalid_years.append([value, count])
            continue
        year = int(value)
        collection_year_counter[value] += count
        if year > current_year:
            future_years.append([value, count])
        elif year < MIN_COLLECTION_YEAR:
            impossible_years.append([value, count])

    total = int(counts[0] or 0)
    metrics = {
        "total_rows_scanned": total,
        "country_present_count": int(counts[1] or 0),
        "country_present_percent": percent(int(counts[1] or 0), total),
        "continent_present_count": int(counts[2] or 0),
        "continent_present_percent": percent(int(counts[2] or 0), total),
        "subcontinent_present_count": int(counts[3] or 0),
        "subcontinent_present_percent": percent(int(counts[3] or 0), total),
        "collection_year_present_count": int(counts[4] or 0),
        "collection_year_present_percent": percent(int(counts[4] or 0), total),
        "raw_country_present_count": int(counts[5] or 0),
        "raw_country_present_percent": percent(int(counts[5] or 0), total),
        "raw_collection_date_present_count": int(counts[6] or 0),
        "raw_collection_date_present_percent": percent(int(counts[6] or 0), total),
        "standardized_only_rescued_country_rows": int(counts[7] or 0),
        "standardized_only_rescued_collection_year_rows": int(counts[8] or 0),
        "raw_only_unresolved_country_rows": int(counts[9] or 0),
        "raw_only_unresolved_collection_date_rows": int(counts[10] or 0),
        "neither_raw_nor_standardized_country_rows": int(counts[11] or 0),
        "neither_raw_nor_standardized_collection_year_rows": int(counts[12] or 0),
        "country_present_continent_missing_rows": int(counts[13] or 0),
        "country_present_subcontinent_missing_rows": int(counts[14] or 0),
        "continent_or_subcontinent_present_country_missing_rows": int(counts[15] or 0),
        "non_country_values_in_country_rows": sum(count for _value, count in non_country),
        "country_continent_mismatch_rows": continent_mismatch,
        "country_subcontinent_mismatch_rows": subcontinent_mismatch,
        "invalid_collection_year_rows": sum(int(row[1]) for row in invalid_years),
        "future_collection_year_rows": sum(int(row[1]) for row in future_years),
        "impossible_collection_year_rows": sum(int(row[1]) for row in impossible_years),
    }
    artifacts = [
        "geography_collection_date_qa_summary.json",
        "geography_collection_date_qa_summary.md",
        "country_counts.csv",
        "continent_counts.csv",
        "subcontinent_counts.csv",
        "country_continent_subcontinent_lookup_used.csv",
        "geography_mismatches.csv",
        "non_country_values_in_country.csv",
        "rows_country_present_but_continent_or_subcontinent_missing.csv",
        "rows_continent_or_subcontinent_present_but_country_missing.csv",
        "top_unresolved_raw_country_values.csv",
        "collection_year_counts.csv",
        "invalid_collection_years.csv",
        "future_collection_years.csv",
        "impossible_collection_years.csv",
        "top_unresolved_raw_collection_date_values.csv",
        "top_unresolved_raw_collection_year_values.csv",
    ]
    provenance = {
        "qa_timestamp": utc_now(),
        "qa_commit": git_commit(),
        "snapshot_id": snapshot_id,
        "country_lookup_file": str(COUNTRY_LOOKUP_PATH.relative_to(ROOT)),
        "country_lookup_sha256": sha256_file(COUNTRY_LOOKUP_PATH),
        "collection_date_parser_file": str(COLLECTION_PARSER_PATH.relative_to(ROOT)),
        "collection_date_parser_sha256": sha256_file(COLLECTION_PARSER_PATH),
        "collection_date_parser_policy": f"Collection Date normalized to integer year in range {MIN_COLLECTION_YEAR}-{current_year}; secondary recovery requires collection context and blocks publication/submission/accession/protocol dates.",
        "generated_artifacts": artifacts,
    }
    summary = {
        "status": "pass",
        "metrics": metrics,
        "provenance": provenance,
        "hard_failures": [],
    }

    hard_checks = {
        "non-country values in Country": metrics["non_country_values_in_country_rows"],
        "Country-Continent mismatches": metrics["country_continent_mismatch_rows"],
        "Country-Subcontinent mismatches": metrics["country_subcontinent_mismatch_rows"],
        "Country present but Continent missing": metrics["country_present_continent_missing_rows"],
        "Country present but Subcontinent missing": metrics["country_present_subcontinent_missing_rows"],
        "invalid Collection_Year rows": metrics["invalid_collection_year_rows"],
        "future Collection_Year rows": metrics["future_collection_year_rows"],
        "impossible Collection_Year rows": metrics["impossible_collection_year_rows"],
    }
    summary["hard_failures"] = [f"{label}: {count}" for label, count in hard_checks.items() if count]
    summary["status"] = "fail" if summary["hard_failures"] else "pass"

    write_csv(output_dir / "country_counts.csv", ["country", "assembly_count"], country_rows)
    write_csv(output_dir / "continent_counts.csv", ["continent", "assembly_count"], continent_rows)
    write_csv(output_dir / "subcontinent_counts.csv", ["subcontinent", "assembly_count"], subcontinent_rows)
    write_csv(
        output_dir / "country_continent_subcontinent_lookup_used.csv",
        ["country", "continent", "subcontinent"],
        [[country, values.get("Continent", ""), values.get("Subcontinent", "")] for country, values in sorted(COUNTRY_MAPPING.items())],
    )
    write_csv(output_dir / "geography_mismatches.csv", ["country", "field", "actual", "expected", "assembly_count"], mismatch_rows)
    write_csv(output_dir / "non_country_values_in_country.csv", ["value", "assembly_count"], non_country)
    write_csv(
        output_dir / "rows_country_present_but_continent_or_subcontinent_missing.csv",
        ["country", "continent", "subcontinent", "assembly_count"],
        country_missing_geo,
    )
    write_csv(
        output_dir / "rows_continent_or_subcontinent_present_but_country_missing.csv",
        ["continent", "subcontinent", "assembly_count"],
        missing_country_geo,
    )
    write_csv(output_dir / "top_unresolved_raw_country_values.csv", ["raw_country", "assembly_count"], unresolved_country)
    write_csv(output_dir / "collection_year_counts.csv", ["collection_year", "assembly_count"], collection_year_counter.most_common())
    write_csv(output_dir / "invalid_collection_years.csv", ["value", "assembly_count"], invalid_years)
    write_csv(output_dir / "future_collection_years.csv", ["value", "assembly_count"], future_years)
    write_csv(output_dir / "impossible_collection_years.csv", ["value", "assembly_count"], impossible_years)
    write_csv(output_dir / "top_unresolved_raw_collection_date_values.csv", ["raw_collection_date", "assembly_count"], unresolved_dates)
    unresolved_year_like = [
        [value, count] for value, count in unresolved_dates if re.search(r"\b\d{4}\b", str(value))
    ]
    write_csv(output_dir / "top_unresolved_raw_collection_year_values.csv", ["raw_collection_year_value", "assembly_count"], unresolved_year_like)

    summary_path = output_dir / "geography_collection_date_qa_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Geography and Collection-Date QA Summary",
        "",
        f"- Status: **{summary['status']}**",
        f"- Canonical snapshot: `{snapshot_id}`",
        f"- QA timestamp: `{provenance['qa_timestamp']}`",
        f"- Rows audited: {total:,}",
        f"- Country coverage: {metrics['country_present_count']:,} ({metrics['country_present_percent']}%)",
        f"- Continent coverage: {metrics['continent_present_count']:,} ({metrics['continent_present_percent']}%)",
        f"- Subcontinent coverage: {metrics['subcontinent_present_count']:,} ({metrics['subcontinent_present_percent']}%)",
        f"- Collection year coverage: {metrics['collection_year_present_count']:,} ({metrics['collection_year_present_percent']}%)",
        f"- Non-country values in Country: {metrics['non_country_values_in_country_rows']:,}",
        f"- Country/continent mismatches: {metrics['country_continent_mismatch_rows']:,}",
        f"- Country/subcontinent mismatches: {metrics['country_subcontinent_mismatch_rows']:,}",
        f"- Invalid/future/impossible collection years: {metrics['invalid_collection_year_rows'] + metrics['future_collection_year_rows'] + metrics['impossible_collection_year_rows']:,}",
        "",
        "Country and collection geography describe public repository metadata representation, not biological prevalence.",
    ]
    (output_dir / "geography_collection_date_qa_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (output_root / "latest.json").write_text(
        json.dumps({
            "snapshot_id": snapshot_id,
            "qa_timestamp": provenance["qa_timestamp"],
            "summary": str(summary_path.relative_to(output_root)),
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
