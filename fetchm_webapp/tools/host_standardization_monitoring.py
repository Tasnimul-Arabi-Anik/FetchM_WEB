#!/usr/bin/env python3
"""Generate host-standardization QA summaries and recurring review exports."""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect


DATA_ROOT = ROOT / "data" / "host_standardization_monitoring"
STANDARDIZATION_ROOT = ROOT / "standardization"
RULE_FILES = {
    "host_synonyms": STANDARDIZATION_ROOT / "host_synonyms.csv",
    "host_negative_rules": STANDARDIZATION_ROOT / "host_negative_rules.csv",
    "host_context_rules": STANDARDIZATION_ROOT / "host_context_rules.csv",
    "host_microbial_allowlist": STANDARDIZATION_ROOT / "host_microbial_allowlist.csv",
}
MISSING_HOST_TOKENS = (
    "",
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
    "unreported",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return sum(1 for _row in csv.DictReader(handle))


def file_sha256(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def host_rule_version() -> str:
    digest = hashlib.sha256()
    for name, path in RULE_FILES.items():
        digest.update(name.encode("utf-8"))
        if path.exists():
            digest.update(path.read_bytes())
    return f"sha256:{digest.hexdigest()[:16]}"


def read_allowlisted_taxids() -> list[str]:
    path = RULE_FILES["host_microbial_allowlist"]
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return sorted({
            str(row.get("taxid") or "").strip()
            for row in csv.DictReader(handle)
            if str(row.get("taxid") or "").strip().isdigit()
        })


def latest_active_snapshot_id() -> str:
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


def _write_query_csv(connection: Any, path: Path, query: str, params: tuple[Any, ...]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        fieldnames = [column.name for column in cursor.description]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(fieldnames)
        writer.writerows(rows)
    return len(rows)


def generate_host_monitoring(
    snapshot_id: str | None = None,
    *,
    output_root: Path | None = None,
    validation_sample: Path | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    snapshot_id = snapshot_id or latest_active_snapshot_id()
    output_root = output_root or DATA_ROOT
    output_dir = output_root / snapshot_id
    output_dir.mkdir(parents=True, exist_ok=True)
    allowlisted_taxids = read_allowlisted_taxids()

    with connect() as connection:
        counts = connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NOT NULL
                ) AS host_sd_nonempty,
                COUNT(*) FILTER (
                    WHERE NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_Context_SD', '')), '') IS NOT NULL
                ) AS host_context_nonempty,
                COUNT(*) FILTER (
                    WHERE LOWER(BTRIM(COALESCE(
                            s.standardized_payload->>'Host',
                            s.standardized_payload->>'Host_Original',
                            ''
                        ))) <> ALL(%s)
                      AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NULL
                      AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_Context_SD', '')), '') IS NULL
                      AND COALESCE(s.standardized_payload->>'Host_Review_Status', '') NOT IN (
                          'non_host_source', 'missing'
                      )
                ) AS unresolved_hosts,
                COUNT(*) FILTER (
                    WHERE COALESCE(s.standardized_payload->>'Host_Superkingdom', '') IN (
                        'Bacteria', 'Archaea', 'Viruses'
                    )
                      AND NOT (COALESCE(s.standardized_payload->>'Host_TaxID', '') = ANY(%s))
                ) AS microbial_leakage,
                COUNT(*) FILTER (
                    WHERE COALESCE(s.standardized_payload->>'Host_TaxID', '') = ANY(%s)
                      AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NOT NULL
                ) AS allowlisted_microbial_hosts
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            """,
            (list(MISSING_HOST_TOKENS), allowlisted_taxids, allowlisted_taxids, snapshot_id),
        ).fetchone()

        exports = {
            "top_unresolved_hosts.csv": _write_query_csv(
                connection,
                output_dir / "top_unresolved_hosts.csv",
                """
                SELECT COALESCE(NULLIF(BTRIM(s.standardized_payload->>'Host'), ''),
                                NULLIF(BTRIM(s.standardized_payload->>'Host_Original'), '')) AS raw_host,
                       COUNT(*) AS assembly_count
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s
                  AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NULL
                  AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_Context_SD', '')), '') IS NULL
                  AND COALESCE(s.standardized_payload->>'Host_Review_Status', '') NOT IN ('non_host_source', 'missing')
                  AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host',
                                            s.standardized_payload->>'Host_Original', '')), '') IS NOT NULL
                GROUP BY 1 ORDER BY assembly_count DESC, raw_host LIMIT %s
                """,
                (snapshot_id, limit),
            ),
            "top_context_only_hosts.csv": _write_query_csv(
                connection,
                output_dir / "top_context_only_hosts.csv",
                """
                SELECT COALESCE(NULLIF(BTRIM(s.standardized_payload->>'Host'), ''),
                                NULLIF(BTRIM(s.standardized_payload->>'Host_Original'), '')) AS raw_host,
                       s.standardized_payload->>'Host_Context_SD' AS host_context_sd,
                       COUNT(*) AS assembly_count
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s
                  AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NULL
                  AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_Context_SD', '')), '') IS NOT NULL
                GROUP BY 1, 2 ORDER BY assembly_count DESC, raw_host LIMIT %s
                """,
                (snapshot_id, limit),
            ),
            "top_negative_host_values.csv": _write_query_csv(
                connection,
                output_dir / "top_negative_host_values.csv",
                """
                SELECT COALESCE(NULLIF(BTRIM(s.standardized_payload->>'Host'), ''),
                                NULLIF(BTRIM(s.standardized_payload->>'Host_Original'), '')) AS raw_host,
                       COALESCE(NULLIF(s.standardized_payload->>'Host_Review_Status', ''),
                                s.standardized_payload->>'Host_SD_Method') AS decision,
                       COUNT(*) AS assembly_count
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s
                  AND COALESCE(s.standardized_payload->>'Host_Review_Status',
                               s.standardized_payload->>'Host_SD_Method', '') IN (
                      'non_host_source', 'missing', 'not_identifiable'
                  )
                GROUP BY 1, 2 ORDER BY assembly_count DESC, raw_host LIMIT %s
                """,
                (snapshot_id, limit),
            ),
            "host_review_candidates.csv": _write_query_csv(
                connection,
                output_dir / "host_review_candidates.csv",
                """
                SELECT raw_host, suggested_queue, host_context_sd, review_status, assembly_count
                FROM (
                    SELECT COALESCE(NULLIF(BTRIM(s.standardized_payload->>'Host'), ''),
                                    NULLIF(BTRIM(s.standardized_payload->>'Host_Original'), '')) AS raw_host,
                           CASE
                               WHEN NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_Context_SD', '')), '') IS NOT NULL
                                   THEN 'context_only'
                               WHEN COALESCE(s.standardized_payload->>'Host_Review_Status', '') = 'non_host_source'
                                   THEN 'negative'
                               ELSE 'unresolved'
                           END AS suggested_queue,
                           COALESCE(s.standardized_payload->>'Host_Context_SD', '') AS host_context_sd,
                           COALESCE(s.standardized_payload->>'Host_Review_Status', '') AS review_status,
                           COUNT(*) AS assembly_count
                    FROM bacterial_inventory_membership AS i
                    JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                    WHERE i.snapshot_id = %s
                      AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NULL
                      AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host',
                                                s.standardized_payload->>'Host_Original', '')), '') IS NOT NULL
                    GROUP BY 1, 2, 3, 4
                ) AS candidates
                ORDER BY assembly_count DESC, raw_host
                LIMIT %s
                """,
                (snapshot_id, limit),
            ),
        }

    validation_sample = validation_sample or ROOT / "data" / "host_validation_sample_600.csv"
    validation_size = 0
    if validation_sample.exists():
        with validation_sample.open(newline="", encoding="utf-8", errors="replace") as handle:
            validation_size = sum(1 for _row in csv.DictReader(handle))

    summary = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now(),
        "total_canonical_assemblies_standardized": int(counts[0] or 0),
        "host_sd_nonempty_count": int(counts[1] or 0),
        "host_context_sd_count": int(counts[2] or 0),
        "unresolved_host_count": int(counts[3] or 0),
        "microbial_leakage_count": int(counts[4] or 0),
        "allowlisted_microbial_host_count": int(counts[5] or 0),
        "validation_sample_size": validation_size,
        "validation_sample_filename": validation_sample.name if validation_sample.exists() else "",
        "validation_sample_sha256": file_sha256(validation_sample),
        "host_rule_version": host_rule_version(),
        "host_rule_commit": "b92a591",
        "rule_row_counts": {name: csv_row_count(path) for name, path in RULE_FILES.items()},
        "exports": exports,
    }
    summary_path = output_dir / "host_standardization_refresh_qa.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest = {
        "snapshot_id": snapshot_id,
        "summary": str(summary_path.relative_to(output_root)),
        "generated_at": summary["generated_at"],
    }
    (output_root / "latest.json").write_text(json.dumps(latest, indent=2) + "\n", encoding="utf-8")
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument("--output-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--validation-sample", type=Path)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    result = generate_host_monitoring(
        args.snapshot_id or None,
        output_root=args.output_root,
        validation_sample=args.validation_sample,
        limit=max(1, args.limit),
    )
    print(json.dumps(result, sort_keys=True))
