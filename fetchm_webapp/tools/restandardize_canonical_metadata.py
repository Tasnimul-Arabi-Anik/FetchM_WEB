#!/usr/bin/env python3
"""Re-standardize canonical bacterial metadata with the currently loaded rules."""

from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import normalize_managed_metadata_row
from dataset_production_store import connect, seed_standardized_metadata_batch, standardized_metadata_coverage
from global_insights.generator import standardization_rule_manifest


def current_rule_fingerprint(default: str | None = None) -> str:
    if default:
        return default
    return str(standardization_rule_manifest().get("version") or "not available")


def rows_to_restandardize(snapshot_id: str, last_accession: str, limit: int) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT i.assembly_accession, s.standardized_payload
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND i.assembly_accession > %s
            ORDER BY i.assembly_accession
            LIMIT %s
            """,
            (snapshot_id, last_accession, int(limit)),
        ).fetchall()
    payloads: list[dict[str, Any]] = []
    for accession, payload in rows:
        row = dict(payload or {})
        row["Assembly Accession"] = row.get("Assembly Accession") or str(accession)
        payloads.append(row)
    return payloads


def restandardize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized, _report = normalize_managed_metadata_row(row, force_standardization=True)
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--max-batches", type=int, default=0, help="Limit batches for validation runs only.")
    parser.add_argument("--standardization-workers", type=int, default=0)
    parser.add_argument("--rule-fingerprint", default="")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 50000:
        parser.error("--batch-size must be between 1 and 50000")
    worker_default = int(os.environ.get("FETCHM_WEBAPP_CANONICAL_STANDARDIZATION_WORKERS", "10") or "10")
    workers = min(32, max(1, args.standardization_workers or worker_default))
    rule_fingerprint = current_rule_fingerprint(args.rule_fingerprint or None)
    processed = updated = batches = 0
    last_accession = ""
    executor = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None
    try:
        while True:
            if args.max_batches and batches >= args.max_batches:
                break
            payloads = rows_to_restandardize(args.snapshot_id, last_accession, args.batch_size)
            if not payloads:
                break
            last_accession = str(payloads[-1].get("Assembly Accession") or last_accession)
            if executor is None or len(payloads) <= 1:
                standardized = [restandardize_row(row) for row in payloads]
            else:
                standardized = list(executor.map(restandardize_row, payloads, chunksize=max(1, len(payloads) // (workers * 4))))
            result = seed_standardized_metadata_batch(
                args.snapshot_id,
                standardized,
                rule_fingerprint=rule_fingerprint,
                status="restandardized_current_rules",
            )
            processed += len(payloads)
            updated += int(result.get("seeded") or 0)
            batches += 1
            if batches % 20 == 0:
                print(json.dumps({"batches_complete": batches, "processed_rows": processed, "updated_rows": updated, "standardization_workers": workers}, sort_keys=True), flush=True)
    finally:
        if executor is not None:
            executor.shutdown()
    summary = {
        "snapshot_id": args.snapshot_id,
        "standardization_status": "restandardized_current_rules",
        "rule_fingerprint": rule_fingerprint,
        "batches_complete": batches,
        "processed_rows": processed,
        "updated_rows": updated,
        "standardization_workers": workers,
        **standardized_metadata_coverage(args.snapshot_id),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
