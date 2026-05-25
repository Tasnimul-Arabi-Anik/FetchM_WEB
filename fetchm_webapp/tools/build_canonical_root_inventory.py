#!/usr/bin/env python3
"""Build the canonical GenBank bacterial assembly inventory with restartable date chunks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import (
    BACTERIA_TAXON_ID,
    completed_inventory_chunks,
    fail_inventory_snapshot,
    finish_inventory_chunk,
    finish_inventory_snapshot,
    insert_inventory_batch,
    inventory_chunk_progress,
    start_inventory_chunk,
    start_inventory_snapshot,
)


def parse_reports(line: str) -> Iterable[dict[str, Any]]:
    payload = json.loads(line)
    reports = payload.get("reports") if isinstance(payload, dict) else None
    if isinstance(reports, list):
        for report in reports:
            if isinstance(report, dict):
                yield report
    elif isinstance(payload, dict):
        yield payload


def datasets_version(binary: str) -> str:
    try:
        result = subprocess.run([binary, "version"], capture_output=True, text=True, timeout=20, check=False)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    value = (result.stdout or result.stderr or "").strip().splitlines()
    return value[0] if value else "unknown"


def month_end(value: date) -> date:
    return value.replace(day=monthrange(value.year, value.month)[1])


def inventory_windows(last_day: date) -> list[tuple[str, date, date]]:
    """Use broad historical chunks and monthly recent chunks to bound API streams."""
    windows: list[tuple[str, date, date]] = []
    for year in range(1900, 2015):
        windows.append((f"year-{year}", date(year, 1, 1), date(year, 12, 31)))
    cursor = date(2015, 1, 1)
    while cursor <= last_day:
        end = min(month_end(cursor), last_day)
        windows.append((f"month-{cursor:%Y-%m}", cursor, end))
        cursor = end + timedelta(days=1)
    return windows


def query_bounds(start: date, end: date) -> tuple[str, str]:
    # The previous-day overlap avoids missing boundary records if NCBI treats --released-after as exclusive.
    after = start - timedelta(days=1) if start > date(1900, 1, 1) else start
    return after.isoformat(), end.isoformat()


def fetch_chunk(
    args: argparse.Namespace,
    snapshot_id: str,
    chunk_key: str,
    start: date,
    end: date,
) -> dict[str, int]:
    released_after, released_before = query_bounds(start, end)
    command = [
        args.datasets_bin,
        "summary", "genome", "taxon", str(BACTERIA_TAXON_ID),
        "--assembly-source", "genbank",
        "--assembly-version", "latest",
        "--released-after", released_after,
        "--released-before", released_before,
        "--limit", "all",
        "--as-json-lines",
    ]
    last_error = ""
    for attempt in range(1, max(1, args.max_attempts) + 1):
        start_inventory_chunk(snapshot_id, chunk_key, released_after, released_before)
        raw_records = canonical = noncanonical = duplicates = 0
        batch: list[dict[str, Any]] = []
        process = subprocess.Popen(
            ["timeout", "--signal=TERM", str(args.chunk_timeout), *command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            if not line.strip():
                continue
            for report in parse_reports(line):
                raw_records += 1
                batch.append(report)
                if len(batch) >= args.batch_size:
                    written, invalid, repeated = insert_inventory_batch(snapshot_id, batch)
                    canonical += written
                    noncanonical += invalid
                    duplicates += repeated
                    batch.clear()
        if batch:
            written, invalid, repeated = insert_inventory_batch(snapshot_id, batch)
            canonical += written
            noncanonical += invalid
            duplicates += repeated
        stderr = process.stderr.read() if process.stderr is not None else ""
        return_code = process.wait()
        if return_code == 0:
            finish_inventory_chunk(
                snapshot_id,
                chunk_key,
                "completed",
                raw_records=raw_records,
                canonical_records=canonical,
                noncanonical_records=noncanonical,
                duplicate_records=duplicates,
            )
            return {
                "raw_records": raw_records,
                "canonical_records": canonical,
                "noncanonical_records": noncanonical,
                "duplicate_records": duplicates,
            }
        last_error = (
            f"chunk {chunk_key} attempt {attempt}/{args.max_attempts} failed "
            f"with return code {return_code}: {stderr[-1000:]}"
        )
        finish_inventory_chunk(
            snapshot_id,
            chunk_key,
            "failed",
            raw_records=raw_records,
            canonical_records=canonical,
            noncanonical_records=noncanonical,
            duplicate_records=duplicates,
            error=last_error,
        )
        print(last_error, file=sys.stderr, flush=True)
        if attempt < max(1, args.max_attempts):
            time.sleep(max(0.0, args.retry_sleep))
    raise RuntimeError(last_error or f"chunk {chunk_key} failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--datasets-bin", default="datasets")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-sleep", type=float, default=20.0)
    parser.add_argument("--chunk-timeout", type=int, default=1800)
    parser.add_argument("--through-date", default=None, help="Last release date to query (YYYY-MM-DD; defaults to today).")
    args = parser.parse_args()
    snapshot_id = args.snapshot_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_genbank_bacteria_root")
    through_date = date.fromisoformat(args.through_date) if args.through_date else date.today()
    invocation = (
        f"{args.datasets_bin} summary genome taxon {BACTERIA_TAXON_ID} --assembly-source genbank "
        "--assembly-version latest --released-after/--released-before <chunk> --limit all --as-json-lines"
    )
    start_inventory_snapshot(snapshot_id, invocation, datasets_version(args.datasets_bin))
    completed = completed_inventory_chunks(snapshot_id)
    try:
        for chunk_key, start, end in inventory_windows(through_date):
            if chunk_key in completed:
                continue
            fetch_chunk(args, snapshot_id, chunk_key, start, end)
        progress = inventory_chunk_progress(snapshot_id)
        summary = finish_inventory_snapshot(
            snapshot_id,
            progress["raw_records"],
            progress["noncanonical_records"],
            progress["duplicate_records"],
        )
        summary.update(progress)
        summary["inventory_mode"] = "release_date_chunks"
        print(json.dumps({"snapshot_id": snapshot_id, **summary}, sort_keys=True))
        return 0 if summary.get("status") == "completed" else 1
    except Exception as exc:
        fail_inventory_snapshot(snapshot_id, str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
