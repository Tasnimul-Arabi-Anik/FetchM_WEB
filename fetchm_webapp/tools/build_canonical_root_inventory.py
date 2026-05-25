#!/usr/bin/env python3
"""Stream the canonical GenBank bacterial assembly inventory into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import (
    BACTERIA_TAXON_ID,
    fail_inventory_snapshot,
    finish_inventory_snapshot,
    insert_inventory_batch,
    start_inventory_snapshot,
)

def parse_reports(line: str) -> Iterable[dict[str, Any]]:
    payload = json.loads(line)
    reports = payload.get('reports') if isinstance(payload, dict) else None
    if isinstance(reports, list):
        for report in reports:
            if isinstance(report, dict):
                yield report
    elif isinstance(payload, dict):
        yield payload

def datasets_version(binary: str) -> str:
    try:
        result = subprocess.run([binary, 'version'], capture_output=True, text=True, timeout=20, check=False)
    except (OSError, subprocess.SubprocessError):
        return 'unknown'
    value = (result.stdout or result.stderr or '').strip().splitlines()
    return value[0] if value else 'unknown'

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', default=None)
    parser.add_argument('--datasets-bin', default='datasets')
    parser.add_argument('--batch-size', type=int, default=1000)
    args = parser.parse_args()
    snapshot_id = args.snapshot_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ_genbank_bacteria_root')
    command = [args.datasets_bin, 'summary', 'genome', 'taxon', str(BACTERIA_TAXON_ID), '--assembly-source', 'genbank', '--as-json-lines']
    invocation = ' '.join(command)
    start_inventory_snapshot(snapshot_id, invocation, datasets_version(args.datasets_bin))
    raw_records = noncanonical = duplicates = 0
    batch: list[dict[str, Any]] = []
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        assert process.stdout is not None
        for line in process.stdout:
            if not line.strip():
                continue
            for report in parse_reports(line):
                raw_records += 1
                batch.append(report)
                if len(batch) >= args.batch_size:
                    _, invalid, repeated = insert_inventory_batch(snapshot_id, batch)
                    noncanonical += invalid
                    duplicates += repeated
                    batch.clear()
        if batch:
            _, invalid, repeated = insert_inventory_batch(snapshot_id, batch)
            noncanonical += invalid
            duplicates += repeated
        stderr = process.stderr.read() if process.stderr is not None else ''
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f'datasets inventory command failed with return code {return_code}: {stderr[-1000:]}')
        summary = finish_inventory_snapshot(snapshot_id, raw_records, noncanonical, duplicates)
        print(json.dumps({'snapshot_id': snapshot_id, **summary}, sort_keys=True))
        return 0 if summary.get('status') == 'completed' else 1
    except Exception as exc:
        fail_inventory_snapshot(snapshot_id, str(exc))
        raise

if __name__ == '__main__':
    raise SystemExit(main())
