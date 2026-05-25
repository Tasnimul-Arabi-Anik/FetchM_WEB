#!/usr/bin/env python3
"""Materialize canonical bacterial root partitions into PostgreSQL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import materialize_partitions_from_inventory

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--dataset-version-id', required=True)
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument(
        '--release-views-materialized',
        action='store_true',
        help='Mark reconciliation as backed by materialized public release views. Preview partition runs should not use this.',
    )
    args = parser.parse_args()
    summary = materialize_partitions_from_inventory(
        args.snapshot_id,
        args.dataset_version_id,
        batch_size=args.batch_size,
        release_views_materialized=args.release_views_materialized,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get('status') == 'pass' else 1

if __name__ == '__main__':
    raise SystemExit(main())
