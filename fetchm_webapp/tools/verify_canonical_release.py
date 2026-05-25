#!/usr/bin/env python3
"""Verify that a canonical bacterial inventory is fully represented by release partitions."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataset_production_store import reconcile_root_partitions

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--dataset-version-id', required=True)
    args = parser.parse_args()
    summary = reconcile_root_partitions(args.snapshot_id, args.dataset_version_id)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary['status'] == 'pass' else 1
if __name__ == '__main__':
    raise SystemExit(main())
