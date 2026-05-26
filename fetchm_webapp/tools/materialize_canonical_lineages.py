#!/usr/bin/env python3
"""Materialize seven-rank NCBI taxonomy lineages for a staged canonical bacterial snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import materialize_taxonomy_lineage_from_inventory


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    args = parser.parse_args()
    summary = materialize_taxonomy_lineage_from_inventory(args.snapshot_id)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
