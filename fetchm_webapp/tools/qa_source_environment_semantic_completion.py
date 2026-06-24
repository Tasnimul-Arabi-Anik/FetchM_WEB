#!/usr/bin/env python3
"""QA gate for the Source/Environment Semantic Completion release."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.source_environment_semantic_completion import DEFAULT_OUTPUT_ROOT, DEFAULT_SNAPSHOT_ID, run


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT / "qa_latest")
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args()
    summary = run(args.snapshot_id, args.output_dir, apply=False)
    print(json.dumps(summary, sort_keys=True))
    if args.fail_on_hard_errors and summary.get("hard_failures"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
