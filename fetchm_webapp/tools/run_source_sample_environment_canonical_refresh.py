#!/usr/bin/env python3
"""Refresh canonical source/sample/environment fields and run canonical QA.

This is the release-consolidation wrapper for reviewed source/sample/environment
curation batches. It reuses the existing canonical secondary-only refresh path and
then writes the standard canonical QA artifact set.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.qa_source_sample_environment_standardization import generate_qa, latest_snapshot_id


def run_command(command: list[str]) -> dict[str, Any]:
    completed = subprocess.run(command, cwd=ROOT.parent, text=True, capture_output=True, check=False)
    payload: dict[str, Any] = {
        "command": command,
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-8000:],
        "stderr_tail": completed.stderr[-8000:],
    }
    if completed.returncode != 0:
        raise RuntimeError(json.dumps(payload, indent=2))
    try:
        payload["json"] = json.loads(completed.stdout.strip().splitlines()[-1])
    except Exception:
        payload["json"] = {}
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument("--output-root", type=Path, default=ROOT / "data" / "source_sample_environment_qa")
    parser.add_argument("--run-date", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--standardization-workers", type=int, default=8)
    parser.add_argument("--skip-restandardize", action="store_true", help="Run QA only; useful for failed reruns after refresh completed.")
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args()

    snapshot_id = args.snapshot_id or latest_snapshot_id()
    refresh_result: dict[str, Any] = {"skipped": True}
    if not args.skip_restandardize:
        refresh_result = run_command([
            sys.executable,
            str(ROOT / "tools" / "restandardize_canonical_metadata.py"),
            "--snapshot-id", snapshot_id,
            "--secondary-only",
            "--batch-size", str(args.batch_size),
            "--standardization-workers", str(args.standardization_workers),
        ])

    run_date = datetime.strptime(args.run_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    qa_summary = generate_qa(snapshot_id, output_root=args.output_root, run_date=run_date)
    output_dir = args.output_root / args.run_date
    wrapper_summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot_id,
        "secondary_only_refresh": refresh_result,
        "qa_status": qa_summary.get("status"),
        "qa_summary_path": str(output_dir / "source_sample_environment_qa_summary.json"),
        "hard_failures": qa_summary.get("hard_failures") or [],
    }
    (output_dir / "canonical_refresh_wrapper_summary.json").write_text(json.dumps(wrapper_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(wrapper_summary, sort_keys=True))
    return 1 if args.fail_on_hard_errors and wrapper_summary["hard_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
