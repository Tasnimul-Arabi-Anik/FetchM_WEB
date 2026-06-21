#!/usr/bin/env python3
"""Run the hidden full Archaea inventory, metadata standardization, and audit pipeline."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_profiles import ARCHAEA_PROFILE, validate_snapshot_id_for_profile

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_ROOT = ROOT / "standardization" / "review" / "archaea_pilot"


def dated_output_dir(snapshot_id: str) -> Path:
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_snapshot = snapshot_id.replace("/", "_")
    return DEFAULT_REVIEW_ROOT / date / f"hidden_full_{safe_snapshot}"


def json_from_output(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def run_command(args: list[str]) -> tuple[dict[str, Any], str]:
    result = subprocess.run(args, check=True, capture_output=True, text=True)
    output = "\n".join(part for part in [result.stdout, result.stderr] if part)
    return json_from_output(output), output


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def write_summary(output_dir: Path, summary: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "hidden_archaea_pipeline_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Hidden Full Archaea Database Pipeline",
        "",
        f"Snapshot ID: `{summary['snapshot_id']}`",
        f"Generated: {summary['generated_at']}",
        "",
        "## Result",
        "",
        "This run builds a hidden Archaea canonical database snapshot and metadata analysis artifacts. It does not expose Archaea publicly, regenerate public Global Insights, or deploy.",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Inventory status | `{summary.get('inventory_status', '')}` |",
        f"| Root-unique assemblies | {int(summary.get('root_unique_assemblies') or 0):,} |",
        f"| Standardized assemblies | {int(summary.get('standardized_assemblies') or 0):,} |",
        f"| Missing standardized assemblies | {int(summary.get('missing_standardized_assemblies') or 0):,} |",
        f"| Rule-reuse review signals | {int(summary.get('rule_reuse_risk_rows') or 0):,} |",
        f"| High-risk rule-reuse signals | {int(summary.get('rule_reuse_high_risk_rows') or 0):,} |",
        f"| Pipeline pass | `{str(summary.get('pipeline_pass')).lower()}` |",
        "",
        "## Boundaries",
        "",
        "- Archaea remains hidden.",
        "- Public bacterial Global Insights are not regenerated.",
        "- Public UI and deployment are not touched.",
        "- This is a staging database pipeline for later review, not a NAR-facing public release.",
        "",
    ]
    (output_dir / "hidden_archaea_pipeline_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--request-workers", type=int, default=4)
    parser.add_argument("--standardization-workers", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=5.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--request-sleep", type=float, default=0.1)
    parser.add_argument("--top-limit", type=int, default=75)
    parser.add_argument("--skip-fetch", action="store_true", help="Run inventory and audit only; useful for resume/debug workflows.")
    parser.add_argument("--reuse-existing-snapshot", action="store_true", help="Skip inventory and metadata fetch; refresh audit/insights for an existing hidden Archaea snapshot.")
    args = parser.parse_args()

    snapshot_id = validate_snapshot_id_for_profile(
        args.snapshot_id or ARCHAEA_PROFILE.snapshot_id(datetime.now(timezone.utc)),
        ARCHAEA_PROFILE,
    )
    output_dir = args.output_dir or dated_output_dir(snapshot_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    inventory_json: dict[str, Any] = {}
    inventory_output = ""
    if not args.reuse_existing_snapshot:
        inventory_json, inventory_output = run_command([
            sys.executable, str(ROOT / "tools" / "build_canonical_root_inventory.py"),
            "--domain", "archaea",
            "--snapshot-id", snapshot_id,
            "--page-size", str(args.page_size),
            "--max-attempts", str(args.max_attempts),
            "--retry-sleep", str(args.retry_sleep),
            "--request-timeout", str(args.request_timeout),
        ])
    (output_dir / "inventory_command.log").write_text(inventory_output, encoding="utf-8")

    fetch_json: dict[str, Any] = {}
    fetch_output = ""
    if not args.skip_fetch and not args.reuse_existing_snapshot:
        fetch_json, fetch_output = run_command([
            sys.executable, str(ROOT / "tools" / "fetch_canonical_missing_metadata.py"),
            "--snapshot-id", snapshot_id,
            "--batch-size", str(args.batch_size),
            "--request-workers", str(args.request_workers),
            "--standardization-workers", str(args.standardization_workers),
            "--max-attempts", str(args.max_attempts),
            "--retry-sleep", str(args.retry_sleep),
            "--request-timeout", str(args.request_timeout),
            "--request-sleep", str(args.request_sleep),
            "--skip-host-monitoring",
        ])
    (output_dir / "metadata_fetch_command.log").write_text(fetch_output, encoding="utf-8")

    audit_json, audit_output = run_command([
        sys.executable, str(ROOT / "tools" / "audit_archaea_metadata_pilot.py"),
        "--snapshot-id", snapshot_id,
        "--output-dir", str(output_dir),
        "--top-limit", str(args.top_limit),
    ])
    (output_dir / "audit_command.log").write_text(audit_output, encoding="utf-8")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot_id,
        "domain_profile": "archaea",
        "visibility": "hidden_staging",
        "inventory_status": first_present(inventory_json.get("status"), inventory_json.get("snapshot_status"), audit_json.get("snapshot_status")),
        "root_unique_assemblies": first_present(audit_json.get("root_unique_assemblies"), fetch_json.get("root_unique_assemblies"), inventory_json.get("root_unique_assemblies")),
        "standardized_assemblies": first_present(audit_json.get("standardized_assemblies"), fetch_json.get("standardized_assemblies")),
        "missing_standardized_assemblies": first_present(audit_json.get("missing_standardized_assemblies"), fetch_json.get("missing_standardized_assemblies")),
        "rule_reuse_risk_rows": audit_json.get("rule_reuse_risk_rows"),
        "rule_reuse_high_risk_rows": audit_json.get("rule_reuse_high_risk_rows"),
        "audit_pass": bool(audit_json.get("audit_pass")),
        "public_ui_exposed": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
    }
    summary["pipeline_pass"] = (
        summary.get("inventory_status") == "completed"
        and int(summary.get("root_unique_assemblies") or 0) > 0
        and int(summary.get("missing_standardized_assemblies") or 0) == 0
        and int(summary.get("rule_reuse_high_risk_rows") or 0) == 0
        and bool(summary.get("audit_pass"))
    )
    write_summary(output_dir, summary)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["pipeline_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
