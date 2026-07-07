#!/usr/bin/env python3
"""QA gate for admin-hidden domain pipeline snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import (  # noqa: E402
    connect,
    domain_standardized_metadata_coverage,
    latest_domain_inventory_snapshot,
    normalize_domain_pipeline_key,
)

EXPECTED_DOMAIN_PROFILES = {
    "archaea": {
        "label": "Archaea",
        "profile": "archaea_hidden_v1",
        "release_status": "locked_admin_hidden",
        "visibility": "admin_hidden",
    },
    "virus": {
        "label": "Virus",
        "profile": "virus_hidden_v1",
        "release_status": "locked_admin_hidden",
        "visibility": "admin_hidden",
    },
}


def _scalar(row: Any, index: int, default: Any = None) -> Any:
    try:
        return row[index]
    except Exception:
        return default


def _check(checks: list[dict[str, Any]], key: str, passed: bool, detail: str, *, hard: bool = True) -> None:
    checks.append({"key": key, "status": "pass" if passed else ("fail" if hard else "warn"), "detail": detail, "hard": hard})


def collect_hidden_domain_qa(domain_key: str, snapshot_id: str | None = None) -> dict[str, Any]:
    key = normalize_domain_pipeline_key(domain_key)
    expected = EXPECTED_DOMAIN_PROFILES.get(key)
    if expected is None:
        raise ValueError(f"No hidden-domain QA profile is defined for {key!r}.")
    latest = latest_domain_inventory_snapshot(key) if not snapshot_id else None
    if not snapshot_id:
        if latest is None:
            raise RuntimeError(f"No hidden {key} inventory snapshot found.")
        snapshot_id = str(latest["snapshot_id"] or "")
    checks: list[dict[str, Any]] = []
    with connect() as connection:
        snapshot = connection.execute(
            """
            SELECT status, visibility, release_locked, root_unique_assemblies, raw_records,
                   noncanonical_records, duplicate_records, completed_at, error
            FROM domain_inventory_snapshot
            WHERE domain_key = %s AND snapshot_id = %s
            """,
            (key, snapshot_id),
        ).fetchone()
        inventory_task = connection.execute(
            """
            SELECT status, continue_after, completed_at, error
            FROM domain_inventory_task
            WHERE domain_key = %s AND snapshot_id = %s
            ORDER BY requested_at DESC LIMIT 1
            """,
            (key, snapshot_id),
        ).fetchone()
        metadata_task = connection.execute(
            """
            SELECT status, refetch_all, completed_at, error, summary_json
            FROM domain_metadata_fetch_task
            WHERE domain_key = %s AND snapshot_id = %s
            ORDER BY requested_at DESC LIMIT 1
            """,
            (key, snapshot_id),
        ).fetchone()
        payload_counts = connection.execute(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE standardized_payload->>'FetchM_Domain_Key' = %s) AS domain_key_tagged,
                   COUNT(*) FILTER (WHERE standardized_payload->>'FetchM_Domain' = %s) AS domain_label_tagged,
                   COUNT(*) FILTER (WHERE standardized_payload->>'FetchM_Domain_Profile' = %s) AS profile_tagged,
                   COUNT(*) FILTER (WHERE standardized_payload->>'FetchM_Public_Release_Status' = %s) AS release_status_tagged,
                   COUNT(*) FILTER (WHERE status = 'fetched_ncbi_full_report') AS fetched_status,
                   COUNT(*) FILTER (WHERE domain_key <> %s) AS invalid_domain_key
            FROM domain_assembly_standardization
            WHERE domain_key = %s
            """,
            (key, expected["label"], expected["profile"], expected["release_status"], key, key),
        ).fetchone()
    if snapshot is None:
        _check(checks, "snapshot_exists", False, f"Snapshot {snapshot_id} does not exist.")
        root_total = standardized = missing = 0
    else:
        root_total = int(_scalar(snapshot, 3, 0) or 0)
        coverage = domain_standardized_metadata_coverage(key, snapshot_id)
        standardized = int(coverage.get("standardized_assemblies") or 0)
        missing = int(coverage.get("missing_standardized_assemblies") or 0)
        _check(checks, "snapshot_completed", str(_scalar(snapshot, 0, "")) == "completed", f"status={_scalar(snapshot, 0, '')}")
        _check(checks, "visibility_admin_hidden", str(_scalar(snapshot, 1, "")) == expected["visibility"], f"visibility={_scalar(snapshot, 1, '')}")
        _check(checks, "release_locked", bool(_scalar(snapshot, 2, False)), f"release_locked={bool(_scalar(snapshot, 2, False))}")
        _check(checks, "root_nonempty", root_total > 0, f"root_unique_assemblies={root_total:,}")
        _check(checks, "raw_equals_root", int(_scalar(snapshot, 4, 0) or 0) == root_total, f"raw_records={int(_scalar(snapshot, 4, 0) or 0):,}; root={root_total:,}")
        _check(checks, "noncanonical_zero", int(_scalar(snapshot, 5, 0) or 0) == 0, f"noncanonical_records={int(_scalar(snapshot, 5, 0) or 0):,}")
        _check(checks, "duplicates_zero", int(_scalar(snapshot, 6, 0) or 0) == 0, f"duplicate_records={int(_scalar(snapshot, 6, 0) or 0):,}")
        _check(checks, "metadata_complete", standardized == root_total and missing == 0, f"standardized={standardized:,}; root={root_total:,}; missing={missing:,}")
    total_payloads = int(_scalar(payload_counts, 0, 0) or 0) if payload_counts else 0
    _check(checks, "payload_count_matches_root", total_payloads == root_total, f"payloads={total_payloads:,}; root={root_total:,}")
    _check(checks, "payload_domain_key", int(_scalar(payload_counts, 1, 0) or 0) == total_payloads, f"tagged={int(_scalar(payload_counts, 1, 0) or 0):,}; total={total_payloads:,}")
    _check(checks, "payload_domain_label", int(_scalar(payload_counts, 2, 0) or 0) == total_payloads, f"tagged={int(_scalar(payload_counts, 2, 0) or 0):,}; total={total_payloads:,}")
    _check(checks, "payload_profile", int(_scalar(payload_counts, 3, 0) or 0) == total_payloads, f"tagged={int(_scalar(payload_counts, 3, 0) or 0):,}; total={total_payloads:,}")
    _check(checks, "payload_release_status", int(_scalar(payload_counts, 4, 0) or 0) == total_payloads, f"tagged={int(_scalar(payload_counts, 4, 0) or 0):,}; total={total_payloads:,}")
    _check(checks, "payload_status", int(_scalar(payload_counts, 5, 0) or 0) == total_payloads, f"fetched_status={int(_scalar(payload_counts, 5, 0) or 0):,}; total={total_payloads:,}")
    _check(checks, "hidden_table_domain_isolated", int(_scalar(payload_counts, 6, 0) or 0) == 0, f"invalid_domain_key_rows={int(_scalar(payload_counts, 6, 0) or 0):,}")
    _check(checks, "inventory_task_completed", inventory_task is not None and str(_scalar(inventory_task, 0, "")) == "completed", f"status={_scalar(inventory_task, 0, 'missing') if inventory_task else 'missing'}")
    _check(checks, "metadata_task_completed", metadata_task is not None and str(_scalar(metadata_task, 0, "")) == "completed", f"status={_scalar(metadata_task, 0, 'missing') if metadata_task else 'missing'}")
    hard_failures = [check for check in checks if check["hard"] and check["status"] == "fail"]
    return {
        "domain_key": key,
        "snapshot_id": snapshot_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass" if not hard_failures else "fail",
        "hard_failure_count": len(hard_failures),
        "root_unique_assemblies": root_total,
        "standardized_assemblies": standardized,
        "missing_standardized_assemblies": missing,
        "payload_rows": total_payloads,
        "checks": checks,
        "hard_failures": hard_failures,
        "release_locked": True,
        "public_enabled": False,
    }


def write_outputs(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "qa_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    checks = "\n".join(f"- `{check['key']}`: {check['status']} - {check['detail']}" for check in summary["checks"])
    markdown = (
        f"# Hidden Domain QA Summary\n\n"
        f"- Domain: `{summary['domain_key']}`\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Status: `{summary['status']}`\n"
        f"- Hard failures: {summary['hard_failure_count']}\n"
        f"- Root assemblies: {summary['root_unique_assemblies']:,}\n"
        f"- Standardized assemblies: {summary['standardized_assemblies']:,}\n"
        f"- Missing standardized assemblies: {summary['missing_standardized_assemblies']:,}\n"
        f"- Public enabled: false\n"
        f"- Release locked: true\n\n"
        f"## Checks\n\n{checks}\n"
    )
    (output_dir / "qa_summary.md").write_text(markdown, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--domain", default="archaea")
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args(argv)
    summary = collect_hidden_domain_qa(args.domain, args.snapshot_id)
    if args.output_dir:
        write_outputs(summary, Path(args.output_dir))
    print(json.dumps({
        "domain_key": summary["domain_key"],
        "snapshot_id": summary["snapshot_id"],
        "status": summary["status"],
        "hard_failure_count": summary["hard_failure_count"],
        "root_unique_assemblies": summary["root_unique_assemblies"],
        "standardized_assemblies": summary["standardized_assemblies"],
        "missing_standardized_assemblies": summary["missing_standardized_assemblies"],
    }, indent=2))
    if args.fail_on_hard_errors and summary["hard_failure_count"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
