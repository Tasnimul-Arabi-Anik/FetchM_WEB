#!/usr/bin/env python3
"""Release-candidate gate for the hidden admin-only Archaea database.

This gate proves that the hidden Archaea pipeline is ready for admin review
without changing public release state. It intentionally never authorizes public
release; unlocking Archaea remains a separate manual decision.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import (  # noqa: E402
    domain_standardized_metadata_coverage,
    domain_taxon_metadata_csv,
    domain_taxon_report,
    domain_taxon_search_results,
    latest_domain_inventory_snapshot,
)
from domain_profiles import domain_profile_contract  # noqa: E402
from tools.qa_hidden_domain_pipeline import collect_hidden_domain_qa  # noqa: E402

DOMAIN_KEY = "archaea"
DEFAULT_QUERY = "Methano"


def _check(checks: list[dict[str, Any]], key: str, passed: bool, detail: str, *, hard: bool = True) -> None:
    checks.append({
        "key": key,
        "status": "pass" if passed else ("fail" if hard else "warn"),
        "detail": detail,
        "hard": hard,
    })


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _resolve_snapshot_id(snapshot_id: str | None) -> str:
    if snapshot_id:
        return snapshot_id
    latest = latest_domain_inventory_snapshot(DOMAIN_KEY)
    if not latest:
        raise RuntimeError("No hidden Archaea inventory snapshot found.")
    resolved = str(latest.get("snapshot_id") or "")
    if not resolved:
        raise RuntimeError("Latest hidden Archaea inventory snapshot has no snapshot_id.")
    return resolved


def collect_archaea_release_candidate(snapshot_id: str | None = None, *, query: str = DEFAULT_QUERY) -> dict[str, Any]:
    snapshot_id = _resolve_snapshot_id(snapshot_id)
    generated_at = datetime.now(timezone.utc).isoformat()
    checks: list[dict[str, Any]] = []

    contract = domain_profile_contract(DOMAIN_KEY)
    _check(
        checks,
        "profile_is_archaea_hidden_v1",
        contract.get("profile") == "archaea_hidden_v1",
        f"profile={contract.get('profile')}",
    )
    _check(
        checks,
        "public_release_disabled",
        contract.get("public_enabled") is False,
        f"public_enabled={contract.get('public_enabled')}",
    )
    _check(
        checks,
        "release_locked",
        contract.get("release_locked") is True,
        f"release_locked={contract.get('release_locked')}",
    )
    _check(
        checks,
        "prokaryote_record_model",
        contract.get("primary_record_model") == "prokaryote_assembly",
        f"primary_record_model={contract.get('primary_record_model')}",
    )

    hidden_qa = collect_hidden_domain_qa(DOMAIN_KEY, snapshot_id)
    _check(
        checks,
        "hidden_domain_qa_passes",
        hidden_qa.get("status") == "pass" and _int(hidden_qa.get("hard_failure_count")) == 0,
        f"status={hidden_qa.get('status')}; hard_failures={_int(hidden_qa.get('hard_failure_count'))}",
    )

    coverage = domain_standardized_metadata_coverage(DOMAIN_KEY, snapshot_id)
    root_total = _int(coverage.get("root_unique_assemblies"))
    standardized = _int(coverage.get("standardized_assemblies"))
    missing = _int(coverage.get("missing_standardized_assemblies"))
    _check(checks, "root_nonempty", root_total > 0, f"root_unique_assemblies={root_total:,}")
    _check(
        checks,
        "standardization_complete",
        root_total > 0 and standardized == root_total and missing == 0,
        f"standardized={standardized:,}; root={root_total:,}; missing={missing:,}",
    )

    search_results = domain_taxon_search_results(DOMAIN_KEY, query, snapshot_id=snapshot_id, limit=20)
    top_result = search_results[0] if search_results else None
    _check(checks, "admin_taxon_search_returns_results", bool(top_result), f"query={query!r}; results={len(search_results)}")

    report: dict[str, Any] | None = None
    csv_export: dict[str, Any] | None = None
    if top_result:
        rank = str(top_result.get("rank") or "")
        name = str(top_result.get("name") or "")
        report = domain_taxon_report(DOMAIN_KEY, rank, name, snapshot_id=snapshot_id)
        csv_export = domain_taxon_metadata_csv(DOMAIN_KEY, rank, name, snapshot_id=snapshot_id)
        expected_count = _int(top_result.get("genome_count"))
        report_rows = _int(report.get("row_count") if report else 0)
        csv_rows = _int(csv_export.get("row_count") if csv_export else 0)
        _check(
            checks,
            "admin_taxon_report_available",
            report is not None and report_rows > 0,
            f"rank={rank}; name={name}; row_count={report_rows:,}",
        )
        _check(
            checks,
            "admin_taxon_report_matches_search_count",
            report is not None and report_rows == expected_count,
            f"search_count={expected_count:,}; report_rows={report_rows:,}",
        )
        _check(
            checks,
            "admin_taxon_report_hidden_locked",
            report is not None and report.get("public_enabled") is False and report.get("release_locked") is True,
            f"public_enabled={report.get('public_enabled') if report else None}; release_locked={report.get('release_locked') if report else None}",
        )
        _check(
            checks,
            "admin_metadata_csv_available",
            csv_export is not None and csv_rows > 0,
            f"row_count={csv_rows:,}",
        )
        _check(
            checks,
            "admin_metadata_csv_matches_report",
            report is not None and csv_export is not None and csv_rows == report_rows,
            f"csv_rows={csv_rows:,}; report_rows={report_rows:,}",
        )

    hard_failures = [check for check in checks if check["hard"] and check["status"] == "fail"]
    release_candidate_ready = not hard_failures
    return {
        "domain_key": DOMAIN_KEY,
        "snapshot_id": snapshot_id,
        "generated_at": generated_at,
        "status": "pass" if release_candidate_ready else "fail",
        "release_candidate_ready": release_candidate_ready,
        "safe_to_publicly_release": False,
        "manual_public_release_required": True,
        "public_enabled": False,
        "release_locked": True,
        "hard_failure_count": len(hard_failures),
        "root_unique_assemblies": root_total,
        "standardized_assemblies": standardized,
        "missing_standardized_assemblies": missing,
        "hidden_domain_qa": {
            "status": hidden_qa.get("status"),
            "hard_failure_count": _int(hidden_qa.get("hard_failure_count")),
            "payload_rows": _int(hidden_qa.get("payload_rows")),
        },
        "admin_validation": {
            "query": query,
            "result_count": len(search_results),
            "top_result": top_result or {},
            "taxon_report_rows": _int(report.get("row_count") if report else 0),
            "metadata_csv_rows": _int(csv_export.get("row_count") if csv_export else 0),
            "report_release_locked": bool(report.get("release_locked")) if report else False,
            "report_public_enabled": bool(report.get("public_enabled")) if report else False,
            "metadata_csv_filename": str(csv_export.get("filename") or "") if csv_export else "",
        },
        "checks": checks,
        "hard_failures": hard_failures,
        "safety": {
            "bacterial_canonical_metadata_changed": False,
            "global_insights_regenerated": False,
            "public_archaea_routes_enabled": False,
            "bucket_b_environment_rules_applied": False,
            "virus_database_build_run": False,
        },
        "remaining_public_release_blockers": [
            "manual public release approval",
            "public route and Global Insights activation explicitly not enabled by this gate",
            "public manuscript/readiness thresholds remain a separate reviewed task",
        ],
        "decision": (
            "Hidden Archaea release-candidate prechecks pass; public release remains locked."
            if release_candidate_ready
            else "Hidden Archaea release-candidate prechecks failed; public release remains locked."
        ),
    }


def write_outputs(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "archaea_release_candidate_gate.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checks = "\n".join(f"- `{check['key']}`: {check['status']} - {check['detail']}" for check in summary["checks"])
    failures = "\n".join(f"- `{check['key']}` - {check['detail']}" for check in summary["hard_failures"]) or "- None"
    top = summary.get("admin_validation", {}).get("top_result", {}) or {}
    markdown = (
        "# Hidden Archaea Release-Candidate Gate\n\n"
        f"- Status: `{summary['status']}`\n"
        f"- Release candidate ready for admin review: {str(summary['release_candidate_ready']).lower()}\n"
        f"- Safe to publicly release: {str(summary['safe_to_publicly_release']).lower()}\n"
        f"- Manual public release required: {str(summary['manual_public_release_required']).lower()}\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Root assemblies: {summary['root_unique_assemblies']:,}\n"
        f"- Standardized assemblies: {summary['standardized_assemblies']:,}\n"
        f"- Missing standardized assemblies: {summary['missing_standardized_assemblies']:,}\n"
        f"- Hard failures: {summary['hard_failure_count']}\n"
        "- Public enabled: false\n"
        "- Release locked: true\n\n"
        "## Admin Validation\n\n"
        f"- Search query: `{summary['admin_validation']['query']}`\n"
        f"- Top result: `{top.get('name', '')}` ({top.get('rank', '')}), {int(top.get('genome_count') or 0):,} genomes\n"
        f"- Taxon report rows: {summary['admin_validation']['taxon_report_rows']:,}\n"
        f"- Metadata CSV rows: {summary['admin_validation']['metadata_csv_rows']:,}\n"
        f"- Metadata CSV: `{summary['admin_validation']['metadata_csv_filename']}`\n\n"
        "## Checks\n\n"
        f"{checks}\n\n"
        "## Hard Failures\n\n"
        f"{failures}\n\n"
        "## Decision\n\n"
        f"{summary['decision']} Public release remains locked; this gate does not enable public routes, deploy Archaea, or regenerate Global Insights.\n"
    )
    (output_dir / "archaea_release_candidate_gate.md").write_text(markdown, encoding="utf-8")

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args(argv)
    summary = collect_archaea_release_candidate(args.snapshot_id, query=args.query)
    if args.output_dir:
        write_outputs(summary, Path(args.output_dir))
    print(json.dumps({
        "domain_key": summary["domain_key"],
        "snapshot_id": summary["snapshot_id"],
        "status": summary["status"],
        "release_candidate_ready": summary["release_candidate_ready"],
        "safe_to_publicly_release": summary["safe_to_publicly_release"],
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
