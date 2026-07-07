#!/usr/bin/env python3
"""Release-candidate gate for the hidden admin-only Virus sequence model.

This validates the hidden Virus sequence/genome-group/relationship model and
admin report/export behavior. It never authorizes public Virus release.
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
    domain_taxon_metadata_csv,
    domain_taxon_report,
    domain_taxon_search_results,
    hidden_virus_model_summary,
)
from domain_profiles import domain_profile_contract  # noqa: E402
from tools.qa_hidden_virus_pipeline import collect_hidden_virus_qa  # noqa: E402

DOMAIN_KEY = "virus"
DEFAULT_QUERY = "Influenza"


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


def collect_virus_release_candidate(snapshot_id: str, *, query: str = DEFAULT_QUERY) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    checks: list[dict[str, Any]] = []

    contract = domain_profile_contract(DOMAIN_KEY)
    _check(checks, "profile_is_virus_hidden_v1", contract.get("profile") == "virus_hidden_v1", f"profile={contract.get('profile')}")
    _check(checks, "public_release_disabled", contract.get("public_enabled") is False, f"public_enabled={contract.get('public_enabled')}")
    _check(checks, "release_locked", contract.get("release_locked") is True, f"release_locked={contract.get('release_locked')}")
    _check(
        checks,
        "virus_sequence_record_model",
        contract.get("primary_record_model") == "virus_sequence_or_assembly_surrogate",
        f"primary_record_model={contract.get('primary_record_model')}",
    )

    qa = collect_hidden_virus_qa(snapshot_id)
    _check(
        checks,
        "hidden_virus_qa_passes",
        qa.get("status") == "pass" and _int(qa.get("hard_failure_count")) == 0,
        f"status={qa.get('status')}; hard_failures={_int(qa.get('hard_failure_count'))}",
    )

    model = hidden_virus_model_summary(snapshot_id=snapshot_id)
    sequence_records = _int(model.get("virus_sequence_records"))
    genome_groups = _int(model.get("virus_genome_groups"))
    relationships = _int(model.get("taxon_relationships"))
    _check(checks, "virus_sequence_rows_present", sequence_records > 0, f"virus_sequence_records={sequence_records:,}")
    _check(checks, "virus_genome_groups_present", genome_groups > 0, f"virus_genome_groups={genome_groups:,}")
    _check(checks, "virus_host_relationships_present", relationships > 0, f"taxon_relationships={relationships:,}")
    _check(
        checks,
        "relationship_targets_include_host_domains",
        bool(model.get("top_target_domains")),
        f"target_domains={model.get('top_target_domains')}",
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
        expected_count = _int(top_result.get("sequence_count") or top_result.get("genome_count"))
        report_rows = _int(report.get("row_count") if report else 0)
        csv_rows = _int(csv_export.get("row_count") if csv_export else 0)
        _check(checks, "admin_taxon_report_available", report is not None and report_rows > 0, f"rank={rank}; name={name}; row_count={report_rows:,}")
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
        _check(checks, "admin_metadata_csv_available", csv_export is not None and csv_rows > 0, f"row_count={csv_rows:,}")
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
        "virus_sequence_records": sequence_records,
        "virus_genome_groups": genome_groups,
        "taxon_relationships": relationships,
        "relationship_type_counts": qa.get("relationship_type_counts", {}),
        "target_domain_counts": qa.get("target_domain_counts", {}),
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
            "bacterial_workflow_changed": False,
            "archaeal_workflow_changed": False,
            "global_insights_regenerated": False,
            "public_virus_routes_enabled": False,
        },
        "remaining_public_release_blockers": [
            "manual public release approval",
            "full NCBI Virus root-scale ingestion is not claimed by this reviewed-seed gate",
            "public route and Global Insights activation explicitly not enabled by this gate",
        ],
        "decision": (
            "Hidden Virus sequence-model release-candidate prechecks pass; public release remains locked."
            if release_candidate_ready
            else "Hidden Virus sequence-model release-candidate prechecks failed; public release remains locked."
        ),
    }


def write_outputs(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "virus_release_candidate_gate.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checks = "\n".join(f"- `{check['key']}`: {check['status']} - {check['detail']}" for check in summary["checks"])
    failures = "\n".join(f"- `{check['key']}` - {check['detail']}" for check in summary["hard_failures"]) or "- None"
    top = summary.get("admin_validation", {}).get("top_result", {}) or {}
    markdown = (
        "# Hidden Virus Release-Candidate Gate\n\n"
        f"- Status: `{summary['status']}`\n"
        f"- Release candidate ready for admin review: {str(summary['release_candidate_ready']).lower()}\n"
        f"- Safe to publicly release: {str(summary['safe_to_publicly_release']).lower()}\n"
        f"- Manual public release required: {str(summary['manual_public_release_required']).lower()}\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Virus sequence records: {summary['virus_sequence_records']:,}\n"
        f"- Virus genome groups: {summary['virus_genome_groups']:,}\n"
        f"- Taxon relationships: {summary['taxon_relationships']:,}\n"
        f"- Hard failures: {summary['hard_failure_count']}\n"
        "- Public enabled: false\n"
        "- Release locked: true\n\n"
        "## Admin Validation\n\n"
        f"- Search query: `{summary['admin_validation']['query']}`\n"
        f"- Top result: `{top.get('name', '')}` ({top.get('rank', '')}), {int(top.get('sequence_count') or top.get('genome_count') or 0):,} sequences\n"
        f"- Taxon report rows: {summary['admin_validation']['taxon_report_rows']:,}\n"
        f"- Metadata CSV rows: {summary['admin_validation']['metadata_csv_rows']:,}\n"
        f"- Metadata CSV: `{summary['admin_validation']['metadata_csv_filename']}`\n\n"
        "## Relationship Summary\n\n"
        f"- Relationship types: `{summary['relationship_type_counts']}`\n"
        f"- Target domains: `{summary['target_domain_counts']}`\n\n"
        "## Checks\n\n"
        f"{checks}\n\n"
        "## Hard Failures\n\n"
        f"{failures}\n\n"
        "## Decision\n\n"
        f"{summary['decision']} This gate validates reviewed hidden sequence-model rows only; it does not enable public Virus routes or claim full root-scale Virus coverage.\n"
    )
    (output_dir / "virus_release_candidate_gate.md").write_text(markdown, encoding="utf-8")

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args(argv)
    summary = collect_virus_release_candidate(args.snapshot_id, query=args.query)
    if args.output_dir:
        write_outputs(summary, Path(args.output_dir))
    print(json.dumps({
        "domain_key": summary["domain_key"],
        "snapshot_id": summary["snapshot_id"],
        "status": summary["status"],
        "release_candidate_ready": summary["release_candidate_ready"],
        "safe_to_publicly_release": summary["safe_to_publicly_release"],
        "hard_failure_count": summary["hard_failure_count"],
        "virus_sequence_records": summary["virus_sequence_records"],
        "virus_genome_groups": summary["virus_genome_groups"],
        "taxon_relationships": summary["taxon_relationships"],
    }, indent=2))
    if args.fail_on_hard_errors and summary["hard_failure_count"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
