#!/usr/bin/env python3
"""Run a hidden Virus sequence-model build from reviewed NCBI Virus-style reports.

This is an admin-only operational wrapper around the Virus importer and QA gate.
It does not fetch live data and cannot enable public Virus release.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dataset_production_store as production_store
from domain_profiles import domain_profile_contract
from tools import import_hidden_virus_sequences as virus_import
from tools import qa_hidden_virus_pipeline as virus_qa

REPO_APP_DIR = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_ROOT = REPO_APP_DIR / "standardization" / "review" / "virus_hidden_pipeline"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_label(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned[:120] or "hidden-virus-snapshot"


def default_output_dir(snapshot_id: str) -> Path:
    return DEFAULT_REVIEW_ROOT / f"{safe_label(snapshot_id)}_operational_build"


def build_summary_markdown(summary: dict[str, Any]) -> str:
    qa = summary.get("qa") or {}
    persistence = summary.get("persistence") or {}
    import_summary = summary.get("import_summary") or {}
    checks = "\n".join(
        f"- `{check['key']}`: {check['status']} - {check['detail']}"
        for check in qa.get("checks", [])
    ) or "- not run"
    return (
        "# Hidden Virus Operational Build\n\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Status: `{summary['status']}`\n"
        f"- Dry run: {str(summary['dry_run']).lower()}\n"
        "- Public enabled: false\n"
        "- Release locked: true\n"
        f"- Input file: `{summary['input_file']}`\n"
        f"- Input SHA-256: `{summary['input_sha256']}`\n\n"
        "## Import Summary\n\n"
        f"- Reports loaded: {int(import_summary.get('reports_loaded', 0)):,}\n"
        f"- Reports valid: {int(import_summary.get('reports_valid', 0)):,}\n"
        f"- Reports skipped: {int(import_summary.get('reports_skipped', 0)):,}\n"
        f"- Virus sequence records: {int(import_summary.get('virus_sequence_records', 0)):,}\n"
        f"- Virus assembly surrogates: {int(import_summary.get('virus_assembly_surrogates', 0)):,}\n"
        f"- Genome groups: {int(import_summary.get('virus_genome_groups', 0)):,}\n"
        f"- Taxon relationships: {int(import_summary.get('taxon_relationships', 0)):,}\n\n"
        "## Persistence\n\n"
        f"- Sequence records seeded: {int(persistence.get('virus_sequences_seeded', 0)):,}\n"
        f"- Genome groups touched: {int(persistence.get('virus_genome_groups_touched', 0)):,}\n"
        f"- Taxon relationships seeded: {int(persistence.get('taxon_relationships_seeded', 0)):,}\n"
        f"- Skipped reports: {int(persistence.get('skipped_reports', 0)):,}\n\n"
        "## QA\n\n"
        f"- QA status: `{qa.get('status', 'not_run')}`\n"
        f"- Hard failures: {int(qa.get('hard_failure_count', 0) or 0):,}\n"
        f"- Virus sequence records: {int(qa.get('virus_sequence_records', 0) or 0):,}\n"
        f"- Virus genome groups: {int(qa.get('virus_genome_groups', 0) or 0):,}\n"
        f"- Taxon relationships: {int(qa.get('taxon_relationships', 0) or 0):,}\n\n"
        "## Checks\n\n"
        f"{checks}\n\n"
        "## Release Gate\n\n"
        "- `safe_to_publish`: false\n"
        "- Reason: hidden Virus release remains locked pending explicit public-release approval.\n"
    )


def write_build_outputs(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "hidden_virus_build_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "hidden_virus_build_summary.md").write_text(build_summary_markdown(summary), encoding="utf-8")
    if summary.get("qa"):
        virus_qa.write_outputs(summary["qa"], output_dir)


def run_hidden_virus_build(
    *,
    input_path: Path,
    snapshot_id: str,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    contract = domain_profile_contract("virus")
    reports = virus_import.load_reports(input_path)
    import_summary = virus_import.summarize_reports(reports, snapshot_id)
    persistence: dict[str, Any] = {}
    qa_summary: dict[str, Any] | None = None
    consistency_errors: list[str] = []

    if int(import_summary.get("reports_valid", 0) or 0) == 0:
        consistency_errors.append("no valid Virus sequence reports were loaded")

    if not dry_run and not consistency_errors:
        persistence = production_store.seed_virus_canonical_entities_batch(
            snapshot_id,
            reports,
            source_status="hidden_virus_sequence_operational_build",
        )
        if int(persistence.get("virus_sequences_seeded", 0) or 0) != int(import_summary.get("reports_valid", 0) or 0):
            consistency_errors.append(
                "persisted Virus sequence count does not match valid report count"
            )
        qa_summary = virus_qa.collect_hidden_virus_qa(snapshot_id)
    elif dry_run:
        persistence = {"dry_run": True}

    qa_hard_failures = int((qa_summary or {}).get("hard_failure_count", 0) or 0)
    status = "pass"
    if consistency_errors or qa_hard_failures:
        status = "fail"
    if dry_run and not consistency_errors:
        status = "dry_run_pass"

    summary: dict[str, Any] = {
        "domain_key": "virus",
        "profile": contract["profile"],
        "snapshot_id": snapshot_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "dry_run": bool(dry_run),
        "input_file": str(input_path),
        "input_sha256": sha256_file(input_path),
        "import_summary": import_summary,
        "persistence": persistence,
        "qa": qa_summary,
        "consistency_errors": consistency_errors,
        "public_enabled": False,
        "release_locked": True,
        "release_gate": {
            "safe_to_publish": False,
            "reason": "hidden Virus release remains locked pending explicit public-release approval",
        },
        "boundary": {
            "live_fetch_performed": False,
            "public_routes_enabled": False,
            "bacterial_workflow_changed": False,
            "archaeal_workflow_changed": False,
        },
    }
    if output_dir is not None:
        write_build_outputs(summary, output_dir)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Reviewed JSON/JSONL Virus sequence reports.")
    parser.add_argument("--snapshot-id", required=True, help="Hidden Virus snapshot identifier.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory for compact build and QA artifacts.")
    parser.add_argument("--dry-run", action="store_true", help="Validate input and summarize without writing hidden Virus tables.")
    parser.add_argument("--fail-on-hard-errors", action="store_true", help="Exit nonzero when consistency or QA hard failures are present.")
    args = parser.parse_args(argv)

    output_dir = args.output_dir or default_output_dir(args.snapshot_id)
    summary = run_hidden_virus_build(
        input_path=args.input,
        snapshot_id=args.snapshot_id,
        output_dir=output_dir,
        dry_run=bool(args.dry_run),
    )
    print(json.dumps({
        "domain_key": summary["domain_key"],
        "snapshot_id": summary["snapshot_id"],
        "status": summary["status"],
        "reports_valid": summary["import_summary"]["reports_valid"],
        "virus_sequences_seeded": summary.get("persistence", {}).get("virus_sequences_seeded", 0),
        "qa_status": (summary.get("qa") or {}).get("status", "not_run"),
        "release_locked": summary["release_locked"],
        "output_dir": str(output_dir),
    }, sort_keys=True))
    if args.fail_on_hard_errors and summary["status"] == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
