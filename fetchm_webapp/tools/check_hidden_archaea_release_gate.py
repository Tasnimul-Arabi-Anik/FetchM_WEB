#!/usr/bin/env python3
"""Create a hidden-only Archaea readiness gate from staging artifacts.

This gate intentionally separates hidden technical readiness from public release
readiness. It must not expose Archaea in the FetchM UI or mark it deployable.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_FILES = [
    "hidden_archaea_pipeline_summary.json",
    "archaea_metadata_audit_summary.json",
    "hidden_archaea_metadata_insights.json",
    "rule_reuse_risk.tsv",
    "standardized_field_coverage.tsv",
]

PUBLIC_BLOCKERS = [
    "Archaea-specific metadata curation batches are not complete.",
    "Manual validation of Archaea standardized metadata is not complete.",
    "Public Archaea Global Insights have not been generated or reviewed.",
    "Public UI scope exposure has not been approved.",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def int_value(*values: Any) -> int:
    for value in values:
        if value is None or value == "":
            continue
        return int(value)
    return 0


def bool_value(value: Any) -> bool:
    return bool(value) is True


def hidden_boundary_pass(pipeline: dict[str, Any], audit: dict[str, Any], insights: dict[str, Any]) -> bool:
    return all([
        pipeline.get("visibility") == "hidden_staging",
        pipeline.get("public_ui_exposed") is False,
        pipeline.get("global_insights_regenerated") is False,
        pipeline.get("deployment_run") is False,
        audit.get("public_ui_exposed") is False,
        audit.get("global_insights_regenerated") is False,
        audit.get("deployment_run") is False,
        insights.get("visibility") == "hidden_staging",
        insights.get("public_ui_exposed") is False,
        insights.get("global_insights_regenerated") is False,
    ])


def build_gate(artifact_dir: Path) -> dict[str, Any]:
    missing_files = [name for name in REQUIRED_FILES if not (artifact_dir / name).exists()]
    pipeline = read_json(artifact_dir / "hidden_archaea_pipeline_summary.json") if not missing_files else {}
    audit = read_json(artifact_dir / "archaea_metadata_audit_summary.json") if not missing_files else {}
    insights = read_json(artifact_dir / "hidden_archaea_metadata_insights.json") if not missing_files else {}
    risk_rows = read_tsv(artifact_dir / "rule_reuse_risk.tsv")

    root_rows = int_value(
        pipeline.get("root_unique_assemblies"),
        audit.get("root_unique_assemblies"),
        insights.get("root_unique_assemblies"),
    )
    standardized_rows = int_value(
        pipeline.get("standardized_assemblies"),
        audit.get("standardized_assemblies"),
        insights.get("standardized_assemblies"),
    )
    missing_standardized_rows = int_value(
        pipeline.get("missing_standardized_assemblies"),
        audit.get("missing_standardized_assemblies"),
        insights.get("missing_standardized_assemblies"),
    )
    high_risk_rows = int_value(pipeline.get("rule_reuse_high_risk_rows"), audit.get("rule_reuse_high_risk_rows"))
    medium_risk_rows = sum(1 for row in risk_rows if row.get("severity") == "medium")

    hidden_database_ready = root_rows > 0 and standardized_rows == root_rows and missing_standardized_rows == 0
    hidden_standardization_audit_pass = bool_value(pipeline.get("pipeline_pass")) and bool_value(audit.get("audit_pass")) and high_risk_rows == 0
    hidden_visibility_pass = not missing_files and hidden_boundary_pass(pipeline, audit, insights)

    hidden_staging_ready = hidden_database_ready and hidden_standardization_audit_pass and hidden_visibility_pass
    public_release_ready = False

    return {
        "generated_at": utc_now(),
        "artifact_dir": str(artifact_dir),
        "snapshot_id": pipeline.get("snapshot_id") or audit.get("snapshot_id") or insights.get("snapshot_id"),
        "domain_profile": "archaea",
        "visibility": "hidden_staging",
        "root_unique_assemblies": root_rows,
        "standardized_assemblies": standardized_rows,
        "missing_standardized_assemblies": missing_standardized_rows,
        "rule_reuse_risk_rows": int_value(pipeline.get("rule_reuse_risk_rows"), audit.get("rule_reuse_risk_rows"), len(risk_rows)),
        "rule_reuse_high_risk_rows": high_risk_rows,
        "rule_reuse_medium_risk_values": medium_risk_rows,
        "required_files_present": not missing_files,
        "missing_required_files": missing_files,
        "hidden_database_ready": hidden_database_ready,
        "hidden_standardization_audit_pass": hidden_standardization_audit_pass,
        "hidden_visibility_pass": hidden_visibility_pass,
        "hidden_staging_ready": hidden_staging_ready,
        "public_ui_exposed": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "public_release_ready": public_release_ready,
        "public_release_blockers": PUBLIC_BLOCKERS,
        "recommended_next_step": "Run Archaea-specific hidden curation and manual validation before any public UI or Global Insights exposure.",
    }


def write_markdown(path: Path, gate: dict[str, Any]) -> None:
    lines = [
        "# Hidden Archaea Readiness Gate",
        "",
        f"Snapshot ID: `{gate.get('snapshot_id')}`",
        f"Generated: {gate['generated_at']}",
        "",
        "## Decision",
        "",
        "Archaea is technically ready as a hidden staging database, but it is not public-release ready.",
        "",
        "## Hidden Staging Checks",
        "",
        "| Check | Result |",
        "| --- | ---: |",
        f"| Required files present | `{str(gate['required_files_present']).lower()}` |",
        f"| Root-unique assemblies | {gate['root_unique_assemblies']:,} |",
        f"| Standardized assemblies | {gate['standardized_assemblies']:,} |",
        f"| Missing standardized assemblies | {gate['missing_standardized_assemblies']:,} |",
        f"| High-risk rule-reuse rows | {gate['rule_reuse_high_risk_rows']:,} |",
        f"| Hidden database ready | `{str(gate['hidden_database_ready']).lower()}` |",
        f"| Hidden standardization audit pass | `{str(gate['hidden_standardization_audit_pass']).lower()}` |",
        f"| Hidden visibility pass | `{str(gate['hidden_visibility_pass']).lower()}` |",
        f"| Hidden staging ready | `{str(gate['hidden_staging_ready']).lower()}` |",
        "",
        "## Public Release Gate",
        "",
        f"Public release ready: `{str(gate['public_release_ready']).lower()}`",
        "",
        "Archaea remains hidden. Public UI exposure, public Global Insights regeneration, and deployment remain blocked until the following are complete:",
        "",
    ]
    lines.extend(f"- {item}" for item in gate["public_release_blockers"])
    lines.extend([
        "",
        "## Recommended Next Step",
        "",
        gate["recommended_next_step"],
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    args = parser.parse_args()

    gate = build_gate(args.artifact_dir)
    output_json = args.artifact_dir / "hidden_archaea_readiness_gate.json"
    output_md = args.artifact_dir / "hidden_archaea_readiness_gate.md"
    output_json.write_text(json.dumps(gate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(output_md, gate)
    print(json.dumps(gate, sort_keys=True))
    return 0 if gate["hidden_staging_ready"] and not gate["public_release_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
