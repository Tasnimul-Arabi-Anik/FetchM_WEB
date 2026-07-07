#!/usr/bin/env python3
"""QA for applied Semantic Closure Phase 1 Bucket A corrections."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect

DEFAULT_SNAPSHOT_ID = "20260602T140414Z_genbank_bacteria_root"
EXPECTED_COUNTS = {
    "host_health_healthy_control_remaining": 0,
    "host_health_diseased_patient_remaining": 0,
    "isolation_site_catheter_remaining": 0,
    "host_study_group_control_rows": 1077,
    "host_context_human_phase1_rule_rows": 49,
    "catheter_clear_rule_rows": 2,
    "healthy_control_health_rule_rows": 1077,
    "healthy_control_study_rule_rows": 1077,
    "diseased_patient_health_rule_rows": 50,
    "semantic_closure_phase1_dry_run_method_mentions": 0,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_qa(snapshot_id: str, backup_table: str = "") -> dict[str, Any]:
    query = """
        SELECT
            COUNT(*) AS rows_scanned,
            COUNT(*) FILTER (WHERE COALESCE(s.standardized_payload->>'Host_Health_State_SD', '') = 'healthy/control') AS host_health_healthy_control_remaining,
            COUNT(*) FILTER (WHERE COALESCE(s.standardized_payload->>'Host_Health_State_SD', '') = 'diseased/patient') AS host_health_diseased_patient_remaining,
            COUNT(*) FILTER (WHERE COALESCE(s.standardized_payload->>'Isolation_Site_SD', '') = 'catheter') AS isolation_site_catheter_remaining,
            COUNT(*) FILTER (WHERE COALESCE(s.standardized_payload->>'Host_Study_Group_SD', '') = 'control') AS host_study_group_control_rows,
            COUNT(*) FILTER (WHERE s.standardized_payload::text LIKE '%%SC1-SITE-CATHETER-CLEAR%%') AS catheter_clear_rule_rows,
            COUNT(*) FILTER (WHERE s.standardized_payload::text LIKE '%%SC1-HHS-HEALTHY-CONTROL-HEALTH%%') AS healthy_control_health_rule_rows,
            COUNT(*) FILTER (WHERE s.standardized_payload::text LIKE '%%SC1-HHS-HEALTHY-CONTROL-STUDY%%') AS healthy_control_study_rule_rows,
            COUNT(*) FILTER (WHERE s.standardized_payload::text LIKE '%%SC1-HHS-DISEASED-PATIENT-HEALTH%%') AS diseased_patient_health_rule_rows,
            COUNT(*) FILTER (WHERE (s.standardized_payload->'Semantic_Axis_Provenance'->'Host_Context_SD')::text LIKE '%%SC1-HHS-DISEASED-PATIENT-HUMAN%%') AS host_context_human_phase1_rule_rows,
            COUNT(*) FILTER (WHERE s.standardized_payload::text LIKE '%%semantic_closure_phase1_dry_run%%') AS semantic_closure_phase1_dry_run_method_mentions
        FROM bacterial_inventory_membership AS i
        JOIN assembly_standardization AS s USING (assembly_accession)
        WHERE i.snapshot_id = %s
    """
    with connect() as connection:
        cursor = connection.execute(query, (snapshot_id,))
        columns = [desc.name for desc in cursor.description]
        row = cursor.fetchone()
        metrics = {column: int(value or 0) for column, value in zip(columns, row)}
        backup_rows = None
        if backup_table:
            backup_rows = int(connection.execute(f"SELECT COUNT(*) FROM {backup_table}").fetchone()[0])
            metrics["backup_table_rows"] = backup_rows
    hard_failures: dict[str, int] = {}
    for metric, expected in EXPECTED_COUNTS.items():
        observed = int(metrics.get(metric, -1))
        if observed != expected:
            hard_failures[metric] = observed
    if backup_table and metrics.get("backup_table_rows") != 1129:
        hard_failures["backup_table_rows"] = int(metrics.get("backup_table_rows", -1))
    return {
        "generated_at": utc_now(),
        "snapshot_id": snapshot_id,
        "backup_table": backup_table,
        "metrics": metrics,
        "expected_counts": EXPECTED_COUNTS,
        "hard_failures": hard_failures,
        "pass": not hard_failures,
    }


def write_outputs(output_dir: Path, result: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "semantic_closure_phase1_bucket_a_qa.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Semantic Closure Phase 1 Bucket A QA",
        "",
        f"- Snapshot: `{result['snapshot_id']}`",
        f"- Backup table: `{result.get('backup_table') or 'not checked'}`",
        f"- Pass: `{str(result['pass']).lower()}`",
        "",
        "## Metrics",
        "",
    ]
    for key, value in sorted((result.get("metrics") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Hard Failures", ""])
    failures = result.get("hard_failures") or {}
    if failures:
        lines.extend(f"- {key}: {value}" for key, value in sorted(failures.items()))
    else:
        lines.append("- none")
    (output_dir / "semantic_closure_phase1_bucket_a_qa.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--backup-table", default="")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args()
    result = run_qa(args.snapshot_id, args.backup_table)
    write_outputs(args.output_dir, result)
    print(json.dumps({"pass": result["pass"], "hard_failures": result["hard_failures"], "output_dir": str(args.output_dir)}, sort_keys=True))
    if args.fail_on_hard_errors and not result["pass"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
