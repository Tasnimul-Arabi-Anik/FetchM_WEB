#!/usr/bin/env python3
"""Apply reviewed Semantic Closure Phase 1 Bucket A corrections.

This command intentionally applies only semantic_closure_phase1_apply_now_rules.csv.
It never reads the full Phase 1 rule file and never applies environment Bucket B.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import Jsonb, connect
from tools import semantic_closure_phase1_dry_run as closure

DEFAULT_SNAPSHOT_ID = closure.DEFAULT_SNAPSHOT_ID
DEFAULT_RULES = ROOT / "standardization" / "semantic_closure_phase1_apply_now_rules.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "semantic_closure_phase1_bucket_a_apply"
APPLY_METHOD = "semantic_closure_phase1_bucket_a_apply"
APPLY_STATUS = "semantic_closure_phase1_bucket_a"
EXPECTED_RULE_IDS = {
    "SC1-SITE-CATHETER-CLEAR",
    "SC1-HHS-HEALTHY-CONTROL-HEALTH",
    "SC1-HHS-HEALTHY-CONTROL-STUDY",
    "SC1-HHS-DISEASED-PATIENT-HEALTH",
    "SC1-HHS-DISEASED-PATIENT-HUMAN",
}
COUNT_FIELDS = [
    "Host_Health_State_SD",
    "Host_Study_Group_SD",
    "Host_Context_SD",
    "Isolation_Site_SD",
]
COUNT_VALUES = {
    "Host_Health_State_SD": ["healthy/control", "diseased/patient", "healthy", "diseased"],
    "Host_Study_Group_SD": ["control"],
    "Host_Context_SD": ["human-associated"],
    "Isolation_Site_SD": ["catheter"],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, text=True, capture_output=True, check=False)
    return result.stdout.strip() or "unknown"


def default_output_dir() -> Path:
    return DEFAULT_OUTPUT_ROOT / datetime.now(timezone.utc).strftime("%Y%m%d")


def backup_table_name() -> str:
    return "assembly_standardization_backup_semantic_closure_bucket_a_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def validate_bucket_a_rules(rules: list[closure.ClosureRule]) -> None:
    rule_ids = {rule.rule_id for rule in rules}
    if rule_ids != EXPECTED_RULE_IDS:
        raise RuntimeError(f"Unexpected Bucket A rule set: {sorted(rule_ids)}")
    forbidden = [rule.rule_id for rule in rules if rule.rule_id.startswith("SC1-ENV-")]
    if forbidden:
        raise RuntimeError(f"Environment Bucket B rules are not allowed in Bucket A apply: {forbidden}")


def initialize_backup_table(connection: Any, snapshot_id: str, backup_table: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {backup_table} AS
        SELECT s.*
        FROM assembly_standardization AS s
        JOIN bacterial_inventory_membership AS i USING (assembly_accession)
        WHERE i.snapshot_id = %s AND false
        """,
        (snapshot_id,),
    )


def update_rows(connection: Any, backup_table: str, snapshot_id: str, rows: list[tuple[str, dict[str, Any]]]) -> None:
    if not rows:
        return
    now = datetime.now(timezone.utc)
    with connection.cursor() as cursor:
        for accession, payload in rows:
            cursor.execute(f"INSERT INTO {backup_table} SELECT s.* FROM assembly_standardization AS s WHERE s.assembly_accession = %s", (accession,))
            cursor.execute(
                """
                UPDATE assembly_standardization
                SET standardized_payload = %s,
                    status = %s,
                    updated_at = %s
                WHERE assembly_accession = %s
                  AND EXISTS (
                    SELECT 1 FROM bacterial_inventory_membership
                    WHERE snapshot_id = %s AND assembly_accession = %s
                  )
                """,
                (Jsonb(payload), APPLY_STATUS, now, accession, snapshot_id, accession),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Expected exactly one canonical row update for {accession}; observed {cursor.rowcount}")


def value_counts(payload: dict[str, Any], counter: Counter[tuple[str, str]]) -> None:
    for field in COUNT_FIELDS:
        value = str(payload.get(field) or "").strip()
        if value:
            counter[(field, value)] += 1


def count_row(field: str, value: str, before: Counter[tuple[str, str]], after: Counter[tuple[str, str]]) -> dict[str, Any]:
    return {
        "field": field,
        "value": value,
        "before_count": before.get((field, value), 0),
        "after_count": after.get((field, value), 0),
        "delta": after.get((field, value), 0) - before.get((field, value), 0),
    }


def write_tsv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    closure.write_tsv(path, columns, rows)


def scan_changes(snapshot_id: str, rules_path: Path, *, apply_method: bool = False) -> dict[str, Any]:
    original_method = closure.METHOD
    if apply_method:
        closure.METHOD = APPLY_METHOD
    try:
        rules = closure.load_rules(rules_path)
        validate_bucket_a_rules(rules)
        grouped = closure.rules_by_current(rules)
        changed_rows: list[tuple[str, dict[str, Any]]] = []
        changed_examples: list[dict[str, Any]] = []
        rule_counts: dict[str, Counter[str]] = {rule.rule_id: Counter() for rule in rules}
        changed_field_counts: Counter[str] = Counter()
        before_counts: Counter[tuple[str, str]] = Counter()
        after_counts: Counter[tuple[str, str]] = Counter()
        legacy_changed = Counter()
        raw_changed = Counter()
        protected_changed = Counter()
        unexpected_changed = Counter()
        provenance_events = 0
        missing_provenance = 0
        second_pass_changes = 0
        destination_conflicts = 0
        rows_scanned = 0

        for record in closure.iter_records(snapshot_id):
            rows_scanned += 1
            before = record["payload"]
            value_counts(before, before_counts)
            result = closure.apply_rules_to_payload(before, grouped)
            after = result["after"]
            value_counts(after, after_counts)
            if not result["matched_rules"]:
                continue
            for rule in result["matched_rules"]:
                rule_counts[rule.rule_id]["matched_rows"] += 1
            for event in result["outcomes"]:
                status = str(event.get("status") or "")
                rule_id = str(event.get("rule_id") or "")
                rule_counts[rule_id][status] += 1
                if status == "conflict":
                    destination_conflicts += 1
            for field in result["changed_fields"]:
                changed_field_counts[field] += 1
            for field in result["legacy_changed"]:
                legacy_changed[field] += 1
            for field in result["raw_changed"]:
                raw_changed[field] += 1
            for field in result["protected_changed"]:
                protected_changed[field] += 1
            for field in result["unexpected_changed"]:
                unexpected_changed[field] += 1
            if result["clears"] and not result["removal_events"]:
                missing_provenance += 1
            for event in result["removal_events"]:
                if event:
                    provenance_events += 1
            provenance = after.get(closure.PROVENANCE_FIELD)
            if isinstance(provenance, dict):
                for entries in provenance.values():
                    if isinstance(entries, list):
                        provenance_events += len(entries)
            second = closure.apply_rules_to_payload(after, grouped)
            if second["changed_fields"]:
                second_pass_changes += 1
            if result["changed_fields"]:
                changed_rows.append((record["assembly_accession"], after))
                if len(changed_examples) < 5000:
                    changed_examples.append(closure.change_row(record, result))

        rule_rows = [
            {
                "rule_id": rule.rule_id,
                "current_field": rule.current_field,
                "current_value": rule.current_value,
                "destination_field": rule.destination_field,
                "destination_value": rule.destination_value,
                "matched_rows": rule_counts[rule.rule_id].get("matched_rows", 0),
                "applied": rule_counts[rule.rule_id].get("applied", 0),
                "already_same": rule_counts[rule.rule_id].get("already_same", 0),
                "existing_different": rule_counts[rule.rule_id].get("existing_different", 0),
                "conditional_assignment_skip": rule_counts[rule.rule_id].get("conditional_assignment_skip", 0),
                "clear_only": rule_counts[rule.rule_id].get("clear_only", 0),
            }
            for rule in rules
        ]
        field_rows = [{"field": field, "changed_rows": count} for field, count in sorted(changed_field_counts.items())]
        count_rows = [count_row(field, value, before_counts, after_counts) for field, values in COUNT_VALUES.items() for value in values]
        hard_failures = {
            "destination_conflicts": destination_conflicts,
            "legacy_compatibility_field_changes": sum(legacy_changed.values()),
            "raw_changes": sum(raw_changed.values()),
            "protected_field_changes": sum(protected_changed.values()),
            "host_taxonomy_changes": sum(protected_changed[field] for field in closure.PROTECTED_FIELDS if field.startswith("Host_")),
            "geography_date_changes": sum(protected_changed[field] for field in ["Country", "Continent", "Subcontinent", "Collection Date"]),
            "outside_allowlist_changes": sum(unexpected_changed.values()),
            "missing_provenance": missing_provenance,
            "second_pass_changes": second_pass_changes,
        }
        return {
            "rows_scanned": rows_scanned,
            "changed_rows": changed_rows,
            "changed_examples": changed_examples,
            "rule_rows": rule_rows,
            "field_rows": field_rows,
            "count_rows": count_rows,
            "hard_failures": hard_failures,
            "provenance_events": provenance_events,
            "rules_count": len(rules),
        }
    finally:
        closure.METHOD = original_method


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Semantic Closure Phase 1 Bucket A Apply",
        "",
        f"- Snapshot: `{summary['snapshot_id']}`",
        f"- Rows scanned: {summary['rows_scanned']:,}",
        f"- Rows changed: {summary['rows_changed']:,}",
        f"- Backup table: `{summary.get('backup_table') or 'not created'}`",
        f"- Canonical write run: `{str(summary['canonical_write_run']).lower()}`",
        f"- Global Insights regenerated: `{str(summary['global_insights_regenerated']).lower()}`",
        f"- Deployment run: `{str(summary['deployment_run']).lower()}`",
        "",
        "## Hard Failures",
        "",
    ]
    failures = summary.get("hard_failures") or {}
    active = {key: value for key, value in failures.items() if value}
    if active:
        lines.extend(f"- {key}: {value}" for key, value in sorted(active.items()))
    else:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_placeholder_artifacts(output_dir: Path) -> None:
    (output_dir / "qa_summary.md").write_text("# QA Summary\n\nPost-apply QA not yet recorded.\n", encoding="utf-8")
    (output_dir / "global_insights_regeneration.json").write_text(json.dumps({"global_insights_regenerated": False, "status": "not_run"}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "live_smoke_test.md").write_text("# Live Smoke Test\n\nNot run yet.\n", encoding="utf-8")


def run(snapshot_id: str, rules_path: Path, output_dir: Path, *, apply: bool, expected_rows: int | None) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    scan = scan_changes(snapshot_id, rules_path, apply_method=apply)
    backup_table = backup_table_name() if apply else ""
    rows_changed = len(scan["changed_rows"])
    hard_failures = dict(scan["hard_failures"])
    if expected_rows is not None and rows_changed != expected_rows:
        hard_failures["unexpected_changed_rows"] = rows_changed
    if any(hard_failures.values()):
        summary = {
            "generated_at": utc_now(),
            "implementation_commit": git_commit(),
            "snapshot_id": snapshot_id,
            "rules_path": str(rules_path),
            "rules_count": scan["rules_count"],
            "rows_scanned": scan["rows_scanned"],
            "rows_changed": rows_changed,
            "backup_table": "",
            "canonical_write_run": False,
            "global_insights_regenerated": False,
            "deployment_run": False,
            "hard_failures": hard_failures,
        }
        write_outputs(output_dir, summary, scan)
        raise RuntimeError("Bucket A apply gate failed before write: " + json.dumps(hard_failures, sort_keys=True))

    if apply and scan["changed_rows"]:
        write_context = connect()
        connection = write_context.__enter__()
        transaction = connection.transaction()
        transaction.__enter__()
        try:
            initialize_backup_table(connection, snapshot_id, backup_table)
            update_rows(connection, backup_table, snapshot_id, scan["changed_rows"])
            transaction.__exit__(None, None, None)
            write_context.__exit__(None, None, None)
        except Exception as exc:
            transaction.__exit__(type(exc), exc, exc.__traceback__)
            write_context.__exit__(type(exc), exc, exc.__traceback__)
            raise

    post_summary = {}
    if apply:
        post_dir = output_dir / "post_apply_zero_change_dry_run"
        post_summary = closure.run_dry_run(snapshot_id, rules_path, post_dir, example_limit=1000)
        if post_summary.get("projected_rows_changed") != 0 or post_summary.get("second_pass_changes") != 0:
            hard_failures["post_apply_projected_rows_changed"] = post_summary.get("projected_rows_changed", -1)
            hard_failures["post_apply_second_pass_changes"] = post_summary.get("second_pass_changes", -1)

    summary = {
        "generated_at": utc_now(),
        "implementation_commit": git_commit(),
        "snapshot_id": snapshot_id,
        "rules_path": str(rules_path),
        "rules_count": scan["rules_count"],
        "rows_scanned": scan["rows_scanned"],
        "rows_changed": rows_changed,
        "backup_table": backup_table,
        "canonical_write_run": bool(apply),
        "global_insights_regenerated": False,
        "deployment_run": False,
        "provenance_events": scan["provenance_events"],
        "hard_failures": hard_failures,
        "post_apply_zero_change_dry_run": {
            "projected_rows_changed": post_summary.get("projected_rows_changed", None),
            "destination_conflicts": post_summary.get("destination_conflicts", None),
            "second_pass_changes": post_summary.get("second_pass_changes", None),
        },
    }
    write_outputs(output_dir, summary, scan)
    if apply and any(hard_failures.values()):
        raise RuntimeError("Bucket A post-apply gate failed: " + json.dumps(hard_failures, sort_keys=True))
    return summary


def write_outputs(output_dir: Path, summary: dict[str, Any], scan: dict[str, Any]) -> None:
    write_tsv(output_dir / "before_after_counts.tsv", ["field", "value", "before_count", "after_count", "delta"], scan["count_rows"])
    write_tsv(output_dir / "applied_rules.tsv", ["rule_id", "current_field", "current_value", "destination_field", "destination_value", "matched_rows", "applied", "already_same", "existing_different", "conditional_assignment_skip", "clear_only"], scan["rule_rows"])
    write_tsv(output_dir / "changed_field_counts.tsv", ["field", "changed_rows"], scan["field_rows"])
    write_tsv(output_dir / "changed_examples.tsv", ["assembly_accession", "biosample", "organism", "changed_fields", "matched_rules", "outcomes"], scan["changed_examples"])
    (output_dir / "bucket_a_apply_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_summary_md(output_dir / "bucket_a_apply_summary.md", summary)
    (output_dir / "backup_table.txt").write_text((summary.get("backup_table") or "") + "\n", encoding="utf-8")
    (output_dir / "post_apply_zero_change_dry_run.json").write_text(json.dumps(summary.get("post_apply_zero_change_dry_run") or {}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_placeholder_artifacts(output_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-rows", type=int, default=1129)
    args = parser.parse_args(argv)
    output_dir = args.output_dir or default_output_dir()
    summary = run(args.snapshot_id, args.rules, output_dir, apply=bool(args.apply), expected_rows=args.expected_rows)
    print(json.dumps({
        "snapshot_id": summary["snapshot_id"],
        "rows_scanned": summary["rows_scanned"],
        "rows_changed": summary["rows_changed"],
        "backup_table": summary.get("backup_table", ""),
        "canonical_write_run": summary["canonical_write_run"],
        "hard_failures": summary["hard_failures"],
        "output_dir": str(output_dir),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
