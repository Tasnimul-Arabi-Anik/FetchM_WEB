#!/usr/bin/env python3
"""Correct narrow source/environment semantic completion exceptions.

This post-application tool does not roll back the source/environment release. It
only clears reviewed false-positive semantic-axis destinations, migrates Phase
2A provenance method labels from dry-run to canonical-apply, and writes compact
review artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import Jsonb, connect
from tools.semantic_phase2a_dry_run import (
    LEGACY_COMPATIBILITY_FIELDS,
    PROTECTED_FIELDS,
    PROVENANCE_FIELD,
    RAW_EVIDENCE_FIELDS,
    REMOVAL_PROVENANCE_KEY,
    has_host_colonization_evidence,
    has_host_vital_status_evidence,
    has_human_evidence,
    norm,
    patient_environment_object_context,
    present,
    read_payload,
    write_tsv,
)
from tools.source_environment_semantic_completion import DEFAULT_SNAPSHOT_ID, git_commit, source_text, utc_now

DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "source_environment_semantic_exception_correction"
CORRECTION_PROVENANCE_KEY = "_semantic_exception_corrections"
OLD_PHASE2A_METHOD = "semantic_phase2a_dry_run"
NEW_PHASE2A_METHOD = "semantic_phase2a_canonical_apply"

PATIENT_RULES = {"PH2A-HHS-PATIENT-SAMPLING", "PH2A-HHS-PATIENT-HUMAN-CONTEXT"}
COLONIZATION_RULES = {"PH2A-HHS-COLONIZED", "PH2A-HHS-CARRIER"}
VITAL_RULES = {"PH2A-HHS-ALIVE", "PH2A-HHS-DEAD"}


def backup_table_name() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"assembly_standardization_backup_semantic_exception_{timestamp}"


def iter_records(snapshot_id: str, batch_size: int = 5000) -> Iterable[dict[str, Any]]:
    with connect() as connection:
        with connection.cursor(name="semantic_exception_correction_stream") as cursor:
            cursor.execute(
                """
                SELECT i.assembly_accession,
                       COALESCE(m.organism_name, '') AS organism_name,
                       COALESCE(m.biosample_accession, '') AS biosample_accession,
                       s.standardized_payload
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                LEFT JOIN assembly_master AS m ON m.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s
                """,
                (snapshot_id,),
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for accession, organism, biosample, payload in rows:
                    yield {
                        "assembly_accession": str(accession),
                        "organism": str(organism or ""),
                        "biosample": str(biosample or ""),
                        "payload": read_payload(payload),
                    }


def provenance(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get(PROVENANCE_FIELD)
    if not isinstance(value, dict):
        value = {}
        payload[PROVENANCE_FIELD] = value
    return value


def field_rule_entries(payload: dict[str, Any], field: str, rule_id: str | None = None) -> list[dict[str, Any]]:
    entries = provenance(payload).get(field)
    if not isinstance(entries, list):
        return []
    if rule_id is None:
        return [entry for entry in entries if isinstance(entry, dict)]
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("rule_id") == rule_id]


def has_field_rule(payload: dict[str, Any], field: str, rule_ids: set[str]) -> bool:
    return any(entry.get("rule_id") in rule_ids for entry in field_rule_entries(payload, field))


def remove_field_rule_entries(payload: dict[str, Any], field: str, rule_ids: set[str]) -> list[dict[str, Any]]:
    prov = provenance(payload)
    entries = prov.get(field)
    if not isinstance(entries, list):
        return []
    removed = [entry for entry in entries if isinstance(entry, dict) and entry.get("rule_id") in rule_ids]
    kept = [entry for entry in entries if not (isinstance(entry, dict) and entry.get("rule_id") in rule_ids)]
    if kept:
        prov[field] = kept
    else:
        prov.pop(field, None)
    return removed


def migrate_phase2a_methods(payload: dict[str, Any]) -> int:
    changed = 0

    def visit(value: Any) -> None:
        nonlocal changed
        if isinstance(value, dict):
            rule_id = str(value.get("rule_id") or "")
            if value.get("method") == OLD_PHASE2A_METHOD and rule_id.startswith("PH2A-"):
                value["method"] = NEW_PHASE2A_METHOD
                changed += 1
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload.get(PROVENANCE_FIELD))
    return changed


def add_exception_event(
    payload: dict[str, Any],
    *,
    field: str,
    previous_value: str,
    reason: str,
    rule_ids: set[str],
    removed_entries: list[dict[str, Any]],
) -> None:
    event = {
        "field": field,
        "previous_value": previous_value,
        "reason": reason,
        "rule_ids": sorted(rule_ids),
        "removed_destination_provenance": removed_entries,
        "method": "source_environment_semantic_exception_correction",
        "corrected_at": utc_now(),
        "git_commit": git_commit(),
    }
    prov = provenance(payload)
    events = prov.setdefault(CORRECTION_PROVENANCE_KEY, [])
    if event not in events:
        events.append(event)


def clear_destination(payload: dict[str, Any], field: str, rule_ids: set[str], reason: str) -> bool:
    current = str(payload.get(field) or "").strip()
    if not current:
        return False
    removed = remove_field_rule_entries(payload, field, rule_ids)
    payload[field] = ""
    add_exception_event(payload, field=field, previous_value=current, reason=reason, rule_ids=rule_ids, removed_entries=removed)
    return True


def phase2a_patient_sampling(payload: dict[str, Any]) -> bool:
    return str(payload.get("Sampling_Context_SD") or "") == "clinical subject" and has_field_rule(payload, "Sampling_Context_SD", {"PH2A-HHS-PATIENT-SAMPLING"})


def phase2a_patient_human_context(payload: dict[str, Any]) -> bool:
    return str(payload.get("Host_Context_SD") or "") == "human-associated" and has_field_rule(payload, "Host_Context_SD", {"PH2A-HHS-PATIENT-HUMAN-CONTEXT"})


def phase2a_colonization(payload: dict[str, Any]) -> bool:
    return str(payload.get("Host_Colonization_Status_SD") or "") in {"colonized", "carrier"} and has_field_rule(payload, "Host_Colonization_Status_SD", COLONIZATION_RULES)


def phase2a_vital(payload: dict[str, Any]) -> bool:
    return str(payload.get("Host_Vital_Status_SD") or "") in {"alive", "deceased"} and has_field_rule(payload, "Host_Vital_Status_SD", VITAL_RULES)


def phase2a_body_fluid_from_bare_fluid(payload: dict[str, Any]) -> bool:
    if str(payload.get("Sample_Material_SD") or "") != "body fluid":
        return False
    for entry in field_rule_entries(payload, "Sample_Material_SD", "SEC-MATERIAL-BODY-FLUID"):
        source_value = norm(entry.get("source_current_value") or entry.get("source_raw_value"))
        if source_value == "fluid":
            return True
    return False


def row_context(record: dict[str, Any], payload: dict[str, Any], decision: str, reason: str = "") -> dict[str, Any]:
    return {
        "assembly_accession": record.get("assembly_accession", ""),
        "biosample": record.get("biosample", ""),
        "organism": record.get("organism", ""),
        "decision": decision,
        "reason": reason,
        "Host": payload.get("Host", ""),
        "Host_Original": payload.get("Host_Original", ""),
        "Host_SD": payload.get("Host_SD", ""),
        "Host_TaxID": payload.get("Host_TaxID", ""),
        "Host_Context_SD": payload.get("Host_Context_SD", ""),
        "Sampling_Context_SD": payload.get("Sampling_Context_SD", ""),
        "Host_Colonization_Status_SD": payload.get("Host_Colonization_Status_SD", ""),
        "Host_Vital_Status_SD": payload.get("Host_Vital_Status_SD", ""),
        "Sample_Material_SD": payload.get("Sample_Material_SD", ""),
        "Isolation_Source_SD": payload.get("Isolation_Source_SD", ""),
        "Sample_Type_SD": payload.get("Sample_Type_SD", ""),
        "Environment_Medium_SD": payload.get("Environment_Medium_SD", ""),
        "Environment_Local_Scale_SD": payload.get("Environment_Local_Scale_SD", ""),
        "Environment_Broad_Scale_SD": payload.get("Environment_Broad_Scale_SD", ""),
        "raw_context": source_text(payload),
    }


def apply_corrections(record: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], Counter[str]]:
    before = record["payload"]
    after = deepcopy(before)
    events: list[dict[str, Any]] = []
    counts = Counter()
    migrated = migrate_phase2a_methods(after)
    counts["provenance_methods_migrated"] += migrated
    if migrated:
        events.append({"type": "provenance_method_migration", "count": migrated})

    if phase2a_patient_sampling(after):
        if patient_environment_object_context(after):
            if clear_destination(after, "Sampling_Context_SD", {"PH2A-HHS-PATIENT-SAMPLING"}, "patient-associated environmental object is not a clinical subject specimen"):
                counts["patient_sampling_cleared"] += 1
                events.append({"type": "patient_environment_false_positive", "field": "Sampling_Context_SD"})
        else:
            counts["patient_sampling_kept"] += 1

    if phase2a_patient_human_context(after):
        if patient_environment_object_context(after) and not has_human_evidence(after):
            if clear_destination(after, "Host_Context_SD", {"PH2A-HHS-PATIENT-HUMAN-CONTEXT"}, "patient-associated environmental object lacks independent human-host evidence"):
                counts["patient_human_context_cleared"] += 1
                events.append({"type": "patient_environment_false_positive", "field": "Host_Context_SD"})
        else:
            counts["patient_human_context_kept"] += 1

    if phase2a_colonization(after):
        value = str(after.get("Host_Colonization_Status_SD") or "")
        if not has_host_colonization_evidence(after, value):
            if clear_destination(after, "Host_Colonization_Status_SD", COLONIZATION_RULES, "colonization evidence describes ecological/environmental context, not host colonization"):
                counts["colonization_cleared"] += 1
                events.append({"type": "colonization_false_positive", "field": "Host_Colonization_Status_SD"})
        else:
            counts["colonization_kept"] += 1

    if phase2a_vital(after):
        if not has_host_vital_status_evidence(after):
            if clear_destination(after, "Host_Vital_Status_SD", VITAL_RULES, "vital-status evidence does not describe a biological host or subject"):
                counts["vital_status_cleared"] += 1
                events.append({"type": "vital_status_false_positive", "field": "Host_Vital_Status_SD"})
        else:
            counts["vital_status_kept"] += 1

    if phase2a_body_fluid_from_bare_fluid(after):
        if clear_destination(after, "Sample_Material_SD", {"SEC-MATERIAL-BODY-FLUID"}, "bare fluid is not deterministic body-fluid evidence"):
            counts["bare_fluid_cleared"] += 1
            events.append({"type": "generic_fluid_false_positive", "field": "Sample_Material_SD"})
    return after, events, counts


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


def write_change_batch(connection: Any, backup_table: str, snapshot_id: str, rows: list[tuple[str, dict[str, Any]]]) -> None:
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
                    status = 'source_environment_semantic_exception_correction',
                    updated_at = %s
                WHERE assembly_accession = %s
                  AND EXISTS (
                    SELECT 1 FROM bacterial_inventory_membership
                    WHERE snapshot_id = %s AND assembly_accession = %s
                  )
                """,
                (Jsonb(payload), now, accession, snapshot_id, accession),
            )


def summarize_existing_tsv(input_path: Path, output_path: Path, kind: str) -> None:
    rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if not input_path.exists():
        write_tsv(output_path, ["rule_id", "destination_field", "proposed_value", "existing_or_detail", "count", "interpretation", "representative_examples"], [])
        return
    with input_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            key = (
                row.get("rule_id", ""),
                row.get("destination_field", ""),
                row.get("destination_value", ""),
                row.get("detail", ""),
            )
            item = rows.setdefault(
                key,
                {
                    "rule_id": key[0],
                    "destination_field": key[1],
                    "proposed_value": key[2],
                    "existing_or_detail": key[3],
                    "count": 0,
                    "interpretation": "manual review" if kind == "ambiguous" else "preserved existing value; review grouped examples",
                    "representative_examples": [],
                },
            )
            item["count"] += 1
            if len(item["representative_examples"]) < 5 and row.get("assembly_accession"):
                item["representative_examples"].append(row["assembly_accession"])
    output_rows = []
    for item in rows.values():
        item = dict(item)
        item["representative_examples"] = ";".join(item["representative_examples"])
        output_rows.append(item)
    output_rows.sort(key=lambda value: (-int(value["count"]), value["rule_id"], value["existing_or_detail"]))
    write_tsv(output_path, ["rule_id", "destination_field", "proposed_value", "existing_or_detail", "count", "interpretation", "representative_examples"], output_rows)


def run(snapshot_id: str, output_dir: Path, *, apply: bool = False) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_table = backup_table_name() if apply else ""
    write_context = None
    write_connection = None
    write_transaction = None
    write_batch: list[tuple[str, dict[str, Any]]] = []
    counts = Counter()
    changed_rows = 0
    patient_rows: list[dict[str, Any]] = []
    colonization_rows: list[dict[str, Any]] = []
    vital_rows: list[dict[str, Any]] = []
    fluid_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []

    try:
        if apply:
            write_context = connect()
            write_connection = write_context.__enter__()
            write_transaction = write_connection.transaction()
            write_transaction.__enter__()
            initialize_backup_table(write_connection, snapshot_id, backup_table)
        for record in iter_records(snapshot_id):
            counts["rows_scanned"] += 1
            before = record["payload"]
            after, events, row_counts = apply_corrections(record)
            counts.update(row_counts)
            if phase2a_patient_sampling(before) or phase2a_patient_human_context(before):
                decision = "clear" if any(event.get("type") == "patient_environment_false_positive" for event in events) else "keep"
                if len(patient_rows) < 10000:
                    patient_rows.append(row_context(record, before, decision, "patient-context audit"))
            if phase2a_colonization(before):
                decision = "clear" if any(event.get("type") == "colonization_false_positive" for event in events) else "keep"
                if len(colonization_rows) < 10000:
                    colonization_rows.append(row_context(record, before, decision, "colonization-context audit"))
            if phase2a_vital(before):
                decision = "clear" if any(event.get("type") == "vital_status_false_positive" for event in events) else "keep"
                if len(vital_rows) < 10000:
                    vital_rows.append(row_context(record, before, decision, "vital-status audit"))
            if str(before.get("Sample_Material_SD") or "") == "body fluid":
                decision = "clear" if any(event.get("type") == "generic_fluid_false_positive" for event in events) else "keep"
                if len(fluid_rows) < 10000:
                    fluid_rows.append(row_context(record, before, decision, "generic-fluid audit"))
            if row_counts.get("provenance_methods_migrated"):
                if len(provenance_rows) < 10000:
                    provenance_rows.append({
                        "assembly_accession": record["assembly_accession"],
                        "biosample": record["biosample"],
                        "organism": record["organism"],
                        "migrated_entries": row_counts["provenance_methods_migrated"],
                    })
            if before != after:
                changed_rows += 1
                if apply:
                    write_batch.append((record["assembly_accession"], after))
                    if len(write_batch) >= 1000:
                        write_change_batch(write_connection, backup_table, snapshot_id, write_batch)
                        write_batch.clear()
        if apply:
            write_change_batch(write_connection, backup_table, snapshot_id, write_batch)
            write_batch.clear()
            write_transaction.__exit__(None, None, None)
            write_context.__exit__(None, None, None)
            write_transaction = None
            write_context = None

        fields = list(row_context({"assembly_accession": "", "biosample": "", "organism": ""}, {}, "", "").keys())
        write_tsv(output_dir / "patient_environment_false_positives.tsv", fields, patient_rows)
        write_tsv(output_dir / "colonization_false_positives.tsv", fields, colonization_rows)
        write_tsv(output_dir / "vital_status_review.tsv", fields, vital_rows)
        write_tsv(output_dir / "generic_fluid_review.tsv", fields, fluid_rows)
        write_tsv(output_dir / "provenance_method_migration_summary.tsv", ["assembly_accession", "biosample", "organism", "migrated_entries"], provenance_rows)

        previous_root = ROOT / "standardization" / "review" / "source_environment_semantic_completion" / "20260624_apply"
        summarize_existing_tsv(previous_root / "preserved_destination_conflicts.tsv", output_dir / "preserved_conflict_summary.tsv", "conflict")
        summarize_existing_tsv(previous_root / "ambiguous_values_retained.tsv", output_dir / "ambiguous_assignment_summary.tsv", "ambiguous")

        summary = {
            "generated_at": utc_now(),
            "git_commit": git_commit(),
            "snapshot_id": snapshot_id,
            "canonical_write_run": bool(apply),
            "backup_table": backup_table,
            "rows_scanned": counts["rows_scanned"],
            "changed_rows": changed_rows,
            "provenance_methods_migrated": counts["provenance_methods_migrated"],
            "patient_sampling_cleared": counts["patient_sampling_cleared"],
            "patient_human_context_cleared": counts["patient_human_context_cleared"],
            "colonization_cleared": counts["colonization_cleared"],
            "vital_status_cleared": counts["vital_status_cleared"],
            "bare_fluid_cleared": counts["bare_fluid_cleared"],
            "patient_sampling_kept": counts["patient_sampling_kept"],
            "colonization_kept": counts["colonization_kept"],
            "vital_status_kept": counts["vital_status_kept"],
            "legacy_field_changes": 0,
            "raw_field_changes": 0,
            "protected_field_changes": 0,
        }
        (output_dir / "phase2a_context_exception_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_tsv(
            output_dir / "phase2a_context_exception_summary.tsv",
            ["metric", "value"],
            [{"metric": key, "value": value} for key, value in summary.items()],
        )
        md = [
            "# Phase 2A Context Exception Correction",
            "",
            f"- Snapshot: `{snapshot_id}`",
            f"- Rows scanned: {summary['rows_scanned']:,}",
            f"- Rows changed: {summary['changed_rows']:,}",
            f"- Provenance method entries migrated: {summary['provenance_methods_migrated']:,}",
            f"- Patient clinical-subject clears: {summary['patient_sampling_cleared']:,}",
            f"- Patient human-context clears: {summary['patient_human_context_cleared']:,}",
            f"- Colonization clears: {summary['colonization_cleared']:,}",
            f"- Vital-status clears: {summary['vital_status_cleared']:,}",
            f"- Bare-fluid clears: {summary['bare_fluid_cleared']:,}",
            f"- Canonical write run: `{str(apply).lower()}`",
            f"- Backup table: `{backup_table}`" if backup_table else "- Backup table: not applicable",
            "",
            "Normal metadata refresh now invokes the shared semantic completion transformer after host/source/sample/environment standardization and before persistence.",
        ]
        (output_dir / "refresh_integration_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
        return summary
    except Exception as exc:
        if write_transaction is not None:
            write_transaction.__exit__(type(exc), exc, exc.__traceback__)
        if write_context is not None:
            write_context.__exit__(type(exc), exc, exc.__traceback__)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--output-label", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / (args.output_label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    summary = run(args.snapshot_id, output_dir, apply=args.apply)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
