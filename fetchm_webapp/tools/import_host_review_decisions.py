#!/usr/bin/env python3
"""Import manually reviewed host decisions into the approved rule database."""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import app, save_approved_standardization_rule


APPROVED_TYPES = {"exact_host", "broad_host"}
REJECTED_TYPES = {"ignore", "non_host_source", "missing"}


def taxonkit_name2taxid(name: str) -> tuple[str, str]:
    result = subprocess.run(
        ["taxonkit", "name2taxid"],
        input=f"{name}\n",
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return "", result.stderr.strip()
    fields = result.stdout.rstrip("\n").split("\t")
    return (fields[1].strip(), "") if len(fields) > 1 else ("", "")


def resolve_missing_taxids(rows: list[dict[str, str]]) -> str:
    names = [
        (row.get("final_host") or "").strip()
        for row in rows
        if parse_bool(row.get("final_is_approved") or "")
        and (row.get("rule_type") or "").strip().lower() in APPROVED_TYPES
        and not (row.get("final_taxid") or "").strip()
        and (row.get("final_host") or "").strip()
    ]
    if not names:
        return ""
    result = subprocess.run(
        ["taxonkit", "name2taxid"],
        input="\n".join(dict.fromkeys(names)) + "\n",
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return result.stderr.strip() or "TaxonKit lookup failed"
    lookup: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) > 1 and fields[1].strip():
            lookup[fields[0].strip()] = fields[1].strip()
    for row in rows:
        name = (row.get("final_host") or "").strip()
        if name in lookup and not (row.get("final_taxid") or "").strip():
            row["final_taxid"] = lookup[name]
    return ""


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def import_row(row: dict[str, str], approved_by: str, dry_run: bool) -> tuple[str, str]:
    raw_host = (row.get("raw_host") or "").strip()
    rule_type = (row.get("rule_type") or "").strip().lower()
    approved = parse_bool(row.get("final_is_approved") or "")
    proposed_host = (row.get("final_host") or "").strip()
    taxid = (row.get("final_taxid") or "").strip()
    confidence = (row.get("final_confidence") or "").strip().lower()
    note = (row.get("reviewer_note") or "").strip()

    if not raw_host or rule_type not in APPROVED_TYPES | REJECTED_TYPES:
        return "invalid", f"{raw_host or '<blank>'}: unsupported or incomplete rule"

    if approved and rule_type in APPROVED_TYPES:
        if not proposed_host:
            return "invalid", f"{raw_host}: approved host has no final_host"
        if not taxid:
            return "unresolved", f"{raw_host}: no TaxID for {proposed_host}"
        method = "manual_host_curation" if rule_type == "exact_host" else "manual_broad_host_curation"
        confidence = "high" if rule_type == "exact_host" else (confidence or "medium")
        if not dry_run:
            save_approved_standardization_rule(
                source_column="Host",
                original_value=raw_host,
                category="taxonomy_candidate" if rule_type == "exact_host" else "broad_host",
                destination="Host_SD",
                proposed_value=proposed_host,
                ontology_id=taxid,
                method=method,
                confidence=confidence,
                note=note,
                approved_by=approved_by,
            )
        return "imported", f"{raw_host} -> {proposed_host} ({taxid})"

    if approved or rule_type not in REJECTED_TYPES:
        return "invalid", f"{raw_host}: approval flag and rule type disagree"

    method = "not_identifiable" if rule_type == "ignore" else rule_type
    if not dry_run:
        save_approved_standardization_rule(
            source_column="Host",
            original_value=raw_host,
            category=method,
            destination="Host_SD",
            proposed_value="",
            ontology_id="",
            method=method,
            confidence="none",
            note=note,
            approved_by=approved_by,
        )
    return "imported", f"{raw_host} -> {method}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    counts: dict[str, int] = {}
    messages: list[str] = []
    with args.csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    lookup_error = resolve_missing_taxids(rows)
    if lookup_error:
        print(f"TaxonKit error: {lookup_error}", file=sys.stderr)
        return 1

    with app.app_context():
        for row in rows:
            status, message = import_row(row, args.approved_by, args.dry_run)
            counts[status] = counts.get(status, 0) + 1
            messages.append(f"{status}: {message}")

    for message in messages:
        print(message)
    print("summary:", ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    return 1 if counts.get("invalid") or counts.get("unresolved") else 0


if __name__ == "__main__":
    sys.exit(main())
