#!/usr/bin/env python3
"""Validate a completed manual review CSV before publishing validation records.

This tool intentionally refuses blank or partially reviewed rows. It prevents the
review sample from being copied directly into Global Insights as if it were
completed manuscript validation.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

REQUIRED_COLUMNS = {
    "field",
    "assembly_accession",
    "reviewer_label",
    "reviewer_decision",
}

VALID_DECISIONS = {
    "correct",
    "false_positive",
    "unresolved",
}


def normalize(value: str | None) -> str:
    return (value or "").strip()


def validate_rows(rows: list[dict[str, str]]) -> tuple[list[str], Counter[str]]:
    errors: list[str] = []
    decisions: Counter[str] = Counter()
    seen_accessions: set[str] = set()

    if not rows:
        return ["input CSV contains no review rows"], decisions

    missing_columns = sorted(REQUIRED_COLUMNS - set(rows[0].keys()))
    if missing_columns:
        return [f"input CSV is missing required columns: {', '.join(missing_columns)}"], decisions

    for index, row in enumerate(rows, start=2):
        accession = normalize(row.get("assembly_accession"))
        label = normalize(row.get("reviewer_label"))
        decision = normalize(row.get("reviewer_decision")).lower()
        field = normalize(row.get("field"))

        if not field:
            errors.append(f"line {index}: field is blank")
        if not accession:
            errors.append(f"line {index}: assembly_accession is blank")
        elif accession in seen_accessions:
            errors.append(f"line {index}: duplicate assembly_accession {accession}")
        seen_accessions.add(accession)

        if not label:
            errors.append(f"line {index}: reviewer_label is blank")
        if not decision:
            errors.append(f"line {index}: reviewer_decision is blank")
        elif decision not in VALID_DECISIONS:
            errors.append(
                f"line {index}: reviewer_decision {decision!r} is invalid; "
                f"use one of {', '.join(sorted(VALID_DECISIONS))}"
            )
        else:
            decisions[decision] += 1

    return errors, decisions


def import_validation_records(input_path: Path, output_path: Path, *, force: bool = False) -> int:
    with input_path.open(newline="", encoding="utf-8", errors="replace") as handle:
        rows = list(csv.DictReader(handle))

    errors, decisions = validate_rows(rows)
    if errors:
        print("Validation import refused. Fix these review issues:", file=sys.stderr)
        for error in errors[:50]:
            print(f"- {error}", file=sys.stderr)
        if len(errors) > 50:
            print(f"- ... {len(errors) - 50} additional errors omitted", file=sys.stderr)
        return 2

    if output_path.exists() and not force:
        print(f"Refusing to overwrite existing {output_path}; pass --force to replace it.", file=sys.stderr)
        return 3

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(
        f"Wrote {len(rows)} reviewed validation records to {output_path} "
        f"({', '.join(f'{key}={value}' for key, value in sorted(decisions.items()))})."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="Completed manual review CSV.")
    parser.add_argument(
        "--output",
        default="global_insights/validation_records.csv",
        help="Validated output consumed by Global Insights.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite an existing validation_records.csv.")
    args = parser.parse_args()
    return import_validation_records(Path(args.input), Path(args.output), force=args.force)


if __name__ == "__main__":
    raise SystemExit(main())
