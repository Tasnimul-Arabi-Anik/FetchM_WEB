from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULE_FILE = ROOT / "standardization" / "controlled_categories.csv"
DEFAULT_OUTPUT_DIR = ROOT / "standardization" / "review" / "controlled_categories_audit"


def clean(value: Any) -> str:
    return str(value or "").strip()


def lower(value: Any) -> str:
    return clean(value).lower()


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit controlled source/sample/environment category rules.")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULE_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    with args.rules.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    approved_rows = [(line_no, row) for line_no, row in enumerate(rows, start=2) if lower(row.get("status")) == "approved"]
    grouped: dict[tuple[str, str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)
    for line_no, row in approved_rows:
        key = (lower(row.get("source_column")), lower(row.get("normalized_value")), clean(row.get("destination")))
        grouped[key].append((line_no, row))

    duplicate_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    suspicious_rows: list[dict[str, Any]] = []
    conflict_key_count = 0
    duplicate_key_count = 0

    for key, entries in sorted(grouped.items()):
        if len(entries) <= 1:
            continue
        duplicate_key_count += 1
        proposed_values = {lower(row.get("proposed_value") or row.get("category")) for _, row in entries}
        is_conflict = len(proposed_values) > 1
        if is_conflict:
            conflict_key_count += 1
        for line_no, row in entries:
            out = {
                "line_no": line_no,
                "source_column": key[0],
                "normalized_value": key[1],
                "destination": key[2],
                "synonym": clean(row.get("synonym")),
                "proposed_value": clean(row.get("proposed_value") or row.get("category")),
                "confidence": clean(row.get("confidence")),
                "status": clean(row.get("status")),
                "note": clean(row.get("note")),
            }
            duplicate_rows.append(out)
            if is_conflict:
                conflict_rows.append(out)

    for line_no, row in approved_rows:
        destination = clean(row.get("destination"))
        normalized = lower(row.get("normalized_value"))
        proposed = lower(row.get("proposed_value") or row.get("category"))
        note = lower(row.get("note"))
        reason = ""
        if destination == "Environment_Local_Scale_SD" and proposed == "intensive care unit":
            if normalized != "intensive care unit":
                reason = "non-ICU value maps to intensive care unit"
        elif destination == "Environment_Local_Scale_SD" and "agricultur" in normalized and "healthcare" in lower(row.get("broad_value")):
            reason = "agricultural local-scale value has healthcare broad category"
        elif "auto conservative" in note and proposed in {"swab", "water", "metadata descriptor/non-source"}:
            reason = "auto rule maps to very broad generic value"
        if reason:
            suspicious_rows.append(
                {
                    "line_no": line_no,
                    "reason": reason,
                    "synonym": clean(row.get("synonym")),
                    "normalized_value": clean(row.get("normalized_value")),
                    "destination": destination,
                    "proposed_value": clean(row.get("proposed_value") or row.get("category")),
                    "broad_value": clean(row.get("broad_value")),
                    "status": clean(row.get("status")),
                    "note": clean(row.get("note")),
                }
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(
        args.output_dir / "duplicate_approved_rules.csv",
        ["line_no", "source_column", "normalized_value", "destination", "synonym", "proposed_value", "confidence", "status", "note"],
        duplicate_rows,
    )
    write_csv(
        args.output_dir / "conflicting_approved_rules.csv",
        ["line_no", "source_column", "normalized_value", "destination", "synonym", "proposed_value", "confidence", "status", "note"],
        conflict_rows,
    )
    write_csv(
        args.output_dir / "suspicious_approved_rules.csv",
        ["line_no", "reason", "synonym", "normalized_value", "destination", "proposed_value", "broad_value", "status", "note"],
        suspicious_rows,
    )
    markdown = [
        "# Controlled Categories Audit",
        "",
        f"- Rule file: `{args.rules}`",
        f"- Total rows: {len(rows):,}",
        f"- Approved rows: {len(approved_rows):,}",
        f"- Duplicate approved keys: {duplicate_key_count:,}",
        f"- Duplicate approved rows: {len(duplicate_rows):,}",
        f"- Conflicting approved keys: {conflict_key_count:,}",
        f"- Conflicting approved rows: {len(conflict_rows):,}",
        f"- Suspicious approved rows: {len(suspicious_rows):,}",
        "",
        "Key definition: `source_column + normalized_value + destination`.",
        "",
        "Generated files:",
        "",
        "- `duplicate_approved_rules.csv`",
        "- `conflicting_approved_rules.csv`",
        "- `suspicious_approved_rules.csv`",
    ]
    (args.output_dir / "controlled_categories_audit.md").write_text("\n".join(markdown), encoding="utf-8")
    print(args.output_dir)
    print(f"total_rows\t{len(rows)}")
    print(f"approved_rows\t{len(approved_rows)}")
    print(f"duplicate_keys\t{duplicate_key_count}")
    print(f"conflict_keys\t{conflict_key_count}")
    print(f"suspicious_rows\t{len(suspicious_rows)}")


if __name__ == "__main__":
    main()
