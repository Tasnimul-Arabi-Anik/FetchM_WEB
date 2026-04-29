from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path


SCIENTIFIC_NAME_PATTERN = re.compile(
    r"^[A-Z][a-z]+(?:\s+(?:[a-z][a-z-]+|x|cf\.|aff\.|sp\.|subsp\.|var\.)){1,3}(?:\s+[A-Za-z0-9_.-]+)?$"
)


def read_existing_synonyms(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {row.get("synonym", "").strip().lower() for row in csv.DictReader(handle) if row.get("synonym")}


def parse_taxonkit_rows(path: Path, *, scientific_only: bool = False) -> tuple[list[dict[str, str]], dict[str, int]]:
    name_taxids: defaultdict[str, set[str]] = defaultdict(set)
    raw_rows: list[tuple[str, str]] = []
    stats = {
        "input_rows": 0,
        "blank_or_unresolved": 0,
        "ambiguous_names": 0,
        "non_scientific_filtered": 0,
    }
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for parts in reader:
            stats["input_rows"] += 1
            if len(parts) < 2:
                stats["blank_or_unresolved"] += 1
                continue
            name = parts[0].strip()
            taxid = parts[1].strip()
            if not name or not taxid or not taxid.isdigit():
                stats["blank_or_unresolved"] += 1
                continue
            raw_rows.append((name, taxid))
            name_taxids[name].add(taxid)

    ambiguous_names = {name for name, taxids in name_taxids.items() if len(taxids) > 1}
    stats["ambiguous_names"] = len(ambiguous_names)

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for name, taxid in raw_rows:
        if name in ambiguous_names:
            continue
        if scientific_only and not SCIENTIFIC_NAME_PATTERN.match(name):
            stats["non_scientific_filtered"] += 1
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "synonym": name,
                "canonical": name,
                "taxid": taxid,
                "confidence": "high",
                "note": "NCBI Taxonomy name2taxid reviewed import",
            }
        )
    return rows, stats


def append_synonyms(synonyms_path: Path, rows: list[dict[str, str]], dry_run: bool) -> int:
    synonyms_path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_existing_synonyms(synonyms_path)
    new_rows = [row for row in rows if row["synonym"].strip().lower() not in existing]
    if dry_run or not new_rows:
        return len(new_rows)
    file_exists = synonyms_path.exists()
    with synonyms_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["synonym", "canonical", "taxid", "confidence", "note"])
        if not file_exists or synonyms_path.stat().st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)
    return len(new_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge NCBI Taxonomy name2taxid output into host_synonyms.csv.")
    parser.add_argument("taxonkit_output", type=Path, help="TSV output from `taxonkit name2taxid`.")
    parser.add_argument(
        "--host-synonyms",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "standardization" / "host_synonyms.csv",
        help="Destination host_synonyms.csv.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report append count without modifying host_synonyms.csv.")
    parser.add_argument(
        "--scientific-only",
        action="store_true",
        help="Only import scientific-name-like matches and skip common-name matches.",
    )
    args = parser.parse_args()

    rows, stats = parse_taxonkit_rows(args.taxonkit_output, scientific_only=args.scientific_only)
    appended = append_synonyms(args.host_synonyms, rows, args.dry_run)
    for key, value in stats.items():
        print(f"{key}\t{value}")
    print(f"resolved_importable_rows\t{len(rows)}")
    print(f"new_synonyms\t{appended}")
    print(f"host_synonyms\t{args.host_synonyms}")
    print(f"dry_run\t{args.dry_run}")
    print(f"scientific_only\t{args.scientific_only}")


if __name__ == "__main__":
    main()
