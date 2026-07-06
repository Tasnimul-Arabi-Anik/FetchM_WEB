#!/usr/bin/env python3
"""Import hidden Virus sequence-report entities into the admin-only Virus model.

Input may be JSON, JSONL, or an NCBI-style JSON object containing a top-level
`reports` list. This command does not publish Virus data.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import seed_virus_canonical_entities_batch
from virus_canonical import virus_canonical_entities


def load_reports(path: Path) -> list[dict[str, Any]]:
    text = path.read_text().strip()
    if not text:
        return []
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        reports = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"line {line_number}: expected JSON object")
            reports.append(value)
        return reports
    value = json.loads(text)
    if isinstance(value, dict) and isinstance(value.get("reports"), list):
        return [item for item in value["reports"] if isinstance(item, dict)]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    raise ValueError("input must be a JSON object, JSON list, JSONL, or object with reports[]")


def summarize_reports(reports: list[dict[str, Any]], snapshot_id: str) -> dict[str, Any]:
    valid = skipped = relationships = sequence_records = assembly_surrogates = 0
    genome_groups: set[str] = set()
    examples: list[dict[str, Any]] = []
    for report in reports:
        try:
            entities = virus_canonical_entities(report, snapshot_id=snapshot_id)
        except ValueError:
            skipped += 1
            continue
        valid += 1
        sequence = entities["virus_sequence"]
        genome_groups.add(str(sequence.get("genome_group_id") or ""))
        relationships += len(entities["host_relationships"])
        if sequence.get("record_model") == "virus_assembly_surrogate":
            assembly_surrogates += 1
        else:
            sequence_records += 1
        if len(examples) < 10:
            examples.append({
                "primary_accession": sequence.get("primary_accession"),
                "record_model": sequence.get("record_model"),
                "genome_group_id": sequence.get("genome_group_id"),
                "relationship_count": len(entities["host_relationships"]),
            })
    return {
        "reports_loaded": len(reports),
        "reports_valid": valid,
        "reports_skipped": skipped,
        "virus_sequence_records": sequence_records,
        "virus_assembly_surrogates": assembly_surrogates,
        "virus_genome_groups": len({group for group in genome_groups if group}),
        "taxon_relationships": relationships,
        "examples": examples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON/JSONL file containing hidden Virus sequence reports.")
    parser.add_argument("--snapshot-id", required=True, help="Hidden Virus snapshot identifier for provenance.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without writing database tables.")
    args = parser.parse_args()

    reports = load_reports(args.input)
    summary = summarize_reports(reports, args.snapshot_id)
    summary.update({
        "domain_key": "virus",
        "snapshot_id": args.snapshot_id,
        "public_enabled": False,
        "release_locked": True,
        "dry_run": bool(args.dry_run),
    })
    if not args.dry_run:
        persisted = seed_virus_canonical_entities_batch(
            args.snapshot_id,
            reports,
            source_status="hidden_virus_sequence_import",
        )
        summary.update({"persistence": persisted})
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
