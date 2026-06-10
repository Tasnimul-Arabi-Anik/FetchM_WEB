#!/usr/bin/env python3
"""Seed canonical assembly-level standardized metadata from existing FetchM SQLite rows."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import seed_standardized_metadata_batch, standardized_metadata_coverage
from global_insights.generator import standardization_rule_manifest
from tools.host_standardization_monitoring import generate_host_monitoring

def rule_fingerprint(default: str | None) -> str:
    if default:
        return default
    manifest = standardization_rule_manifest()
    return str(manifest.get('version') or 'not available')

def iter_existing_metadata_rows(sqlite_db: Path):
    connection = sqlite3.connect(f'file:{sqlite_db}?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    query = """
        SELECT am.assembly_accession, am.row_json, s.taxon_rank, am.refreshed_at
        FROM assembly_metadata am
        JOIN species s ON s.id = am.species_id
        WHERE am.assembly_accession IS NOT NULL AND am.assembly_accession != ''
        ORDER BY am.assembly_accession,
                 CASE WHEN s.taxon_rank = 'species' THEN 0 ELSE 1 END,
                 am.refreshed_at DESC
    """
    previous = None
    scanned = 0
    yielded = 0
    try:
        for row in connection.execute(query):
            scanned += 1
            accession = str(row['assembly_accession'] or '').strip()
            if not accession or accession == previous:
                continue
            previous = accession
            try:
                payload = json.loads(str(row['row_json'] or '{}'))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            payload.setdefault('Assembly Accession', accession)
            yielded += 1
            yield payload, scanned, yielded
    finally:
        connection.close()

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--sqlite-db', required=True)
    parser.add_argument('--batch-size', type=int, default=5000)
    parser.add_argument('--rule-fingerprint', default=None)
    parser.add_argument(
        '--force-legacy-import', action='store_true',
        help='Import reusable rows from the legacy SQLite store even when canonical standardized rows already exist.',
    )
    args = parser.parse_args()
    sqlite_db = Path(args.sqlite_db)
    fingerprint = rule_fingerprint(args.rule_fingerprint)
    coverage_before = standardized_metadata_coverage(args.snapshot_id)
    cached_rows = int(coverage_before.get('standardized_assemblies') or 0)
    # Once the accession-level canonical cache exists, new root snapshots already reuse
    # it through the coverage join. Re-importing millions of SQLite rows only rewrites
    # unchanged data; newly uncovered accessions should be retrieved fresh from NCBI.
    legacy_import = bool(args.force_legacy_import or cached_rows == 0)
    if legacy_import and not sqlite_db.exists():
        raise SystemExit(f'SQLite database not found for legacy bootstrap import: {sqlite_db}')
    batch: list[dict[str, Any]] = []
    scanned = unique_candidates = seeded = skipped = 0
    if legacy_import:
        for payload, scanned_count, yielded_count in iter_existing_metadata_rows(sqlite_db):
            scanned = scanned_count
            unique_candidates = yielded_count
            batch.append(payload)
            if len(batch) >= args.batch_size:
                result = seed_standardized_metadata_batch(args.snapshot_id, batch, rule_fingerprint=fingerprint)
                seeded += result['seeded']
                skipped += result['skipped']
                batch.clear()
        if batch:
            result = seed_standardized_metadata_batch(args.snapshot_id, batch, rule_fingerprint=fingerprint)
            seeded += result['seeded']
            skipped += result['skipped']
    coverage = standardized_metadata_coverage(args.snapshot_id)
    summary = {
        'snapshot_id': args.snapshot_id,
        'sqlite_db': str(sqlite_db),
        'rule_fingerprint': fingerprint,
        'reuse_mode': 'legacy_bootstrap_import' if legacy_import else 'canonical_accession_cache',
        'cached_standardized_rows_at_start': cached_rows,
        'legacy_import_attempted': legacy_import,
        'sqlite_rows_scanned': scanned,
        'unique_sqlite_accessions_seen': unique_candidates,
        'seeded_standardized_rows': seeded,
        'skipped_not_in_canonical_root': skipped,
        **coverage,
    }
    if not summary['missing_standardized_assemblies']:
        summary['host_standardization_monitoring'] = generate_host_monitoring(args.snapshot_id)
    print(json.dumps(summary, sort_keys=True))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
