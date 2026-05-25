"""PostgreSQL storage for canonical bacterial inventory and release accounting."""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # Runtime dependency is installed in the application image.
    psycopg = None
    Jsonb = None

DATASET_DATABASE_URL_ENV = "FETCHM_WEBAPP_DATASET_DATABASE_URL"
CANONICAL_SOURCE_DATABASE = "genbank"
CANONICAL_ACCESSION_NAMESPACE = "GCA"
BACTERIA_TAXON_ID = 2

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS canonical_inventory_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);

CREATE TABLE IF NOT EXISTS canonical_partition_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    dataset_version_id TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS canonical_metadata_seed_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    sqlite_db_path TEXT,
    rule_fingerprint TEXT,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS bacterial_inventory_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    source_database TEXT NOT NULL,
    canonical_accession_namespace TEXT NOT NULL,
    taxon_id BIGINT NOT NULL,
    invocation TEXT NOT NULL,
    datasets_version TEXT,
    raw_records BIGINT NOT NULL DEFAULT 0,
    root_unique_assemblies BIGINT NOT NULL DEFAULT 0,
    noncanonical_records BIGINT NOT NULL DEFAULT 0,
    duplicate_records BIGINT NOT NULL DEFAULT 0,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS assembly_master (
    assembly_accession TEXT PRIMARY KEY,
    canonical_accession_namespace TEXT NOT NULL,
    source_database TEXT NOT NULL,
    organism_name TEXT,
    tax_id BIGINT,
    species_tax_id BIGINT,
    biosample_accession TEXT,
    paired_refseq_accession TEXT,
    first_seen_snapshot_id TEXT NOT NULL REFERENCES bacterial_inventory_snapshot(snapshot_id),
    latest_snapshot_id TEXT NOT NULL REFERENCES bacterial_inventory_snapshot(snapshot_id),
    raw_fingerprint TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bacterial_inventory_membership (
    snapshot_id TEXT NOT NULL REFERENCES bacterial_inventory_snapshot(snapshot_id) ON DELETE CASCADE,
    assembly_accession TEXT NOT NULL REFERENCES assembly_master(assembly_accession),
    raw_fingerprint TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, assembly_accession)
);

CREATE TABLE IF NOT EXISTS assembly_standardization (
    assembly_accession TEXT PRIMARY KEY REFERENCES assembly_master(assembly_accession),
    input_fingerprint TEXT NOT NULL,
    rule_fingerprint TEXT NOT NULL,
    standardized_payload JSONB NOT NULL,
    status TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS taxon_partition_membership (
    snapshot_id TEXT NOT NULL REFERENCES bacterial_inventory_snapshot(snapshot_id) ON DELETE CASCADE,
    assembly_accession TEXT NOT NULL REFERENCES assembly_master(assembly_accession),
    genus_name TEXT,
    species_label TEXT,
    partition_type TEXT NOT NULL,
    assignment_confidence TEXT NOT NULL,
    assignment_reason TEXT,
    PRIMARY KEY (snapshot_id, assembly_accession),
    CHECK (partition_type IN ('named_species', 'provisional_species', 'genus_only', 'unresolved_genus', 'excluded'))
);

CREATE TABLE IF NOT EXISTS canonical_root_reconciliation (
    snapshot_id TEXT PRIMARY KEY REFERENCES bacterial_inventory_snapshot(snapshot_id),
    dataset_version_id TEXT NOT NULL,
    status TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    root_unique_assemblies BIGINT NOT NULL,
    accounted_unique_assemblies BIGINT NOT NULL,
    named_species_assemblies BIGINT NOT NULL DEFAULT 0,
    provisional_species_assemblies BIGINT NOT NULL DEFAULT 0,
    genus_only_assemblies BIGINT NOT NULL DEFAULT 0,
    unresolved_genus_assemblies BIGINT NOT NULL DEFAULT 0,
    excluded_assemblies BIGINT NOT NULL DEFAULT 0,
    unaccounted_assemblies BIGINT NOT NULL DEFAULT 0,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_inventory_membership_accession
ON bacterial_inventory_membership (assembly_accession);
CREATE INDEX IF NOT EXISTS idx_partition_type
ON taxon_partition_membership (snapshot_id, partition_type);
CREATE INDEX IF NOT EXISTS idx_canonical_partition_task_status
ON canonical_partition_task (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_canonical_metadata_seed_task_status
ON canonical_metadata_seed_task (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_assembly_standardization_status
ON assembly_standardization (status, updated_at);
"""

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def dataset_database_url() -> str:
    value = os.environ.get(DATASET_DATABASE_URL_ENV, '').strip()
    if not value:
        raise RuntimeError(f'{DATASET_DATABASE_URL_ENV} is not configured.')
    return value

@contextmanager
def connect() -> Iterator[Any]:
    if psycopg is None:
        raise RuntimeError('psycopg is required for the dataset production store.')
    with psycopg.connect(dataset_database_url(), connect_timeout=2) as connection:
        yield connection

def bootstrap_schema() -> None:
    with connect() as connection:
        connection.execute(SCHEMA_SQL)
        connection.commit()

def queue_inventory_task(requested_by: str | None = None) -> tuple[str | None, str | None]:
    bootstrap_schema()
    snapshot_id = utc_now().strftime('%Y%m%dT%H%M%SZ_genbank_bacteria_root')
    with connect() as connection:
        active = connection.execute(
            "SELECT snapshot_id, status FROM canonical_inventory_task WHERE status IN ('pending', 'running') ORDER BY requested_at ASC LIMIT 1"
        ).fetchone()
        if active is not None:
            return None, f'Canonical inventory task {active[0]} is already {active[1]}.'
        connection.execute(
            "INSERT INTO canonical_inventory_task (snapshot_id, status, requested_by, requested_at) VALUES (%s, 'pending', %s, %s)",
            (snapshot_id, requested_by, utc_now()),
        )
        connection.commit()
    return snapshot_id, None

def claim_inventory_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id FROM canonical_inventory_task
            WHERE status = 'pending' ORDER BY requested_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            "UPDATE canonical_inventory_task SET status = 'running', claimed_by = %s, claimed_at = %s WHERE id = %s",
            (worker_name, utc_now(), row[0]),
        )
        connection.commit()
        return {'id': int(row[0]), 'snapshot_id': str(row[1])}

def finish_inventory_task(task_id: int, status: str, error: str | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_inventory_task SET status = %s, completed_at = %s, error = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, task_id),
        )
        connection.commit()

def latest_inventory_task() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            'SELECT snapshot_id, status, requested_at, completed_at, error FROM canonical_inventory_task ORDER BY requested_at DESC LIMIT 1'
        ).fetchone()
    if row is None:
        return None
    return {'snapshot_id': row[0], 'status': row[1], 'requested_at': row[2], 'completed_at': row[3], 'error': row[4]}

def latest_completed_inventory_snapshot() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT snapshot_id, completed_at, root_unique_assemblies, source_database, canonical_accession_namespace
            FROM bacterial_inventory_snapshot
            WHERE status = 'completed'
            ORDER BY completed_at DESC NULLS LAST, requested_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return {
        'snapshot_id': row[0], 'completed_at': row[1], 'root_unique_assemblies': int(row[2] or 0),
        'source_database': row[3], 'canonical_accession_namespace': row[4],
    }

def queue_partition_task(
    requested_by: str | None = None,
    *,
    snapshot_id: str | None = None,
    dataset_version_id: str | None = None,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    if not snapshot_id:
        latest = latest_completed_inventory_snapshot()
        if latest is None:
            return None, 'No completed canonical inventory snapshot is available.'
        snapshot_id = str(latest['snapshot_id'])
    if not dataset_version_id:
        dataset_version_id = f'canonical-root-preview-{snapshot_id}'
    with connect() as connection:
        active = connection.execute(
            "SELECT snapshot_id, status FROM canonical_partition_task WHERE status IN ('pending', 'running') ORDER BY requested_at ASC LIMIT 1"
        ).fetchone()
        if active is not None:
            return None, f'Canonical partition task for {active[0]} is already {active[1]}.'
        inventory = connection.execute(
            "SELECT status FROM bacterial_inventory_snapshot WHERE snapshot_id = %s",
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            return None, 'Canonical inventory snapshot is not completed.'
        connection.execute(
            """
            INSERT INTO canonical_partition_task (snapshot_id, dataset_version_id, status, requested_by, requested_at)
            VALUES (%s, %s, 'pending', %s, %s)
            """,
            (snapshot_id, dataset_version_id, requested_by, utc_now()),
        )
        connection.commit()
    return snapshot_id, None

def claim_partition_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, dataset_version_id FROM canonical_partition_task
            WHERE status = 'pending' ORDER BY requested_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            "UPDATE canonical_partition_task SET status = 'running', claimed_by = %s, claimed_at = %s WHERE id = %s",
            (worker_name, utc_now(), row[0]),
        )
        connection.commit()
        return {'id': int(row[0]), 'snapshot_id': str(row[1]), 'dataset_version_id': str(row[2])}

def finish_partition_task(task_id: int, status: str, error: str | None = None, summary: dict[str, Any] | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_partition_task SET status = %s, completed_at = %s, error = %s, summary_json = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, Jsonb(summary or {}), task_id),
        )
        connection.commit()

def latest_partition_task() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            'SELECT snapshot_id, dataset_version_id, status, requested_at, completed_at, error, summary_json FROM canonical_partition_task ORDER BY requested_at DESC LIMIT 1'
        ).fetchone()
    if row is None:
        return None
    return {
        'snapshot_id': row[0], 'dataset_version_id': row[1], 'status': row[2], 'requested_at': row[3],
        'completed_at': row[4], 'error': row[5], 'summary': dict(row[6] or {}),
    }

def queue_metadata_seed_task(
    requested_by: str | None = None,
    *,
    snapshot_id: str | None = None,
    sqlite_db_path: str | None = None,
    rule_fingerprint: str | None = None,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    if not snapshot_id:
        latest = latest_completed_inventory_snapshot()
        if latest is None:
            return None, 'No completed canonical inventory snapshot is available.'
        snapshot_id = str(latest['snapshot_id'])
    with connect() as connection:
        active = connection.execute(
            "SELECT snapshot_id, status FROM canonical_metadata_seed_task WHERE status IN ('pending', 'running') ORDER BY requested_at ASC LIMIT 1"
        ).fetchone()
        if active is not None:
            return None, f'Canonical metadata seed task for {active[0]} is already {active[1]}.'
        inventory = connection.execute(
            "SELECT status FROM bacterial_inventory_snapshot WHERE snapshot_id = %s",
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            return None, 'Canonical inventory snapshot is not completed.'
        connection.execute(
            """
            INSERT INTO canonical_metadata_seed_task (
                snapshot_id, status, requested_by, requested_at, sqlite_db_path, rule_fingerprint
            ) VALUES (%s, 'pending', %s, %s, %s, %s)
            """,
            (snapshot_id, requested_by, utc_now(), sqlite_db_path, rule_fingerprint),
        )
        connection.commit()
    return snapshot_id, None

def claim_metadata_seed_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, sqlite_db_path, rule_fingerprint FROM canonical_metadata_seed_task
            WHERE status = 'pending' ORDER BY requested_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            "UPDATE canonical_metadata_seed_task SET status = 'running', claimed_by = %s, claimed_at = %s WHERE id = %s",
            (worker_name, utc_now(), row[0]),
        )
        connection.commit()
        return {
            'id': int(row[0]), 'snapshot_id': str(row[1]),
            'sqlite_db_path': str(row[2] or ''), 'rule_fingerprint': str(row[3] or ''),
        }

def finish_metadata_seed_task(task_id: int, status: str, error: str | None = None, summary: dict[str, Any] | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_metadata_seed_task SET status = %s, completed_at = %s, error = %s, summary_json = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, Jsonb(summary or {}), task_id),
        )
        connection.commit()

def latest_metadata_seed_task() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            'SELECT snapshot_id, status, requested_at, claimed_at, completed_at, error, summary_json FROM canonical_metadata_seed_task ORDER BY requested_at DESC LIMIT 1'
        ).fetchone()
    if row is None:
        return None
    return {
        'snapshot_id': row[0], 'status': row[1], 'requested_at': row[2], 'claimed_at': row[3],
        'completed_at': row[4], 'error': row[5], 'summary': dict(row[6] or {}),
    }

def start_inventory_snapshot(snapshot_id: str, invocation: str, datasets_version: str | None) -> None:
    bootstrap_schema()
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO bacterial_inventory_snapshot (
                snapshot_id, status, requested_at, started_at, source_database,
                canonical_accession_namespace, taxon_id, invocation, datasets_version
            ) VALUES (%s, 'running', %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                status = 'running', started_at = EXCLUDED.started_at, completed_at = NULL,
                invocation = EXCLUDED.invocation, datasets_version = EXCLUDED.datasets_version, error = NULL
            """,
            (snapshot_id, utc_now(), utc_now(), CANONICAL_SOURCE_DATABASE, CANONICAL_ACCESSION_NAMESPACE, BACTERIA_TAXON_ID, invocation, datasets_version),
        )
        connection.execute('DELETE FROM bacterial_inventory_membership WHERE snapshot_id = %s', (snapshot_id,))
        connection.commit()

def nested_value(payload: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value: Any = payload
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if value not in (None, ''):
            return value
    return None

def normalized_inventory_record(payload: dict[str, Any]) -> dict[str, Any]:
    accession = str(nested_value(payload, ('accession',), ('assembly_accession',), ('assembly_info', 'assembly_accession')) or '').strip()
    organism = nested_value(payload, ('organism', 'organism_name'), ('organism_name',), ('assembly_info', 'organism', 'organism_name'))
    tax_id = nested_value(payload, ('organism', 'tax_id'), ('tax_id',), ('assembly_info', 'organism', 'tax_id'))
    species_tax_id = nested_value(payload, ('organism', 'infraspecific_names', 'species_tax_id'), ('species_tax_id',))
    biosample = nested_value(payload, ('assembly_info', 'biosample', 'accession'), ('biosample', 'accession'), ('biosample_accession',))
    paired = nested_value(payload, ('assembly_info', 'paired_assembly', 'accession'), ('paired_assembly', 'accession'), ('paired_refseq_accession',))
    raw_json = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return {
        'assembly_accession': accession, 'organism_name': str(organism or ''),
        'tax_id': int(tax_id) if str(tax_id or '').isdigit() else None,
        'species_tax_id': int(species_tax_id) if str(species_tax_id or '').isdigit() else None,
        'biosample_accession': str(biosample or ''), 'paired_refseq_accession': str(paired or ''),
        'raw_fingerprint': hashlib.sha256(raw_json.encode('utf-8')).hexdigest(), 'raw_payload': payload,
    }

def insert_inventory_batch(snapshot_id: str, records: Iterable[dict[str, Any]]) -> tuple[int, int, int]:
    canonical = 0
    noncanonical = 0
    duplicates = 0
    now = utc_now()
    with connect() as connection:
        for payload in records:
            record = normalized_inventory_record(payload)
            accession = record['assembly_accession']
            if not accession.startswith('GCA_'):
                noncanonical += 1
                continue
            connection.execute(
                """
                INSERT INTO assembly_master (
                    assembly_accession, canonical_accession_namespace, source_database, organism_name, tax_id,
                    species_tax_id, biosample_accession, paired_refseq_accession, first_seen_snapshot_id,
                    latest_snapshot_id, raw_fingerprint, raw_payload, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (assembly_accession) DO UPDATE SET
                    organism_name = EXCLUDED.organism_name, tax_id = EXCLUDED.tax_id,
                    species_tax_id = EXCLUDED.species_tax_id, biosample_accession = EXCLUDED.biosample_accession,
                    paired_refseq_accession = EXCLUDED.paired_refseq_accession,
                    latest_snapshot_id = EXCLUDED.latest_snapshot_id, raw_fingerprint = EXCLUDED.raw_fingerprint,
                    raw_payload = EXCLUDED.raw_payload, updated_at = EXCLUDED.updated_at
                """,
                (accession, CANONICAL_ACCESSION_NAMESPACE, CANONICAL_SOURCE_DATABASE, record['organism_name'], record['tax_id'], record['species_tax_id'], record['biosample_accession'], record['paired_refseq_accession'], snapshot_id, snapshot_id, record['raw_fingerprint'], Jsonb(record['raw_payload']), now),
            )
            result = connection.execute(
                """
                INSERT INTO bacterial_inventory_membership (snapshot_id, assembly_accession, raw_fingerprint)
                VALUES (%s, %s, %s)
                ON CONFLICT (snapshot_id, assembly_accession) DO NOTHING
                RETURNING assembly_accession
                """,
                (snapshot_id, accession, record['raw_fingerprint']),
            ).fetchone()
            if result is None:
                duplicates += 1
                continue
            canonical += 1
        connection.commit()
    return canonical, noncanonical, duplicates

def finish_inventory_snapshot(snapshot_id: str, raw_records: int, noncanonical: int, duplicates: int) -> dict[str, Any]:
    with connect() as connection:
        root_total = int(connection.execute('SELECT COUNT(*) FROM bacterial_inventory_membership WHERE snapshot_id = %s', (snapshot_id,)).fetchone()[0])
        status = 'completed' if root_total > 0 and noncanonical == 0 else 'failed'
        summary = {
            'source_database': CANONICAL_SOURCE_DATABASE, 'canonical_accession_namespace': CANONICAL_ACCESSION_NAMESPACE,
            'taxon_id': BACTERIA_TAXON_ID, 'root_unique_assemblies': root_total,
            'raw_records': raw_records, 'noncanonical_records': noncanonical, 'duplicate_records': duplicates,
        }
        connection.execute(
            """UPDATE bacterial_inventory_snapshot
            SET status = %s, completed_at = %s, raw_records = %s, root_unique_assemblies = %s,
                noncanonical_records = %s, duplicate_records = %s, summary_json = %s
            WHERE snapshot_id = %s""",
            (status, utc_now(), raw_records, root_total, noncanonical, duplicates, Jsonb(summary), snapshot_id),
        )
        connection.commit()
        summary['status'] = status
        return summary

def fail_inventory_snapshot(snapshot_id: str, error: str) -> None:
    with connect() as connection:
        connection.execute('UPDATE bacterial_inventory_snapshot SET status = %s, completed_at = %s, error = %s WHERE snapshot_id = %s', ('failed', utc_now(), error[:4000], snapshot_id))
        connection.commit()


def metadata_payload_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')).hexdigest()

def seed_standardized_metadata_batch(
    snapshot_id: str,
    rows: Iterable[dict[str, Any]],
    *,
    rule_fingerprint: str,
) -> dict[str, int]:
    total = seeded = skipped = 0
    now = utc_now()
    with connect() as connection:
        with connection.cursor() as cursor:
            for payload in rows:
                total += 1
                accession = str(payload.get('Assembly Accession') or payload.get('assembly_accession') or '').strip()
                if not accession:
                    skipped += 1
                    continue
                input_fingerprint = str(payload.get('FetchM_Standardization_Input_Fingerprint') or '').strip()
                if not input_fingerprint:
                    input_fingerprint = metadata_payload_fingerprint(payload)
                result = cursor.execute(
                    """
                    INSERT INTO assembly_standardization (
                        assembly_accession, input_fingerprint, rule_fingerprint,
                        standardized_payload, status, updated_at
                    )
                    SELECT %s, %s, %s, %s, 'reused_existing', %s
                    WHERE EXISTS (
                        SELECT 1 FROM bacterial_inventory_membership
                        WHERE snapshot_id = %s AND assembly_accession = %s
                    )
                    ON CONFLICT (assembly_accession) DO UPDATE SET
                        input_fingerprint = EXCLUDED.input_fingerprint,
                        rule_fingerprint = EXCLUDED.rule_fingerprint,
                        standardized_payload = EXCLUDED.standardized_payload,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at
                    RETURNING assembly_accession
                    """,
                    (accession, input_fingerprint, rule_fingerprint, Jsonb(payload), now, snapshot_id, accession),
                ).fetchone()
                if result is None:
                    skipped += 1
                else:
                    seeded += 1
        connection.commit()
    return {'total': total, 'seeded': seeded, 'skipped': skipped}

def standardized_metadata_coverage(snapshot_id: str) -> dict[str, int]:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS root_total,
                   COUNT(s.assembly_accession) AS standardized_total,
                   COUNT(*) - COUNT(s.assembly_accession) AS missing_total
            FROM bacterial_inventory_membership i
            LEFT JOIN assembly_standardization s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()
    return {
        'root_unique_assemblies': int(row[0] or 0),
        'standardized_assemblies': int(row[1] or 0),
        'missing_standardized_assemblies': int(row[2] or 0),
    }


NON_CANONICAL_SPECIES_TOKENS = {
    'sp', 'sp.', 'spp', 'spp.', 'bacterium', 'archaeon', 'microorganism',
    'metagenome', 'uncultured', 'unclassified', 'endosymbiont', 'symbiont',
}

def normalize_taxon_label(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '').strip())

def canonical_partition_from_organism_name(value: Any) -> dict[str, str]:
    name = normalize_taxon_label(value)
    if not name:
        return {
            'genus_name': '', 'species_label': '', 'partition_type': 'unresolved_genus',
            'assignment_confidence': 'none', 'assignment_reason': 'missing_organism_name',
        }
    parts = name.split()
    offset = 1 if parts and parts[0].casefold() == 'candidatus' else 0
    if len(parts) <= offset:
        return {
            'genus_name': '', 'species_label': '', 'partition_type': 'unresolved_genus',
            'assignment_confidence': 'none', 'assignment_reason': 'missing_genus_token',
        }
    genus = parts[offset].strip().rstrip('.,;:')
    if not re.match(r'^[A-Z][A-Za-z0-9_-]*$', genus):
        return {
            'genus_name': '', 'species_label': name, 'partition_type': 'unresolved_genus',
            'assignment_confidence': 'low', 'assignment_reason': 'invalid_genus_token',
        }
    if len(parts) < offset + 2:
        return {
            'genus_name': genus, 'species_label': '', 'partition_type': 'genus_only',
            'assignment_confidence': 'medium', 'assignment_reason': 'no_species_token',
        }
    lower = name.casefold()
    epithet = parts[offset + 1].strip().rstrip('.,;:')
    cleaned_epithet = epithet.casefold()
    provisional_marker = (
        offset == 1
        or 'uncultured' in lower
        or 'metagenome' in lower
        or 'unclassified' in lower
        or ' sp.' in lower
        or ' spp.' in lower
        or lower.endswith(' sp')
        or lower.endswith(' spp')
        or re.search(r'\bsp\d', lower) is not None
        or ' species complex' in lower
        or ' group ' in lower
        or lower.endswith(' group')
        or ' clade ' in lower
        or lower.endswith(' clade')
        or any(char in name for char in '()[]')
    )
    if not provisional_marker and re.match(r'^[a-z][a-z0-9-]*$', cleaned_epithet) and cleaned_epithet not in NON_CANONICAL_SPECIES_TOKENS:
        return {
            'genus_name': genus, 'species_label': f'{genus} {epithet}', 'partition_type': 'named_species',
            'assignment_confidence': 'high', 'assignment_reason': 'canonical_binomial',
        }
    return {
        'genus_name': genus, 'species_label': name, 'partition_type': 'provisional_species',
        'assignment_confidence': 'medium' if provisional_marker else 'low',
        'assignment_reason': 'provisional_or_noncanonical_species_label',
    }

def materialize_partitions_from_inventory(
    snapshot_id: str,
    dataset_version_id: str,
    *,
    batch_size: int = 10000,
    release_views_materialized: bool = False,
) -> dict[str, Any]:
    bootstrap_schema()
    counts = {
        'named_species': 0, 'provisional_species': 0, 'genus_only': 0,
        'unresolved_genus': 0, 'excluded': 0,
    }
    processed = 0
    last_accession = ''
    with connect() as connection:
        inventory = connection.execute(
            'SELECT status, root_unique_assemblies FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            raise RuntimeError('Canonical inventory snapshot is not completed.')
        connection.execute('DELETE FROM taxon_partition_membership WHERE snapshot_id = %s', (snapshot_id,))
        connection.commit()
        while True:
            rows = connection.execute(
                """
                SELECT i.assembly_accession, COALESCE(m.organism_name, '') AS organism_name
                FROM bacterial_inventory_membership i
                JOIN assembly_master m ON m.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s AND i.assembly_accession > %s
                ORDER BY i.assembly_accession
                LIMIT %s
                """,
                (snapshot_id, last_accession, int(batch_size)),
            ).fetchall()
            if not rows:
                break
            payload = []
            for accession, organism_name in rows:
                partition = canonical_partition_from_organism_name(organism_name)
                counts[partition['partition_type']] = counts.get(partition['partition_type'], 0) + 1
                payload.append((
                    snapshot_id, accession, partition['genus_name'], partition['species_label'],
                    partition['partition_type'], partition['assignment_confidence'], partition['assignment_reason'],
                ))
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO taxon_partition_membership (
                        snapshot_id, assembly_accession, genus_name, species_label,
                        partition_type, assignment_confidence, assignment_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_id, assembly_accession) DO UPDATE SET
                        genus_name = EXCLUDED.genus_name,
                        species_label = EXCLUDED.species_label,
                        partition_type = EXCLUDED.partition_type,
                        assignment_confidence = EXCLUDED.assignment_confidence,
                        assignment_reason = EXCLUDED.assignment_reason
                    """,
                    payload,
                )
            connection.commit()
            processed += len(rows)
            last_accession = str(rows[-1][0])
    summary = reconcile_root_partitions(
        snapshot_id,
        dataset_version_id,
        release_views_materialized=release_views_materialized,
    )
    summary.update({
        'processed_assemblies': processed,
        'partition_counts': counts,
        'release_views_materialized': bool(release_views_materialized),
    })
    return summary


def reconcile_root_partitions(snapshot_id: str, dataset_version_id: str, *, release_views_materialized: bool = False) -> dict[str, Any]:
    bootstrap_schema()
    with connect() as connection:
        inventory = connection.execute(
            'SELECT status, root_unique_assemblies FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            raise RuntimeError('Canonical inventory snapshot is not completed.')
        rows = connection.execute(
            """
            SELECT p.partition_type, COUNT(*)
            FROM bacterial_inventory_membership i
            LEFT JOIN taxon_partition_membership p
              ON p.snapshot_id = i.snapshot_id AND p.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            GROUP BY p.partition_type
            """,
            (snapshot_id,),
        ).fetchall()
        counts = {str(kind) if kind is not None else 'unaccounted': int(total) for kind, total in rows}
        root_total = int(inventory[1] or 0)
        unaccounted = int(counts.get('unaccounted', 0))
        accounted = root_total - unaccounted
        status = 'pass' if root_total > 0 and unaccounted == 0 else 'fail'
        summary = {
            'status': status, 'snapshot_id': snapshot_id, 'dataset_version_id': dataset_version_id,
            'source_database': CANONICAL_SOURCE_DATABASE, 'canonical_accession_namespace': CANONICAL_ACCESSION_NAMESPACE,
            'release_views_materialized': bool(release_views_materialized),
            'root_unique_assemblies': root_total, 'accounted_unique_assemblies': accounted,
            'named_species_assemblies': int(counts.get('named_species', 0)),
            'provisional_species_assemblies': int(counts.get('provisional_species', 0)),
            'genus_only_assemblies': int(counts.get('genus_only', 0)),
            'unresolved_genus_assemblies': int(counts.get('unresolved_genus', 0)),
            'excluded_assemblies': int(counts.get('excluded', 0)),
            'unaccounted_assemblies': unaccounted,
        }
        connection.execute(
            """
            INSERT INTO canonical_root_reconciliation (
                snapshot_id, dataset_version_id, status, generated_at, root_unique_assemblies,
                accounted_unique_assemblies, named_species_assemblies, provisional_species_assemblies,
                genus_only_assemblies, unresolved_genus_assemblies, excluded_assemblies,
                unaccounted_assemblies, summary_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                dataset_version_id = EXCLUDED.dataset_version_id, status = EXCLUDED.status,
                generated_at = EXCLUDED.generated_at, root_unique_assemblies = EXCLUDED.root_unique_assemblies,
                accounted_unique_assemblies = EXCLUDED.accounted_unique_assemblies,
                named_species_assemblies = EXCLUDED.named_species_assemblies,
                provisional_species_assemblies = EXCLUDED.provisional_species_assemblies,
                genus_only_assemblies = EXCLUDED.genus_only_assemblies,
                unresolved_genus_assemblies = EXCLUDED.unresolved_genus_assemblies,
                excluded_assemblies = EXCLUDED.excluded_assemblies,
                unaccounted_assemblies = EXCLUDED.unaccounted_assemblies, summary_json = EXCLUDED.summary_json
            """,
            (snapshot_id, dataset_version_id, status, utc_now(), root_total, accounted, summary['named_species_assemblies'], summary['provisional_species_assemblies'], summary['genus_only_assemblies'], summary['unresolved_genus_assemblies'], summary['excluded_assemblies'], unaccounted, Jsonb(summary)),
        )
        connection.commit()
        return summary

def latest_reconciliation_for_dataset_version(dataset_version_id: str) -> dict[str, Any] | None:
    with connect() as connection:
        row = connection.execute(
            'SELECT summary_json FROM canonical_root_reconciliation WHERE dataset_version_id = %s ORDER BY generated_at DESC LIMIT 1',
            (dataset_version_id,),
        ).fetchone()
    return dict(row[0]) if row is not None else None
