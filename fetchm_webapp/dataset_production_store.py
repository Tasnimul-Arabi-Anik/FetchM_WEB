"""PostgreSQL storage for canonical GenBank inventory and release accounting."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from domain_profiles import (
    ARCHAEA_PROFILE,
    BACTERIA_PROFILE,
    domain_profile,
    domain_profile_from_snapshot_id,
    domain_profile_from_taxon_id,
    validate_snapshot_id_for_profile,
)

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # Runtime dependency is installed in the application image.
    psycopg = None
    Jsonb = None

DATASET_DATABASE_URL_ENV = "FETCHM_WEBAPP_DATASET_DATABASE_URL"
CANONICAL_SOURCE_DATABASE = "genbank"
CANONICAL_ACCESSION_NAMESPACE = "GCA"
BACTERIA_TAXON_ID = BACTERIA_PROFILE.ncbi_taxon_id
ARCHAEA_TAXON_ID = ARCHAEA_PROFILE.ncbi_taxon_id

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS canonical_inventory_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    continue_after BOOLEAN NOT NULL DEFAULT FALSE,
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
    continue_after BOOLEAN NOT NULL DEFAULT FALSE,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    sqlite_db_path TEXT,
    rule_fingerprint TEXT,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS canonical_metadata_fetch_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    continue_after BOOLEAN NOT NULL DEFAULT FALSE,
    refetch_all BOOLEAN NOT NULL DEFAULT FALSE,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS canonical_metadata_restandardization_task (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    continue_after BOOLEAN NOT NULL DEFAULT FALSE,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    rule_fingerprint TEXT,
    error TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS canonical_inventory_chunk (
    snapshot_id TEXT NOT NULL,
    chunk_key TEXT NOT NULL,
    released_after DATE NOT NULL,
    released_before DATE NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    raw_records BIGINT NOT NULL DEFAULT 0,
    canonical_records BIGINT NOT NULL DEFAULT 0,
    noncanonical_records BIGINT NOT NULL DEFAULT 0,
    duplicate_records BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    PRIMARY KEY (snapshot_id, chunk_key)
);

CREATE TABLE IF NOT EXISTS canonical_inventory_page (
    snapshot_id TEXT NOT NULL,
    page_number BIGINT NOT NULL,
    input_page_token TEXT,
    next_page_token TEXT,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    expected_total BIGINT,
    raw_records BIGINT NOT NULL DEFAULT 0,
    canonical_records BIGINT NOT NULL DEFAULT 0,
    noncanonical_records BIGINT NOT NULL DEFAULT 0,
    duplicate_records BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    PRIMARY KEY (snapshot_id, page_number)
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

CREATE TABLE IF NOT EXISTS canonical_taxonomy_lineage_taxid (
    tax_id BIGINT PRIMARY KEY,
    reported_rank TEXT,
    domain_name TEXT,
    phylum_name TEXT,
    class_name TEXT,
    order_name TEXT,
    family_name TEXT,
    genus_name TEXT,
    species_name TEXT,
    lineage_text TEXT,
    taxonomy_source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_taxonomy_lineage_snapshot (
    snapshot_id TEXT PRIMARY KEY REFERENCES bacterial_inventory_snapshot(snapshot_id) ON DELETE CASCADE,
    generated_at TIMESTAMPTZ NOT NULL,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS assembly_taxonomy_lineage (
    snapshot_id TEXT NOT NULL REFERENCES bacterial_inventory_snapshot(snapshot_id) ON DELETE CASCADE,
    assembly_accession TEXT NOT NULL REFERENCES assembly_master(assembly_accession),
    tax_id BIGINT,
    reported_name TEXT,
    reported_rank TEXT,
    domain_name TEXT,
    phylum_name TEXT,
    class_name TEXT,
    order_name TEXT,
    family_name TEXT,
    genus_name TEXT,
    species_name TEXT,
    taxonomy_source TEXT NOT NULL,
    resolution_status TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, assembly_accession)
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

ALTER TABLE canonical_inventory_task ADD COLUMN IF NOT EXISTS continue_after BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE canonical_metadata_seed_task ADD COLUMN IF NOT EXISTS continue_after BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE canonical_metadata_fetch_task ADD COLUMN IF NOT EXISTS continue_after BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE canonical_metadata_fetch_task ADD COLUMN IF NOT EXISTS refetch_all BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE canonical_metadata_restandardization_task ADD COLUMN IF NOT EXISTS continue_after BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_inventory_membership_accession
ON bacterial_inventory_membership (assembly_accession);
CREATE INDEX IF NOT EXISTS idx_inventory_chunk_status
ON canonical_inventory_chunk (snapshot_id, status, released_after);
CREATE INDEX IF NOT EXISTS idx_inventory_page_status
ON canonical_inventory_page (snapshot_id, status, page_number);
CREATE INDEX IF NOT EXISTS idx_partition_type
ON taxon_partition_membership (snapshot_id, partition_type);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_domain
ON assembly_taxonomy_lineage (snapshot_id, domain_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_phylum
ON assembly_taxonomy_lineage (snapshot_id, phylum_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_class
ON assembly_taxonomy_lineage (snapshot_id, class_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_order
ON assembly_taxonomy_lineage (snapshot_id, order_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_family
ON assembly_taxonomy_lineage (snapshot_id, family_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_genus
ON assembly_taxonomy_lineage (snapshot_id, genus_name);
CREATE INDEX IF NOT EXISTS idx_assembly_taxonomy_lineage_species
ON assembly_taxonomy_lineage (snapshot_id, species_name);
CREATE INDEX IF NOT EXISTS idx_canonical_partition_task_status
ON canonical_partition_task (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_canonical_metadata_seed_task_status
ON canonical_metadata_seed_task (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_canonical_metadata_fetch_task_status
ON canonical_metadata_fetch_task (status, requested_at);
CREATE INDEX IF NOT EXISTS idx_canonical_metadata_restandardization_task_status
ON canonical_metadata_restandardization_task (status, requested_at);
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

def active_canonical_pipeline_task(connection: Any | None = None) -> tuple[str, str, str] | None:
    """Return the oldest staged canonical operation still in progress."""
    query = """
        SELECT snapshot_id, status, task_type FROM (
            SELECT snapshot_id, status, requested_at, 'inventory' AS task_type FROM canonical_inventory_task WHERE status IN ('pending', 'running')
            UNION ALL
            SELECT snapshot_id, status, requested_at, 'metadata_seed' AS task_type FROM canonical_metadata_seed_task WHERE status IN ('pending', 'running')
            UNION ALL
            SELECT snapshot_id, status, requested_at, 'metadata_fetch' AS task_type FROM canonical_metadata_fetch_task WHERE status IN ('pending', 'running')
            UNION ALL
            SELECT snapshot_id, status, requested_at, 'metadata_restandardization' AS task_type FROM canonical_metadata_restandardization_task WHERE status IN ('pending', 'running')
            UNION ALL
            SELECT snapshot_id, status, requested_at, 'partitions' AS task_type FROM canonical_partition_task WHERE status IN ('pending', 'running')
        ) active_tasks ORDER BY requested_at ASC LIMIT 1
    """
    if connection is not None:
        row = connection.execute(query).fetchone()
    else:
        bootstrap_schema()
        with connect() as owned_connection:
            row = owned_connection.execute(query).fetchone()
    if row is None:
        return None
    return str(row[0]), str(row[1]), str(row[2])


def queue_inventory_task(
    requested_by: str | None = None,
    *,
    continue_after: bool = False,
    profile_key: str = BACTERIA_PROFILE.key,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    profile = domain_profile(profile_key)
    snapshot_id = profile.snapshot_id(utc_now())
    with connect() as connection:
        active = active_canonical_pipeline_task(connection)
        if active is not None:
            return None, f'Canonical inventory task {active[0]} is already {active[1]}.'
        connection.execute(
            "INSERT INTO canonical_inventory_task (snapshot_id, status, requested_by, requested_at, continue_after) VALUES (%s, 'pending', %s, %s, %s)",
            (snapshot_id, requested_by, utc_now(), bool(continue_after)),
        )
        connection.commit()
    return snapshot_id, None

def claim_inventory_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, continue_after FROM canonical_inventory_task
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
        return {'id': int(row[0]), 'snapshot_id': str(row[1]), 'continue_after': bool(row[2])}

def finish_inventory_task(task_id: int, status: str, error: str | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_inventory_task SET status = %s, completed_at = %s, error = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, task_id),
        )
        connection.commit()


def requeue_inventory_task(task_id: int, error: str | None = None) -> None:
    with connect() as connection:
        connection.execute(
            """
            UPDATE canonical_inventory_task
            SET status = 'pending', claimed_by = NULL, claimed_at = NULL, completed_at = NULL, error = %s
            WHERE id = %s
            """,
            (error[:4000] if error else None, task_id),
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
        active = active_canonical_pipeline_task(connection)
        if active is not None:
            return None, f'Canonical partition task for {active[0]} is already {active[1]}.'
        inventory = connection.execute(
            "SELECT status FROM bacterial_inventory_snapshot WHERE snapshot_id = %s",
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            return None, 'Canonical inventory snapshot is not completed.'
        missing_standardized = int(connection.execute(
            """
            SELECT COUNT(*)
            FROM bacterial_inventory_membership AS i
            LEFT JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s AND s.assembly_accession IS NULL
            """,
            (snapshot_id,),
        ).fetchone()[0] or 0)
        if missing_standardized:
            return None, f'Canonical metadata is incomplete: {missing_standardized:,} assemblies still require retrieval and standardization.'
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
    continue_after: bool = False,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    if not snapshot_id:
        latest = latest_completed_inventory_snapshot()
        if latest is None:
            return None, 'No completed canonical inventory snapshot is available.'
        snapshot_id = str(latest['snapshot_id'])
    with connect() as connection:
        active = active_canonical_pipeline_task(connection)
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
                snapshot_id, status, requested_by, requested_at, sqlite_db_path, rule_fingerprint, continue_after
            ) VALUES (%s, 'pending', %s, %s, %s, %s, %s)
            """,
            (snapshot_id, requested_by, utc_now(), sqlite_db_path, rule_fingerprint, bool(continue_after)),
        )
        connection.commit()
    return snapshot_id, None

def claim_metadata_seed_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, sqlite_db_path, rule_fingerprint, continue_after FROM canonical_metadata_seed_task
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
            'sqlite_db_path': str(row[2] or ''), 'rule_fingerprint': str(row[3] or ''), 'continue_after': bool(row[4]),
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

def queue_metadata_fetch_task(
    requested_by: str | None = None,
    *,
    snapshot_id: str | None = None,
    continue_after: bool = False,
    refetch_all: bool = False,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    if not snapshot_id:
        latest = latest_completed_inventory_snapshot()
        if latest is None:
            return None, 'No completed canonical inventory snapshot is available.'
        snapshot_id = str(latest['snapshot_id'])
    coverage = standardized_metadata_coverage(snapshot_id)
    if not refetch_all and not coverage['missing_standardized_assemblies']:
        return None, 'Canonical metadata coverage is already complete.'
    with connect() as connection:
        active = active_canonical_pipeline_task(connection)
        if active is not None:
            return None, f'Canonical missing-metadata fetch for {active[0]} is already {active[1]}.'
        connection.execute(
            "INSERT INTO canonical_metadata_fetch_task (snapshot_id, status, requested_by, requested_at, continue_after, refetch_all) VALUES (%s, 'pending', %s, %s, %s, %s)",
            (snapshot_id, requested_by, utc_now(), bool(continue_after), bool(refetch_all)),
        )
        connection.commit()
    return snapshot_id, None

def claim_metadata_fetch_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, continue_after, refetch_all FROM canonical_metadata_fetch_task
            WHERE status = 'pending' ORDER BY requested_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            "UPDATE canonical_metadata_fetch_task SET status = 'running', claimed_by = %s, claimed_at = %s WHERE id = %s",
            (worker_name, utc_now(), row[0]),
        )
        connection.commit()
        return {'id': int(row[0]), 'snapshot_id': str(row[1]), 'continue_after': bool(row[2]), 'refetch_all': bool(row[3])}

def finish_metadata_fetch_task(task_id: int, status: str, error: str | None = None, summary: dict[str, Any] | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_metadata_fetch_task SET status = %s, completed_at = %s, error = %s, summary_json = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, Jsonb(summary or {}), task_id),
        )
        connection.commit()

def latest_metadata_fetch_task() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            'SELECT snapshot_id, status, requested_at, claimed_at, completed_at, error, summary_json FROM canonical_metadata_fetch_task ORDER BY requested_at DESC LIMIT 1'
        ).fetchone()
    if row is None:
        return None
    return {
        'snapshot_id': row[0], 'status': row[1], 'requested_at': row[2], 'claimed_at': row[3],
        'completed_at': row[4], 'error': row[5], 'summary': dict(row[6] or {}),
    }

def queue_metadata_restandardization_task(
    requested_by: str | None = None,
    *,
    snapshot_id: str | None = None,
    rule_fingerprint: str | None = None,
    continue_after: bool = False,
) -> tuple[str | None, str | None]:
    bootstrap_schema()
    if not snapshot_id:
        latest = latest_completed_inventory_snapshot()
        if latest is None:
            return None, 'No completed canonical inventory snapshot is available.'
        snapshot_id = str(latest['snapshot_id'])
    with connect() as connection:
        active = active_canonical_pipeline_task(connection)
        if active is not None:
            return None, f'Canonical metadata re-standardization for {active[0]} is already {active[1]}.'
        inventory = connection.execute(
            'SELECT status FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            return None, 'Canonical inventory snapshot is not completed.'
        coverage = standardized_metadata_coverage(snapshot_id)
        if not coverage['standardized_assemblies']:
            return None, 'No standardized canonical metadata exists yet; fetch or seed metadata first.'
        connection.execute(
            """
            INSERT INTO canonical_metadata_restandardization_task (
                snapshot_id, status, requested_by, requested_at, rule_fingerprint, continue_after
            ) VALUES (%s, 'pending', %s, %s, %s, %s)
            """,
            (snapshot_id, requested_by, utc_now(), rule_fingerprint, bool(continue_after)),
        )
        connection.commit()
    return snapshot_id, None


def claim_metadata_restandardization_task(worker_name: str) -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT id, snapshot_id, rule_fingerprint, continue_after
            FROM canonical_metadata_restandardization_task
            WHERE status = 'pending'
            ORDER BY requested_at ASC
            FOR UPDATE SKIP LOCKED LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            'UPDATE canonical_metadata_restandardization_task SET status = %s, claimed_by = %s, claimed_at = %s WHERE id = %s',
            ('running', worker_name, utc_now(), row[0]),
        )
        connection.commit()
        return {'id': int(row[0]), 'snapshot_id': str(row[1]), 'rule_fingerprint': str(row[2] or ''), 'continue_after': bool(row[3])}


def finish_metadata_restandardization_task(task_id: int, status: str, error: str | None = None, summary: dict[str, Any] | None = None) -> None:
    with connect() as connection:
        connection.execute(
            'UPDATE canonical_metadata_restandardization_task SET status = %s, completed_at = %s, error = %s, summary_json = %s WHERE id = %s',
            (status, utc_now(), error[:4000] if error else None, Jsonb(summary or {}), task_id),
        )
        connection.commit()


def latest_metadata_restandardization_task() -> dict[str, Any] | None:
    bootstrap_schema()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT snapshot_id, status, requested_at, claimed_at, completed_at, rule_fingerprint, error, summary_json
            FROM canonical_metadata_restandardization_task
            ORDER BY requested_at DESC LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return {
        'snapshot_id': row[0], 'status': row[1], 'requested_at': row[2], 'claimed_at': row[3],
        'completed_at': row[4], 'rule_fingerprint': row[5], 'error': row[6], 'summary': dict(row[7] or {}),
    }


def inventory_accession_batch(snapshot_id: str, *, after_accession: str = "", limit: int = 100) -> list[str]:
    bootstrap_schema()
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT assembly_accession
            FROM bacterial_inventory_membership
            WHERE snapshot_id = %s AND assembly_accession > %s
            ORDER BY assembly_accession
            LIMIT %s
            """,
            (snapshot_id, after_accession, min(1000, max(1, int(limit)))),
        ).fetchall()
    return [str(row[0]) for row in rows]


def missing_standardized_accession_batch(snapshot_id: str, *, limit: int = 100) -> list[str]:
    bootstrap_schema()
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT i.assembly_accession
            FROM bacterial_inventory_membership AS i
            LEFT JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s AND s.assembly_accession IS NULL
            ORDER BY i.assembly_accession
            LIMIT %s
            """,
            (snapshot_id, min(1000, max(1, int(limit)))),
        ).fetchall()
    return [str(row[0]) for row in rows]

def start_inventory_snapshot(
    snapshot_id: str,
    invocation: str,
    datasets_version: str | None,
    *,
    profile_key: str = BACTERIA_PROFILE.key,
) -> None:
    bootstrap_schema()
    profile = domain_profile(profile_key)
    snapshot_id = validate_snapshot_id_for_profile(snapshot_id, profile)
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO bacterial_inventory_snapshot (
                snapshot_id, status, requested_at, started_at, source_database,
                canonical_accession_namespace, taxon_id, invocation, datasets_version
            ) VALUES (%s, 'running', %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                status = 'running', started_at = EXCLUDED.started_at, completed_at = NULL,
                taxon_id = EXCLUDED.taxon_id, invocation = EXCLUDED.invocation,
                datasets_version = EXCLUDED.datasets_version, error = NULL
            """,
            (snapshot_id, utc_now(), utc_now(), CANONICAL_SOURCE_DATABASE, CANONICAL_ACCESSION_NAMESPACE, profile.ncbi_taxon_id, invocation, datasets_version),
        )
        connection.commit()

def start_inventory_chunk(snapshot_id: str, chunk_key: str, released_after: str, released_before: str) -> None:
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO canonical_inventory_chunk (
                snapshot_id, chunk_key, released_after, released_before, status, attempt_count, started_at
            ) VALUES (%s, %s, %s, %s, 'running', 1, %s)
            ON CONFLICT (snapshot_id, chunk_key) DO UPDATE SET
                status = 'running', attempt_count = canonical_inventory_chunk.attempt_count + 1,
                started_at = EXCLUDED.started_at, completed_at = NULL, error = NULL
            """,
            (snapshot_id, chunk_key, released_after, released_before, utc_now()),
        )
        connection.commit()

def completed_inventory_chunks(snapshot_id: str) -> set[str]:
    with connect() as connection:
        rows = connection.execute(
            "SELECT chunk_key FROM canonical_inventory_chunk WHERE snapshot_id = %s AND status = 'completed'",
            (snapshot_id,),
        ).fetchall()
    return {str(row[0]) for row in rows}

def finish_inventory_chunk(snapshot_id: str, chunk_key: str, status: str, *, raw_records: int = 0, canonical_records: int = 0, noncanonical_records: int = 0, duplicate_records: int = 0, error: str | None = None) -> None:
    with connect() as connection:
        connection.execute(
            """
            UPDATE canonical_inventory_chunk SET status = %s, completed_at = %s, raw_records = %s,
                   canonical_records = %s, noncanonical_records = %s, duplicate_records = %s, error = %s
            WHERE snapshot_id = %s AND chunk_key = %s
            """,
            (status, utc_now(), raw_records, canonical_records, noncanonical_records, duplicate_records, error[:4000] if error else None, snapshot_id, chunk_key),
        )
        connection.commit()

def inventory_chunk_progress(snapshot_id: str) -> dict[str, int]:
    with connect() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'completed'), COUNT(*) FILTER (WHERE status = 'failed'),
                   COALESCE(SUM(raw_records) FILTER (WHERE status = 'completed'), 0),
                   COALESCE(SUM(noncanonical_records) FILTER (WHERE status = 'completed'), 0),
                   COALESCE(SUM(duplicate_records) FILTER (WHERE status = 'completed'), 0)
            FROM canonical_inventory_chunk WHERE snapshot_id = %s
            """, (snapshot_id,),
        ).fetchone()
    return {'chunk_total': int(row[0] or 0), 'chunk_completed': int(row[1] or 0), 'chunk_failed': int(row[2] or 0), 'raw_records': int(row[3] or 0), 'noncanonical_records': int(row[4] or 0), 'duplicate_records': int(row[5] or 0)}

def start_inventory_page(snapshot_id: str, page_number: int, input_page_token: str | None) -> None:
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO canonical_inventory_page (
                snapshot_id, page_number, input_page_token, status, attempt_count, started_at
            ) VALUES (%s, %s, %s, 'running', 1, %s)
            ON CONFLICT (snapshot_id, page_number) DO UPDATE SET
                status = 'running', attempt_count = canonical_inventory_page.attempt_count + 1,
                input_page_token = EXCLUDED.input_page_token, started_at = EXCLUDED.started_at,
                completed_at = NULL, error = NULL
            """,
            (snapshot_id, page_number, input_page_token, utc_now()),
        )
        connection.commit()

def latest_inventory_page_checkpoint(snapshot_id: str) -> dict[str, Any] | None:
    with connect() as connection:
        row = connection.execute(
            """
            SELECT page_number, next_page_token, expected_total
            FROM canonical_inventory_page
            WHERE snapshot_id = %s AND status = 'completed'
            ORDER BY page_number DESC LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
    if row is None:
        return None
    return {'page_number': int(row[0]), 'next_page_token': row[1], 'expected_total': int(row[2] or 0)}

def finish_inventory_page(snapshot_id: str, page_number: int, status: str, *, next_page_token: str | None = None, expected_total: int = 0, raw_records: int = 0, canonical_records: int = 0, noncanonical_records: int = 0, duplicate_records: int = 0, error: str | None = None) -> None:
    with connect() as connection:
        connection.execute(
            """
            UPDATE canonical_inventory_page SET status = %s, completed_at = %s, next_page_token = %s,
                   expected_total = %s, raw_records = %s, canonical_records = %s,
                   noncanonical_records = %s, duplicate_records = %s, error = %s
            WHERE snapshot_id = %s AND page_number = %s
            """,
            (status, utc_now(), next_page_token, expected_total or None, raw_records, canonical_records, noncanonical_records, duplicate_records, error[:4000] if error else None, snapshot_id, page_number),
        )
        connection.commit()

def inventory_page_progress(snapshot_id: str) -> dict[str, int]:
    with connect() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) FILTER (WHERE status = 'completed'), COUNT(*) FILTER (WHERE status = 'failed'),
                   COALESCE(MAX(expected_total), 0),
                   COALESCE(SUM(raw_records) FILTER (WHERE status = 'completed'), 0),
                   COALESCE(SUM(noncanonical_records) FILTER (WHERE status = 'completed'), 0),
                   COALESCE(SUM(duplicate_records) FILTER (WHERE status = 'completed'), 0)
            FROM canonical_inventory_page WHERE snapshot_id = %s
            """, (snapshot_id,),
        ).fetchone()
    expected_total = int(row[2] or 0)
    return {
        'page_completed': int(row[0] or 0), 'page_failed': int(row[1] or 0),
        'expected_total': expected_total, 'expected_pages': (expected_total + 999) // 1000 if expected_total else 0,
        'raw_records': int(row[3] or 0), 'noncanonical_records': int(row[4] or 0),
        'duplicate_records': int(row[5] or 0),
    }

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
                    organism_name = COALESCE(NULLIF(EXCLUDED.organism_name, ''), assembly_master.organism_name),
                    tax_id = COALESCE(EXCLUDED.tax_id, assembly_master.tax_id),
                    species_tax_id = COALESCE(EXCLUDED.species_tax_id, assembly_master.species_tax_id),
                    biosample_accession = COALESCE(NULLIF(EXCLUDED.biosample_accession, ''), assembly_master.biosample_accession),
                    paired_refseq_accession = COALESCE(NULLIF(EXCLUDED.paired_refseq_accession, ''), assembly_master.paired_refseq_accession),
                    latest_snapshot_id = EXCLUDED.latest_snapshot_id,
                    raw_fingerprint = CASE WHEN EXCLUDED.organism_name <> '' OR EXCLUDED.biosample_accession <> '' THEN EXCLUDED.raw_fingerprint ELSE assembly_master.raw_fingerprint END,
                    raw_payload = CASE WHEN EXCLUDED.organism_name <> '' OR EXCLUDED.biosample_accession <> '' THEN EXCLUDED.raw_payload ELSE assembly_master.raw_payload END,
                    updated_at = EXCLUDED.updated_at
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
        inventory_row = connection.execute(
            'SELECT source_database, canonical_accession_namespace, taxon_id FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        source_database = str(inventory_row[0]) if inventory_row else CANONICAL_SOURCE_DATABASE
        accession_namespace = str(inventory_row[1]) if inventory_row else CANONICAL_ACCESSION_NAMESPACE
        taxon_id = int(inventory_row[2]) if inventory_row and inventory_row[2] is not None else BACTERIA_TAXON_ID
        profile = domain_profile_from_taxon_id(taxon_id)
        if profile == BACTERIA_PROFILE:
            profile = domain_profile_from_snapshot_id(snapshot_id)
        status = 'completed' if root_total > 0 and noncanonical == 0 else 'failed'
        summary = {
            'domain_profile': profile.key, 'domain_label': profile.label,
            'source_database': source_database, 'canonical_accession_namespace': accession_namespace,
            'taxon_id': taxon_id, 'root_unique_assemblies': root_total,
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

def finish_inventory_pilot_snapshot(
    snapshot_id: str,
    raw_records: int,
    noncanonical: int,
    duplicates: int,
    *,
    page_limit: int,
) -> dict[str, Any]:
    with connect() as connection:
        root_total = int(connection.execute('SELECT COUNT(*) FROM bacterial_inventory_membership WHERE snapshot_id = %s', (snapshot_id,)).fetchone()[0])
        inventory_row = connection.execute(
            'SELECT source_database, canonical_accession_namespace, taxon_id FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        source_database = str(inventory_row[0]) if inventory_row else CANONICAL_SOURCE_DATABASE
        accession_namespace = str(inventory_row[1]) if inventory_row else CANONICAL_ACCESSION_NAMESPACE
        taxon_id = int(inventory_row[2]) if inventory_row and inventory_row[2] is not None else BACTERIA_TAXON_ID
        profile = domain_profile_from_taxon_id(taxon_id)
        if profile == BACTERIA_PROFILE:
            profile = domain_profile_from_snapshot_id(snapshot_id)
        status = 'pilot_completed' if root_total > 0 and noncanonical == 0 else 'failed'
        summary = {
            'domain_profile': profile.key, 'domain_label': profile.label,
            'source_database': source_database, 'canonical_accession_namespace': accession_namespace,
            'taxon_id': taxon_id, 'root_unique_assemblies': root_total,
            'raw_records': raw_records, 'noncanonical_records': noncanonical, 'duplicate_records': duplicates,
            'pilot_page_limit': page_limit, 'inventory_mode': 'rest_page_pilot',
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
    status: str = 'reused_existing',
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
                    SELECT %s, %s, %s, %s, %s, %s
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
                    (accession, input_fingerprint, rule_fingerprint, Jsonb(payload), status, now, snapshot_id, accession),
                ).fetchone()
                if result is None:
                    skipped += 1
                else:
                    # Inventory pages may carry only accessions; hydrate taxonomy from reused metadata.
                    organism_name = str(payload.get('Organism Name') or '').strip()
                    biosample_accession = str(
                        payload.get('Assembly BioSample Accession')
                        or payload.get('BioSample Accession')
                        or ''
                    ).strip()
                    cursor.execute(
                        """
                        UPDATE assembly_master
                        SET organism_name = COALESCE(NULLIF(organism_name, ''), NULLIF(%s, '')),
                            biosample_accession = COALESCE(NULLIF(biosample_accession, ''), NULLIF(%s, '')),
                            updated_at = %s
                        WHERE assembly_accession = %s
                          AND (
                              (NULLIF(organism_name, '') IS NULL AND NULLIF(%s, '') IS NOT NULL)
                              OR (NULLIF(biosample_accession, '') IS NULL AND NULLIF(%s, '') IS NOT NULL)
                          )
                        """,
                        (organism_name, biosample_accession, now, accession, organism_name, biosample_accession),
                    )
                    seeded += 1
        connection.commit()
    return {'total': total, 'seeded': seeded, 'skipped': skipped}

def hydrate_master_taxonomy_from_standardization(snapshot_id: str, *, batch_size: int = 10000) -> dict[str, int]:
    """Backfill master taxonomy fields from standardized metadata in bounded transactions."""
    bootstrap_schema()
    hydrated = examined = 0
    last_accession = ''
    with connect() as connection:
        while True:
            rows = connection.execute(
                """
                SELECT i.assembly_accession
                FROM bacterial_inventory_membership AS i
                JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
                WHERE i.snapshot_id = %s AND i.assembly_accession > %s
                ORDER BY i.assembly_accession
                LIMIT %s
                """,
                (snapshot_id, last_accession, int(batch_size)),
            ).fetchall()
            if not rows:
                break
            accessions = [str(row[0]) for row in rows]
            cursor = connection.execute(
                """
                UPDATE assembly_master AS m
                SET organism_name = COALESCE(
                        NULLIF(m.organism_name, ''),
                        NULLIF(s.standardized_payload->>'Organism Name', '')
                    ),
                    biosample_accession = COALESCE(
                        NULLIF(m.biosample_accession, ''),
                        NULLIF(s.standardized_payload->>'Assembly BioSample Accession', ''),
                        NULLIF(s.standardized_payload->>'BioSample Accession', '')
                    ),
                    updated_at = %s
                FROM assembly_standardization AS s
                WHERE m.assembly_accession = s.assembly_accession
                  AND m.assembly_accession = ANY(%s)
                  AND (
                      (NULLIF(m.organism_name, '') IS NULL AND NULLIF(s.standardized_payload->>'Organism Name', '') IS NOT NULL)
                      OR (
                          NULLIF(m.biosample_accession, '') IS NULL
                          AND COALESCE(
                              NULLIF(s.standardized_payload->>'Assembly BioSample Accession', ''),
                              NULLIF(s.standardized_payload->>'BioSample Accession', '')
                          ) IS NOT NULL
                      )
                  )
                """,
                (utc_now(), accessions),
            )
            hydrated += int(cursor.rowcount or 0)
            examined += len(accessions)
            last_accession = accessions[-1]
            connection.commit()
    return {'master_rows_examined': examined, 'master_rows_hydrated': hydrated}

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
HIGH_RANK_PLACEHOLDER_TOKENS = {
    # Common phylum/class labels that can appear in NCBI organism names as "<rank> bacterium".
    # Keep these out of genus counts while preserving the assembly in unresolved accounting.
    'acidobacteria', 'acidobacteriota', 'actinomycetes', 'actinomycetota', 'alphaproteobacteria',
    'bacilli', 'bacillota', 'bacteroidetes', 'bacteroidota', 'chloroflexi', 'chloroflexota',
    'clostridia', 'cyanobacteria', 'dehalococcoidia', 'deltaproteobacteria', 'firmicutes',
    'gammaproteobacteria', 'patescibacteria', 'planctomycetes', 'planctomycetota',
    'proteobacteria', 'pseudomonadota', 'verrucomicrobia', 'verrucomicrobiota',
}
HIGH_RANK_PLACEHOLDER_SUFFIXES = ('ota', 'ales', 'aceae')

def normalize_taxon_label(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '').strip())

def is_high_rank_placeholder_label(genus: str, epithet: str) -> bool:
    cleaned_genus = genus.strip().rstrip('.,;:')
    lower_genus = cleaned_genus.casefold()
    lower_epithet = epithet.strip().rstrip('.,;:').casefold()
    if lower_epithet not in {'bacterium', 'archaeon', 'microorganism'}:
        return False
    return lower_genus in HIGH_RANK_PLACEHOLDER_TOKENS or lower_genus.endswith(HIGH_RANK_PLACEHOLDER_SUFFIXES)

def canonical_partition_from_organism_name(value: Any) -> dict[str, str]:
    name = normalize_taxon_label(value)
    if not name:
        return {
            'genus_name': '', 'species_label': '', 'partition_type': 'unresolved_genus',
            'assignment_confidence': 'none', 'assignment_reason': 'missing_organism_name',
        }
    parts = name.split()
    lower_parts = [part.strip().rstrip('.,;:').casefold() for part in parts]
    offset = 1 if parts and parts[0].casefold() == 'candidatus' else 0
    if (
        len(parts) >= 3
        and lower_parts[0] in {'uncultured', 'unclassified'}
        and lower_parts[2] in {'sp', 'sp.', 'spp', 'spp.'}
        and re.match(r'^[A-Z][A-Za-z0-9_-]*$', parts[1].strip().rstrip('.,;:'))
    ):
        return {
            'genus_name': parts[1].strip().rstrip('.,;:'),
            'species_label': name,
            'partition_type': 'provisional_species',
            'assignment_confidence': 'medium',
            'assignment_reason': 'recoverable_uncultured_genus_label',
        }
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
    if is_high_rank_placeholder_label(genus, epithet):
        return {
            'genus_name': '', 'species_label': name, 'partition_type': 'unresolved_genus',
            'assignment_confidence': 'low', 'assignment_reason': 'higher_rank_placeholder_label',
        }
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


def parse_taxonkit_taxonomy_lineages(lineage_output: str, reformat_output: str) -> dict[int, dict[str, str]]:
    ranks: dict[str, tuple[str, str]] = {}
    for line in lineage_output.splitlines():
        parts = line.split('\t')
        if len(parts) >= 3 and parts[0].strip().isdigit():
            ranks[parts[0].strip()] = (parts[1].strip(), parts[2].strip())
    parsed: dict[int, dict[str, str]] = {}
    for line in reformat_output.splitlines():
        parts = line.split('\t')
        if len(parts) < 3 or not parts[0].strip().isdigit():
            continue
        tax_id_text = parts[0].strip()
        lineage_text, reported_rank = ranks.get(tax_id_text, ('', ''))
        domain_name = 'Bacteria' if 'Bacteria' in lineage_text.split(';') else ''
        parsed[int(tax_id_text)] = {
            'reported_rank': reported_rank,
            'domain_name': domain_name,
            'phylum_name': parts[4].strip() if len(parts) > 4 else '',
            'class_name': parts[5].strip() if len(parts) > 5 else '',
            'order_name': parts[6].strip() if len(parts) > 6 else '',
            'family_name': parts[7].strip() if len(parts) > 7 else '',
            'genus_name': parts[8].strip() if len(parts) > 8 else '',
            'species_name': parts[9].strip() if len(parts) > 9 else '',
            'lineage_text': lineage_text,
        }
    return parsed


def run_taxonkit_taxonomy_lineages(tax_ids: list[int]) -> dict[int, dict[str, str]]:
    unique_ids = sorted({int(tax_id) for tax_id in tax_ids if tax_id is not None})
    if not unique_ids:
        return {}
    input_text = '\n'.join(str(tax_id) for tax_id in unique_ids) + '\n'
    lineage_result = subprocess.run(
        ['taxonkit', 'lineage', '-r'], input=input_text, text=True,
        capture_output=True, check=False, timeout=600,
    )
    if lineage_result.returncode != 0:
        raise RuntimeError(f'TaxonKit lineage failed: {lineage_result.stderr[-1000:]}')
    reformat_result = subprocess.run(
        ['taxonkit', 'reformat', '-f', '{k}\t{p}\t{c}\t{o}\t{f}\t{g}\t{s}'],
        input=lineage_result.stdout, text=True, capture_output=True, check=False, timeout=600,
    )
    if reformat_result.returncode != 0:
        raise RuntimeError(f'TaxonKit reformat failed: {reformat_result.stderr[-1000:]}')
    return parse_taxonkit_taxonomy_lineages(lineage_result.stdout, reformat_result.stdout)


def materialize_taxonomy_lineage_from_inventory(snapshot_id: str) -> dict[str, Any]:
    """Populate rank-aware canonical lineage in staging without altering public release views."""
    bootstrap_schema()
    with connect() as connection:
        inventory = connection.execute(
            'SELECT status, root_unique_assemblies FROM bacterial_inventory_snapshot WHERE snapshot_id = %s',
            (snapshot_id,),
        ).fetchone()
        if inventory is None or str(inventory[0]) != 'completed':
            raise RuntimeError('Canonical inventory snapshot is not completed.')
        tax_ids = [int(row[0]) for row in connection.execute(
            """
            SELECT DISTINCT m.tax_id
            FROM bacterial_inventory_membership i
            JOIN assembly_master m ON m.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s AND m.tax_id IS NOT NULL
            ORDER BY m.tax_id
            """,
            (snapshot_id,),
        ).fetchall()]
    lineages = run_taxonkit_taxonomy_lineages(tax_ids)
    now = utc_now()
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO canonical_taxonomy_lineage_taxid (
                    tax_id, reported_rank, domain_name, phylum_name, class_name, order_name,
                    family_name, genus_name, species_name, lineage_text, taxonomy_source, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NCBI Taxonomy via TaxonKit', %s)
                ON CONFLICT (tax_id) DO UPDATE SET
                    reported_rank = EXCLUDED.reported_rank, domain_name = EXCLUDED.domain_name,
                    phylum_name = EXCLUDED.phylum_name, class_name = EXCLUDED.class_name,
                    order_name = EXCLUDED.order_name, family_name = EXCLUDED.family_name,
                    genus_name = EXCLUDED.genus_name, species_name = EXCLUDED.species_name,
                    lineage_text = EXCLUDED.lineage_text, taxonomy_source = EXCLUDED.taxonomy_source,
                    updated_at = EXCLUDED.updated_at
                """,
                [(
                    tax_id, row['reported_rank'], row['domain_name'], row['phylum_name'], row['class_name'],
                    row['order_name'], row['family_name'], row['genus_name'], row['species_name'],
                    row['lineage_text'], now,
                ) for tax_id, row in lineages.items()],
            )
        connection.execute('DELETE FROM assembly_taxonomy_lineage WHERE snapshot_id = %s', (snapshot_id,))
        connection.execute(
            """
            INSERT INTO assembly_taxonomy_lineage (
                snapshot_id, assembly_accession, tax_id, reported_name, reported_rank,
                domain_name, phylum_name, class_name, order_name, family_name, genus_name,
                species_name, taxonomy_source, resolution_status
            )
            SELECT i.snapshot_id, i.assembly_accession, m.tax_id,
                   COALESCE(NULLIF(m.organism_name, ''), NULLIF(s.standardized_payload->>'Organism Name', ''), ''),
                   CASE
                       WHEN l.tax_id IS NOT NULL THEN l.reported_rank
                       WHEN p.partition_type IN ('named_species', 'provisional_species') THEN 'species_label'
                       WHEN p.partition_type = 'genus_only' THEN 'genus_label'
                       ELSE ''
                   END,
                   COALESCE(NULLIF(l.domain_name, ''), 'Bacteria'),
                   COALESCE(l.phylum_name, ''), COALESCE(l.class_name, ''),
                   COALESCE(l.order_name, ''), COALESCE(l.family_name, ''),
                   CASE
                       WHEN l.tax_id IS NOT NULL THEN COALESCE(l.genus_name, '')
                       ELSE COALESCE(p.genus_name, '')
                   END,
                   CASE
                       WHEN l.tax_id IS NOT NULL THEN COALESCE(l.species_name, '')
                       WHEN p.partition_type IN ('named_species', 'provisional_species') THEN COALESCE(p.species_label, '')
                       ELSE ''
                   END,
                   CASE WHEN l.tax_id IS NOT NULL THEN 'NCBI Taxonomy via TaxonKit' ELSE 'organism label fallback' END,
                   CASE
                       WHEN l.tax_id IS NOT NULL AND NULLIF(l.genus_name, '') IS NOT NULL THEN 'ncbi_genus_or_species_lineage'
                       WHEN l.tax_id IS NOT NULL THEN 'ncbi_higher_rank_lineage'
                       WHEN NULLIF(p.genus_name, '') IS NOT NULL THEN 'label_derived_genus_or_species'
                       ELSE 'unresolved_below_domain'
                   END
            FROM bacterial_inventory_membership i
            JOIN assembly_master m ON m.assembly_accession = i.assembly_accession
            LEFT JOIN assembly_standardization s ON s.assembly_accession = i.assembly_accession
            LEFT JOIN taxon_partition_membership p
              ON p.snapshot_id = i.snapshot_id AND p.assembly_accession = i.assembly_accession
            LEFT JOIN canonical_taxonomy_lineage_taxid l ON l.tax_id = m.tax_id
            WHERE i.snapshot_id = %s
            """,
            (snapshot_id,),
        )
        counts = connection.execute(
            """
            SELECT COUNT(*), COUNT(tax_id),
                   COUNT(*) FILTER (WHERE phylum_name <> ''),
                   COUNT(*) FILTER (WHERE class_name <> ''),
                   COUNT(*) FILTER (WHERE order_name <> ''),
                   COUNT(*) FILTER (WHERE family_name <> ''),
                   COUNT(*) FILTER (WHERE genus_name <> ''),
                   COUNT(*) FILTER (WHERE species_name <> '')
            FROM assembly_taxonomy_lineage WHERE snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()
        distinct_counts = connection.execute(
            """
            SELECT COUNT(DISTINCT phylum_name) FILTER (WHERE phylum_name <> ''),
                   COUNT(DISTINCT class_name) FILTER (WHERE class_name <> ''),
                   COUNT(DISTINCT order_name) FILTER (WHERE order_name <> ''),
                   COUNT(DISTINCT family_name) FILTER (WHERE family_name <> ''),
                   COUNT(DISTINCT genus_name) FILTER (WHERE genus_name <> ''),
                   COUNT(DISTINCT species_name) FILTER (WHERE species_name <> '')
            FROM assembly_taxonomy_lineage WHERE snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchone()
        canonical_species_counts = connection.execute(
            """
            SELECT COUNT(DISTINCT species_name), COUNT(*)
            FROM assembly_taxonomy_lineage
            WHERE snapshot_id = %s
              AND species_name ~ '^[A-Z][A-Za-z0-9_-]+ [a-z][a-z0-9-]+$'
              AND lower(split_part(species_name, ' ', 2)) NOT IN (
                  'sp', 'sp.', 'spp', 'spp.', 'bacterium', 'archaeon', 'microorganism',
                  'metagenome', 'uncultured', 'unclassified', 'endosymbiont', 'symbiont'
              )
            """,
            (snapshot_id,),
        ).fetchone()
        status_counts = dict(connection.execute(
            'SELECT resolution_status, COUNT(*) FROM assembly_taxonomy_lineage WHERE snapshot_id = %s GROUP BY resolution_status',
            (snapshot_id,),
        ).fetchall())
        summary = {
        'snapshot_id': snapshot_id,
        'root_assemblies': int(counts[0] or 0),
        'assemblies_with_ncbi_taxid': int(counts[1] or 0),
        'taxids_requested': len(tax_ids),
        'taxids_resolved': len(lineages),
        'assembly_rank_coverage': {
            'phylum': int(counts[2] or 0), 'class': int(counts[3] or 0), 'order': int(counts[4] or 0),
            'family': int(counts[5] or 0), 'genus': int(counts[6] or 0), 'species': int(counts[7] or 0),
        },
        'distinct_rank_labels': {
            'phylum': int(distinct_counts[0] or 0), 'class': int(distinct_counts[1] or 0),
            'order': int(distinct_counts[2] or 0), 'family': int(distinct_counts[3] or 0),
            'genus': int(distinct_counts[4] or 0),
            'species': int(canonical_species_counts[0] or 0),
            'species_level_labels': int(distinct_counts[5] or 0),
        },
        'canonical_species_assembly_count': int(canonical_species_counts[1] or 0),
        'resolution_status_counts': {str(key): int(value) for key, value in status_counts.items()},
        'public_release_replaced': False,
        }
        connection.execute(
            """
            INSERT INTO canonical_taxonomy_lineage_snapshot (snapshot_id, generated_at, summary_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                generated_at = EXCLUDED.generated_at, summary_json = EXCLUDED.summary_json
            """,
            (snapshot_id, now, Jsonb(summary)),
        )
        connection.commit()
    return summary

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
                SELECT i.assembly_accession, COALESCE(
                           NULLIF(m.organism_name, ''),
                           NULLIF(s.standardized_payload->>'Organism Name', ''),
                           ''
                       ) AS organism_name
                FROM bacterial_inventory_membership i
                JOIN assembly_master m ON m.assembly_accession = i.assembly_accession
                LEFT JOIN assembly_standardization s ON s.assembly_accession = i.assembly_accession
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
    lineage_summary = materialize_taxonomy_lineage_from_inventory(snapshot_id)
    summary = reconcile_root_partitions(
        snapshot_id,
        dataset_version_id,
        release_views_materialized=release_views_materialized,
    )
    summary.update({
        'processed_assemblies': processed,
        'partition_counts': counts,
        'lineage_summary': lineage_summary,
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
