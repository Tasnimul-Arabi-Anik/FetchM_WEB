from __future__ import annotations

import argparse
import contextlib
import csv
import fcntl
import json
import logging
import os
import re
import secrets
import shutil
import signal
import sqlite3
import smtplib
import statistics
import subprocess
import time
import uuid
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from scipy.stats import pearsonr, spearmanr
from flask import Flask, abort, flash, g, redirect, render_template, request, send_from_directory, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
JOBS_DIR = DATA_DIR / "jobs"
SPECIES_DIR = DATA_DIR / "species"
METADATA_DIR = DATA_DIR / "metadata"
LOCKS_DIR = DATA_DIR / "locks"
DB_PATH = DATA_DIR / "fetchm_webapp.db"
ENV_FILE = BASE_DIR / ".env"
LOG_FILE_NAME = "job.log"
UPLOADS_DIR_NAME = "uploads"
OUTPUTS_DIR_NAME = "outputs"
from lib.fetchm_runtime.metadata import (
    add_geo_columns,
    extract_country,
    fetch_metadata,
    filter_data,
    load_data,
    save_clean_data,
    save_summary,
    standardize_date,
    standardize_host,
    standardize_location,
)
from lib.fetchm_runtime.sequence import DEFAULT_DOWNLOAD_WORKERS, run_sequence_downloads
RESET_TOKEN_TTL_MINUTES = 60
PUBLIC_ENDPOINTS = {"login", "register", "forgot_password", "reset_password", "static"}

def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


load_dotenv_file(ENV_FILE)

WORKER_POLL_INTERVAL = float(os.environ.get("FETCHM_WEBAPP_WORKER_POLL_INTERVAL", "2"))
WORKER_MODE = os.environ.get("FETCHM_WEBAPP_WORKER_MODE", "all").strip().lower()
WORKER_HEARTBEAT_SECONDS = max(1.0, float(os.environ.get("FETCHM_WEBAPP_WORKER_HEARTBEAT_SECONDS", "10")))
WORKER_HEARTBEAT_STALE_SECONDS = max(2.0, float(os.environ.get("FETCHM_WEBAPP_WORKER_HEARTBEAT_STALE_SECONDS", "30")))
SPECIES_REFRESH_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_SPECIES_REFRESH_HOURS", "24")))
SPECIES_MAX_AUTO_RETRIES = max(0, int(os.environ.get("FETCHM_WEBAPP_SPECIES_MAX_AUTO_RETRIES", "3")))
METADATA_REFRESH_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_METADATA_REFRESH_HOURS", "168")))
METADATA_MAX_AUTO_RETRIES = max(0, int(os.environ.get("FETCHM_WEBAPP_METADATA_MAX_AUTO_RETRIES", "3")))
METADATA_SLEEP_SECONDS = max(0.0, float(os.environ.get("FETCHM_WEBAPP_METADATA_SLEEP_SECONDS", "0.2")))
BIOSAMPLE_CACHE_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_BIOSAMPLE_CACHE_HOURS", "720")))
BIOSAMPLE_NEGATIVE_CACHE_HOURS = max(
    1, int(os.environ.get("FETCHM_WEBAPP_BIOSAMPLE_NEGATIVE_CACHE_HOURS", "168"))
)
SQLITE_VARIABLE_CHUNK_SIZE = 900
DISCOVERY_REFRESH_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_DISCOVERY_REFRESH_HOURS", "24")))
DISCOVERY_LIMIT_PER_SCOPE = os.environ.get("FETCHM_WEBAPP_DISCOVERY_LIMIT_PER_SCOPE", "100").strip() or "100"
DATASETS_BINARY = os.environ.get("FETCHM_WEBAPP_DATASETS_BIN", "datasets")
TAXON_RECENT_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_TAXON_RECENT_HOURS", "168")))
TAXON_VERY_OLD_HOURS = max(TAXON_RECENT_HOURS + 1, int(os.environ.get("FETCHM_WEBAPP_TAXON_VERY_OLD_HOURS", "720")))
SEQUENCE_DOWNLOAD_WORKERS = max(
    1, int(os.environ.get("FETCHM_WEBAPP_SEQUENCE_DOWNLOAD_WORKERS", "8"))
)
DISCOVERY_SCOPES = [
    item.strip()
    for item in os.environ.get("FETCHM_WEBAPP_DISCOVERY_SCOPES", "").split(",")
    if item.strip()
]
ADMIN_USERS = {
    item.strip().lower()
    for item in os.environ.get("FETCHM_WEBAPP_ADMIN_USERS", "").split(",")
    if item.strip()
}


MODES = {
    "metadata": {
        "label": "Metadata only",
        "input_extension": ".tsv",
        "input_help": "Use a managed species TSV generated and refreshed by the server.",
    },
    "run": {
        "label": "Full pipeline",
        "input_extension": ".tsv",
        "input_help": "Use a managed species TSV. This runs metadata generation and sequence download.",
    },
    "seq": {
        "label": "Sequence only",
        "input_extension": ".csv",
        "input_help": "Upload an existing ncbi_clean.csv file produced by fetchm metadata mode.",
    },
}

ASSEMBLY_SOURCES = {
    "all": {"label": "All assemblies", "datasets_value": None},
    "genbank": {"label": "GenBank only", "datasets_value": "GenBank"},
    "refseq": {"label": "RefSeq only", "datasets_value": "RefSeq"},
}

TAXON_RANKS = {
    "species": {"label": "Species"},
    "genus": {"label": "Genus"},
}

METADATA_SECTIONS = {
    "summary": {"label": "Summary"},
    "geography": {"label": "Geography"},
    "species_diversity": {"label": "Species Diversity"},
    "host": {"label": "Host"},
    "environment": {"label": "Environment"},
    "temporal": {"label": "Temporal Analysis"},
    "quality": {"label": "Genome Quality"},
    "genomic": {"label": "Genomic Features"},
    "explore": {"label": "Explore"},
}

SEQUENCE_FILTER_FIELDS = {
    "species_name": {"label": "Species", "column": "Organism Name", "group": "Species Diversity"},
    "country": {"label": "Country", "column": "Country", "group": "Geography"},
    "continent": {"label": "Continent", "column": "Continent", "group": "Geography"},
    "subcontinent": {"label": "Subcontinent", "column": "Subcontinent", "group": "Geography"},
    "host": {"label": "Host", "column": "Host", "group": "Host"},
    "host_disease": {"label": "Host disease", "column": "Host Disease", "group": "Host"},
    "isolation_source": {"label": "Isolation source", "column": "Isolation Source", "group": "Isolation and Environment"},
    "sample_type": {"label": "Sample type", "column": "Sample Type", "group": "Isolation and Environment"},
    "environment_broad": {
        "label": "Environment (broad scale)",
        "column": "Environment (Broad Scale)",
        "group": "Isolation and Environment",
    },
    "environment_local": {
        "label": "Environment (local scale)",
        "column": "Environment (Local Scale)",
        "group": "Isolation and Environment",
    },
    "environment_medium": {
        "label": "Environment medium",
        "column": "Environment Medium",
        "group": "Isolation and Environment",
    },
    "assembly_level": {"label": "Assembly level", "column": "Assembly Level", "group": "Genome Quality"},
}

SEQUENCE_FILTER_GROUPS = [
    {
        "key": "species_diversity",
        "label": "Species Diversity",
        "fields": ["species_name"],
    },
    {
        "key": "geography",
        "label": "Geography",
        "fields": ["country", "continent", "subcontinent"],
    },
    {
        "key": "host",
        "label": "Host",
        "fields": ["host", "host_disease"],
    },
    {
        "key": "isolation_environment",
        "label": "Isolation and Environment",
        "fields": [
            "isolation_source",
            "sample_type",
            "environment_broad",
            "environment_local",
            "environment_medium",
        ],
    },
    {
        "key": "genome_quality",
        "label": "Genome Quality",
        "fields": ["assembly_level"],
    },
]

SEQUENCE_PREVIEW_COLUMNS = [
    "Assembly Accession",
    "Organism Name",
    "Country",
    "Host",
    "Isolation Source",
    "Collection Date",
    "CheckM completeness",
    "Assembly Level",
    "Assembly Stats Number of Contigs",
]

DISCOVERY_POLICIES = {
    "paused": {"label": "Paused", "hours": None},
    "weekly": {"label": "Weekly", "hours": 168},
    "daily": {"label": "Daily", "hours": 24},
}

CATALOG_POLICIES = {
    "paused": {"label": "Paused", "hours": None},
    "daily": {"label": "Daily", "hours": 24},
    "weekly": {"label": "Weekly", "hours": 168},
    "half_monthly": {"label": "Half-monthly", "hours": 24 * 15},
    "monthly": {"label": "Monthly", "hours": 24 * 30},
    "yearly": {"label": "Yearly", "hours": 24 * 365},
}

METADATA_POLICIES = {
    "paused": {"label": "Paused", "hours": None},
    "half_monthly": {"label": "Half-monthly", "hours": 24 * 15},
    "weekly": {"label": "Weekly", "hours": 168},
    "monthly": {"label": "Monthly", "hours": 24 * 30},
    "yearly": {"label": "Yearly", "hours": 24 * 365},
    "daily": {"label": "Daily", "hours": 24},
}

SPECIES_TSV_COLUMNS = [
    "Assembly Accession",
    "Assembly Name",
    "Organism Name",
    "Assembly Level",
    "Assembly Status",
    "Assembly Release Date",
    "ANI Check status",
    "Annotation Name",
    "Assembly BioProject Accession",
    "Assembly BioSample Accession",
    "Organism Infraspecific Names Strain",
    "Assembly Stats Total Sequence Length",
    "Assembly Stats Total Ungapped Length",
    "Assembly Stats GC Percent",
    "Assembly Stats Number of Contigs",
    "Assembly Stats Number of Scaffolds",
    "Assembly Stats Contig N50",
    "Assembly Stats Scaffold N50",
    "Annotation Count Gene Total",
    "Annotation Count Gene Protein-coding",
    "Annotation Count Gene Pseudogene",
    "CheckM completeness",
    "CheckM contamination",
]


@dataclass
class JobRecord:
    id: str
    mode: str
    status: str
    created_at: str
    updated_at: str
    input_name: str
    input_path: str
    output_dir: str
    log_path: str
    command: list[str]
    owner_user_id: int | None = None
    owner_username: str | None = None
    pid: int | None = None
    return_code: int | None = None
    error: str | None = None
    filters: dict[str, Any] | None = None
    cancel_requested: bool = False


@dataclass
class SpeciesRecord:
    id: int
    species_name: str
    slug: str
    status: str
    created_at: str
    updated_at: str
    query_name: str
    taxon_rank: str = "species"
    claim_token: int = 0
    sync_attempt_count: int = 0
    sync_first_claimed_at: str | None = None
    claimed_by: str | None = None
    claimed_at: str | None = None
    taxon_id: int | None = None
    genome_count: int | None = None
    assembly_source: str = "all"
    tsv_path: str | None = None
    last_synced_at: str | None = None
    sync_error: str | None = None
    refresh_requested: bool = False
    metadata_status: str = "missing"
    metadata_path: str | None = None
    metadata_clean_path: str | None = None
    metadata_last_built_at: str | None = None
    metadata_error: str | None = None
    metadata_refresh_requested: bool = False
    metadata_claim_token: int = 0
    metadata_attempt_count: int = 0
    metadata_first_claimed_at: str | None = None
    metadata_claimed_by: str | None = None
    metadata_claimed_at: str | None = None
    metadata_source_taxon_id: int | None = None


@dataclass
class DiscoveryScopeRecord:
    id: int
    scope_value: str
    scope_label: str
    target_rank: str
    assembly_source: str
    status: str
    created_at: str
    updated_at: str
    is_internal: bool = False
    discovered_species_count: int = 0
    last_discovered_at: str | None = None
    last_error: str | None = None
    refresh_requested: bool = False


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)  # type: ignore[assignment]
app.config["SECRET_KEY"] = os.environ.get("FETCHM_WEBAPP_SECRET", "fetchm-dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("FETCHM_WEBAPP_SECURE_COOKIE") == "1"
app.config["MAIL_SERVER"] = os.environ.get("FETCHM_WEBAPP_MAIL_SERVER", "")
app.config["MAIL_PORT"] = int(os.environ.get("FETCHM_WEBAPP_MAIL_PORT", "587"))
app.config["MAIL_USERNAME"] = os.environ.get("FETCHM_WEBAPP_MAIL_USERNAME", "")
app.config["MAIL_PASSWORD"] = os.environ.get("FETCHM_WEBAPP_MAIL_PASSWORD", "")
app.config["MAIL_USE_TLS"] = os.environ.get("FETCHM_WEBAPP_MAIL_USE_TLS", "1") == "1"
app.config["MAIL_USE_SSL"] = os.environ.get("FETCHM_WEBAPP_MAIL_USE_SSL", "0") == "1"
app.config["MAIL_FROM"] = os.environ.get("FETCHM_WEBAPP_MAIL_FROM", "")
app.config["MAIL_NOTIFY_JOB_SUBMITTED"] = os.environ.get("FETCHM_WEBAPP_MAIL_NOTIFY_JOB_SUBMITTED", "1") == "1"
app.config["MAIL_NOTIFY_JOB_FINISHED"] = os.environ.get("FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FINISHED", "1") == "1"
app.config["MAIL_NOTIFY_JOB_FAILED"] = os.environ.get("FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FAILED", "1") == "1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value)


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    SPECIES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)


def get_sqlite_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = get_sqlite_connection()
    return g.db


@app.teardown_appcontext
def close_db(_: Any) -> None:
    connection = g.pop("db", None)
    if connection is not None:
        connection.close()


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            input_name TEXT NOT NULL,
            input_path TEXT NOT NULL,
            output_dir TEXT NOT NULL,
            log_path TEXT NOT NULL,
            command_json TEXT NOT NULL,
            owner_user_id INTEGER,
            owner_username TEXT,
            pid INTEGER,
            return_code INTEGER,
            error TEXT,
            filters_json TEXT,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            claimed_by TEXT,
            claimed_at TEXT,
            FOREIGN KEY (owner_user_id) REFERENCES users (id)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_owner_created ON jobs (owner_user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);

        CREATE TABLE IF NOT EXISTS species (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            taxon_rank TEXT NOT NULL DEFAULT 'species',
            claim_token INTEGER NOT NULL DEFAULT 0,
            sync_attempt_count INTEGER NOT NULL DEFAULT 0,
            sync_first_claimed_at TEXT,
            assembly_source TEXT NOT NULL DEFAULT 'all',
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            query_name TEXT NOT NULL,
            taxon_id INTEGER,
            genome_count INTEGER,
            tsv_path TEXT,
            last_synced_at TEXT,
            sync_error TEXT,
            refresh_requested INTEGER NOT NULL DEFAULT 1,
            claimed_by TEXT,
            claimed_at TEXT,
            metadata_status TEXT NOT NULL DEFAULT 'missing',
            metadata_path TEXT,
            metadata_clean_path TEXT,
            metadata_last_built_at TEXT,
            metadata_error TEXT,
            metadata_refresh_requested INTEGER NOT NULL DEFAULT 0,
            metadata_claim_token INTEGER NOT NULL DEFAULT 0,
            metadata_attempt_count INTEGER NOT NULL DEFAULT 0,
            metadata_first_claimed_at TEXT,
            metadata_claimed_by TEXT,
            metadata_claimed_at TEXT,
            metadata_source_taxon_id INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_species_status_updated ON species (status, updated_at);

        CREATE TABLE IF NOT EXISTS discovery_scopes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_value TEXT NOT NULL UNIQUE,
            scope_label TEXT NOT NULL,
            target_rank TEXT NOT NULL DEFAULT 'species',
            assembly_source TEXT NOT NULL DEFAULT 'all',
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_internal INTEGER NOT NULL DEFAULT 0,
            discovered_species_count INTEGER NOT NULL DEFAULT 0,
            last_discovered_at TEXT,
            last_error TEXT,
            refresh_requested INTEGER NOT NULL DEFAULT 1,
            claimed_by TEXT,
            claimed_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_scopes_status_updated
        ON discovery_scopes (status, updated_at);

        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS taxon_sync_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL,
            taxon_rank TEXT NOT NULL,
            species_name TEXT NOT NULL,
            sync_kind TEXT NOT NULL,
            synced_at TEXT NOT NULL,
            before_genome_count INTEGER,
            after_genome_count INTEGER,
            delta_genome_count INTEGER,
            FOREIGN KEY (species_id) REFERENCES species (id)
        );

        CREATE INDEX IF NOT EXISTS idx_taxon_sync_events_species_synced
        ON taxon_sync_events (species_id, synced_at DESC);

        CREATE INDEX IF NOT EXISTS idx_taxon_sync_events_synced
        ON taxon_sync_events (synced_at DESC);

        CREATE TABLE IF NOT EXISTS biosample_cache (
            biosample_id TEXT PRIMARY KEY,
            found INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_biosample_cache_fetched_at
        ON biosample_cache (fetched_at);

        CREATE TABLE IF NOT EXISTS assembly_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL,
            assembly_accession TEXT NOT NULL,
            assembly_name TEXT,
            organism_name TEXT,
            biosample_accession TEXT,
            row_json TEXT NOT NULL,
            refreshed_at TEXT NOT NULL,
            FOREIGN KEY (species_id) REFERENCES species (id),
            UNIQUE(species_id, assembly_accession)
        );

        CREATE INDEX IF NOT EXISTS idx_assembly_metadata_species
        ON assembly_metadata (species_id, assembly_accession);

        CREATE INDEX IF NOT EXISTS idx_assembly_metadata_biosample
        ON assembly_metadata (biosample_accession);

        CREATE TABLE IF NOT EXISTS problem_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT NOT NULL,
            email TEXT,
            taxon_id INTEGER,
            taxon_name TEXT,
            requested_action TEXT,
            message TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            admin_note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (taxon_id) REFERENCES species (id)
        );

        CREATE INDEX IF NOT EXISTS idx_problem_reports_status_created
        ON problem_reports (status, created_at DESC);
        """
    )
    db.commit()
    ensure_job_columns(db)
    ensure_species_columns(db)
    ensure_discovery_scope_columns(db)
    migrate_legacy_jobs(db)
    migrate_legacy_species(db)
    sync_discovery_scopes_from_env(db)
    ensure_default_settings(db)


def biosample_record_has_data(record: dict[str, Any]) -> bool:
    for value in record.values():
        if value is None or pd.isna(value):
            continue
        if str(value).strip():
            return True
    return False


def serialize_biosample_record(record: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in record.items():
        if value is None or pd.isna(value):
            payload[key] = None
        else:
            text = str(value).strip()
            payload[key] = text if text else None
    return payload


def deserialize_biosample_record(payload: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    for key, value in payload.items():
        record[key] = pd.NA if value in (None, "") else value
    return record


def biosample_cache_is_fresh(fetched_at: str, *, found: bool) -> bool:
    try:
        fetched = parse_utc(fetched_at)
    except ValueError:
        return False
    max_age_hours = BIOSAMPLE_CACHE_HOURS if found else BIOSAMPLE_NEGATIVE_CACHE_HOURS
    return fetched >= utc_now_dt() - timedelta(hours=max_age_hours)


def load_cached_biosample_records(biosample_ids: list[str]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    if not biosample_ids:
        return {}, []
    rows: list[sqlite3.Row] = []
    with get_sqlite_connection() as db:
        for start in range(0, len(biosample_ids), SQLITE_VARIABLE_CHUNK_SIZE):
            chunk = biosample_ids[start : start + SQLITE_VARIABLE_CHUNK_SIZE]
            placeholders = ", ".join("?" for _ in chunk)
            rows.extend(
                db.execute(
                    f"""
                    SELECT biosample_id, found, payload_json, fetched_at
                    FROM biosample_cache
                    WHERE biosample_id IN ({placeholders})
                    """,
                    tuple(chunk),
                ).fetchall()
            )

    cached: dict[str, dict[str, Any]] = {}
    stale_or_missing = set(biosample_ids)
    for row in rows:
        biosample_id = str(row["biosample_id"])
        found = bool(row["found"])
        fetched_at = str(row["fetched_at"])
        if not biosample_cache_is_fresh(fetched_at, found=found):
            continue
        payload = json.loads(str(row["payload_json"]))
        cached[biosample_id] = deserialize_biosample_record(payload)
        stale_or_missing.discard(biosample_id)
    return cached, list(stale_or_missing)


def save_biosample_cache_records(records: dict[str, tuple[dict[str, Any], bool]]) -> None:
    if not records:
        return
    now = utc_now()
    rows = [
        (
            biosample_id,
            int(found),
            json.dumps(serialize_biosample_record(record), sort_keys=True),
            now,
        )
        for biosample_id, (record, found) in records.items()
    ]
    with get_sqlite_connection() as db:
        db.executemany(
            """
            INSERT INTO biosample_cache (biosample_id, found, payload_json, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(biosample_id) DO UPDATE SET
                found = excluded.found,
                payload_json = excluded.payload_json,
                fetched_at = excluded.fetched_at
            """,
            rows,
        )
        db.commit()


def record_taxon_sync_event(
    species: SpeciesRecord,
    *,
    sync_kind: str,
    before_genome_count: int | None,
    after_genome_count: int | None,
    synced_at: str,
) -> None:
    delta = None
    if before_genome_count is not None and after_genome_count is not None:
        delta = after_genome_count - before_genome_count
    elif before_genome_count is None and after_genome_count is not None:
        delta = after_genome_count
    with get_sqlite_connection() as db:
        db.execute(
            """
            INSERT INTO taxon_sync_events (
                species_id, taxon_rank, species_name, sync_kind, synced_at,
                before_genome_count, after_genome_count, delta_genome_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                species.id,
                species.taxon_rank,
                species.species_name,
                sync_kind,
                synced_at,
                before_genome_count,
                after_genome_count,
                delta,
            ),
        )
        db.commit()


def fetch_biosample_metadata_records(biosample_ids: list[str]) -> dict[str, dict[str, Any]]:
    cached, to_fetch = load_cached_biosample_records(biosample_ids)
    fetched: dict[str, tuple[dict[str, Any], bool]] = {}
    for biosample_id in to_fetch:
        record = fetch_metadata_record(biosample_id, METADATA_SLEEP_SECONDS)
        fetched[biosample_id] = (record, biosample_record_has_data(record))
    save_biosample_cache_records(fetched)
    records = dict(cached)
    for biosample_id, (record, _) in fetched.items():
        records[biosample_id] = record
    return records


def fetch_metadata_record(biosample_id: str, sleep_time: float) -> dict[str, Any]:
    del sleep_time
    metadata_tuple, status_info = fetch_metadata(
        biosample_id,
        api_key=os.environ.get("NCBI_API_KEY"),
        email=None,
        persistent_cache=None,
        rate_limiter=None,
    )
    isolation_source, collection_date, geo_location, host = metadata_tuple
    record: dict[str, Any] = {
        "BioSample Accession": biosample_id,
        "Isolation Source": isolation_source,
        "Collection Date": collection_date,
        "Geographic Location": geo_location,
        "Host": host,
    }
    reason = status_info.get("reason")
    if reason:
        record["BioSample Fetch Reason"] = reason
    return record


def normalize_json_scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return value


def normalize_metadata_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized = normalize_json_scalar(value)
        if normalized is None:
            payload[str(key)] = None
        elif isinstance(normalized, (str, int, float, bool)):
            payload[str(key)] = normalized
        else:
            payload[str(key)] = str(normalized)
    return payload


def metadata_row_accession(row: dict[str, Any]) -> str:
    return str(row.get("Assembly Accession") or "").strip()


def metadata_row_biosample_accession(row: dict[str, Any]) -> str | None:
    value = row.get("Assembly BioSample Accession")
    text = str(value).strip() if value is not None else ""
    return text or None


def load_taxon_metadata_rows(species_id: int) -> dict[str, dict[str, Any]]:
    with get_sqlite_connection() as db:
        rows = db.execute(
            """
            SELECT assembly_accession, row_json
            FROM assembly_metadata
            WHERE species_id = ?
            """,
            (species_id,),
        ).fetchall()
    payloads: dict[str, dict[str, Any]] = {}
    for row in rows:
        accession = str(row["assembly_accession"])
        payloads[accession] = json.loads(str(row["row_json"]))
    return payloads


def save_taxon_metadata_rows(
    species_id: int,
    rows: list[dict[str, Any]],
    *,
    refreshed_at: str,
) -> None:
    if not rows:
        return
    serialized_rows = []
    accessions: list[str] = []
    for row in rows:
        accession = metadata_row_accession(row)
        if not accession:
            continue
        accessions.append(accession)
        serialized_rows.append(
            (
                species_id,
                accession,
                str(row.get("Assembly Name") or "").strip() or None,
                str(row.get("Organism Name") or "").strip() or None,
                metadata_row_biosample_accession(row),
                json.dumps(normalize_metadata_row_payload(row), sort_keys=True),
                refreshed_at,
            )
        )
    with get_sqlite_connection() as db:
        if accessions:
            placeholders = ", ".join("?" for _ in accessions)
            db.execute(
                f"""
                DELETE FROM assembly_metadata
                WHERE species_id = ?
                  AND assembly_accession NOT IN ({placeholders})
                """,
                (species_id, *accessions),
            )
        else:
            db.execute("DELETE FROM assembly_metadata WHERE species_id = ?", (species_id,))
        db.executemany(
            """
            INSERT INTO assembly_metadata (
                species_id, assembly_accession, assembly_name, organism_name,
                biosample_accession, row_json, refreshed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(species_id, assembly_accession) DO UPDATE SET
                assembly_name = excluded.assembly_name,
                organism_name = excluded.organism_name,
                biosample_accession = excluded.biosample_accession,
                row_json = excluded.row_json,
                refreshed_at = excluded.refreshed_at
            """,
            serialized_rows,
        )
        db.commit()


def merge_tsv_record_with_stored_metadata(
    tsv_row: dict[str, Any],
    stored_row: dict[str, Any],
) -> dict[str, Any]:
    merged = normalize_metadata_row_payload(tsv_row)
    for key, value in stored_row.items():
        if key not in merged or merged[key] in (None, ""):
            merged[key] = value
            continue
        if key not in tsv_row:
            merged[key] = value
    return merged


def enrich_tsv_row_with_biosample_metadata(
    tsv_row: dict[str, Any],
    biosample_record: dict[str, Any] | None,
) -> dict[str, Any]:
    enriched = normalize_metadata_row_payload(tsv_row)
    record = biosample_record or {}
    for key, value in record.items():
        if key not in enriched or enriched[key] in (None, ""):
            enriched[key] = normalize_json_scalar(value)
    if "Isolation Source" not in enriched:
        enriched["Isolation Source"] = None
    if "Collection Date" not in enriched:
        enriched["Collection Date"] = None
    if "Geographic Location" not in enriched:
        enriched["Geographic Location"] = None
    if "Host" not in enriched:
        enriched["Host"] = None
    enriched["Collection Date"] = standardize_date(enriched.get("Collection Date"))
    enriched["Geographic Location"] = standardize_location(enriched.get("Geographic Location"))
    enriched["Host"] = standardize_host(enriched.get("Host"))
    return enriched


def ensure_job_columns(db: sqlite3.Connection) -> None:
    columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
    additions = {
        "cancel_requested": "INTEGER NOT NULL DEFAULT 0",
        "claimed_by": "TEXT",
        "claimed_at": "TEXT",
        "error": "TEXT",
        "filters_json": "TEXT",
    }
    for column, definition in additions.items():
        if column not in columns:
            db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
    db.commit()


def ensure_species_columns(db: sqlite3.Connection) -> None:
    columns = {row["name"] for row in db.execute("PRAGMA table_info(species)").fetchall()}
    additions = {
        "taxon_rank": "TEXT NOT NULL DEFAULT 'species'",
        "claim_token": "INTEGER NOT NULL DEFAULT 0",
        "sync_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "sync_first_claimed_at": "TEXT",
        "assembly_source": "TEXT NOT NULL DEFAULT 'all'",
        "query_name": "TEXT NOT NULL DEFAULT ''",
        "taxon_id": "INTEGER",
        "genome_count": "INTEGER",
        "tsv_path": "TEXT",
        "last_synced_at": "TEXT",
        "sync_error": "TEXT",
        "refresh_requested": "INTEGER NOT NULL DEFAULT 1",
        "claimed_by": "TEXT",
        "claimed_at": "TEXT",
        "metadata_status": "TEXT NOT NULL DEFAULT 'missing'",
        "metadata_path": "TEXT",
        "metadata_clean_path": "TEXT",
        "metadata_last_built_at": "TEXT",
        "metadata_error": "TEXT",
        "metadata_refresh_requested": "INTEGER NOT NULL DEFAULT 0",
        "metadata_claim_token": "INTEGER NOT NULL DEFAULT 0",
        "metadata_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "metadata_first_claimed_at": "TEXT",
        "metadata_claimed_by": "TEXT",
        "metadata_claimed_at": "TEXT",
        "metadata_source_taxon_id": "INTEGER",
    }
    for column, definition in additions.items():
        if column not in columns:
            db.execute(f"ALTER TABLE species ADD COLUMN {column} {definition}")
    db.commit()


def ensure_discovery_scope_columns(db: sqlite3.Connection) -> None:
    columns = {row["name"] for row in db.execute("PRAGMA table_info(discovery_scopes)").fetchall()}
    additions = {
        "target_rank": "TEXT NOT NULL DEFAULT 'species'",
        "assembly_source": "TEXT NOT NULL DEFAULT 'all'",
        "scope_label": "TEXT NOT NULL DEFAULT ''",
        "is_internal": "INTEGER NOT NULL DEFAULT 0",
        "discovered_species_count": "INTEGER NOT NULL DEFAULT 0",
        "last_discovered_at": "TEXT",
        "last_error": "TEXT",
        "refresh_requested": "INTEGER NOT NULL DEFAULT 1",
        "claimed_by": "TEXT",
        "claimed_at": "TEXT",
    }
    for column, definition in additions.items():
        if column not in columns:
            db.execute(f"ALTER TABLE discovery_scopes ADD COLUMN {column} {definition}")
    db.commit()


def migrate_legacy_jobs(db: sqlite3.Connection) -> None:
    known_ids = {row["id"] for row in db.execute("SELECT id FROM jobs").fetchall()}
    for path in JOBS_DIR.glob("*/job.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            data.setdefault("owner_user_id", None)
            data.setdefault("owner_username", None)
            data.setdefault("filters", None)
            data.setdefault("error", None)
            data.setdefault("cancel_requested", False)
            job = JobRecord(**data)
        except Exception:
            continue
        if job.id in known_ids:
            continue
        save_job(job, db)
        known_ids.add(job.id)


def migrate_legacy_species(db: sqlite3.Connection) -> None:
    known_names = {
        str(row["species_name"]).lower()
        for row in db.execute("SELECT species_name FROM species").fetchall()
    }
    for path in JOBS_DIR.glob("*/outputs/*/metadata_output/ncbi_dataset_updated.tsv"):
        species_name = ""
        genome_count = 0
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                for row in reader:
                    genome_count += 1
                    if not species_name:
                        species_name = normalize_species_name(row.get("Organism Name", ""))
                if not species_name:
                    continue
        except Exception:
            continue

        key = species_name.lower()
        if key in known_names:
            continue

        slug = species_slug(species_name)
        managed_path = species_tsv_path(slug)
        managed_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, managed_path)
        saved = save_species(
            SpeciesRecord(
                id=0,
                species_name=species_name,
                slug=slug,
                taxon_rank="species",
                claim_token=0,
                sync_attempt_count=0,
                sync_first_claimed_at=None,
                assembly_source="all",
                status="ready",
                created_at=utc_now(),
                updated_at=utc_now(),
                query_name=species_name,
                genome_count=genome_count or None,
                tsv_path=str(managed_path),
                last_synced_at=utc_now(),
                refresh_requested=False,
                metadata_status="missing",
            ),
            db,
        )
        known_names.add(saved.species_name.lower())


def normalize_discovery_policy(value: str | None) -> str:
    candidate = (value or "").strip().lower()
    if candidate in DISCOVERY_POLICIES:
        return candidate
    return "daily" if DISCOVERY_REFRESH_HOURS <= 24 else "weekly"


def normalize_catalog_policy(value: str | None) -> str:
    candidate = (value or "").strip().lower()
    if candidate in CATALOG_POLICIES:
        return candidate
    return "weekly" if SPECIES_REFRESH_HOURS > 24 else "daily"


def normalize_metadata_policy(value: str | None) -> str:
    candidate = (value or "").strip().lower()
    if candidate in METADATA_POLICIES:
        return candidate
    return "weekly"


def ensure_default_settings(db: sqlite3.Connection) -> None:
    policy = normalize_discovery_policy(None)
    catalog_policy = normalize_catalog_policy(None)
    metadata_policy = normalize_metadata_policy(None)
    metadata_refresh_policy = "paused"
    now = utc_now()
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("discovery_policy", policy, now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("metadata_policy", metadata_policy, now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("catalog_build_policy", catalog_policy, now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("catalog_refresh_policy", "paused", now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("metadata_build_policy", metadata_policy, now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("metadata_refresh_policy", metadata_refresh_policy, now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("system_temp_alert_enabled", "0", now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("system_temp_alert_email", "", now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("system_temp_alert_threshold_c", "80", now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("system_temp_alert_cooldown_minutes", "60", now),
    )
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO NOTHING
        """,
        ("system_temp_alert_last_sent_at", "", now),
    )
    db.commit()


def parse_multi_value(raw_value: str) -> list[str]:
    values = []
    for item in raw_value.replace("\n", ",").split(","):
        cleaned = item.strip()
        if cleaned:
            values.append(cleaned)
    return values


def allowed_extension(filename: str, expected_extension: str) -> bool:
    return Path(filename).suffix.lower() == expected_extension


def normalize_username(value: str) -> str:
    return value.strip().lower()


def normalize_email(value: str) -> str:
    return value.strip().lower()


def normalize_species_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def normalize_taxon_rank(value: str | None) -> str:
    candidate = (value or "species").strip().lower()
    return candidate if candidate in TAXON_RANKS else "species"


def normalize_discovery_limit(value: str) -> str:
    candidate = (value or "100").strip().lower()
    if candidate == "all":
        return "all"
    try:
        return str(max(1, int(candidate)))
    except ValueError:
        return "100"


def get_setting(key: str, default: str | None = None, db: sqlite3.Connection | None = None) -> str | None:
    connection = db or get_db()
    row = connection.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row is not None else default


def set_setting(key: str, value: str, db: sqlite3.Connection | None = None) -> None:
    connection = db or get_db()
    connection.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, value, utc_now()),
    )
    connection.commit()


def get_discovery_policy(db: sqlite3.Connection | None = None) -> str:
    return normalize_discovery_policy(get_setting("discovery_policy", None, db))


def discovery_refresh_hours(db: sqlite3.Connection | None = None) -> int | None:
    policy = get_discovery_policy(db)
    return DISCOVERY_POLICIES[policy]["hours"]


def get_catalog_build_policy(db: sqlite3.Connection | None = None) -> str:
    return normalize_catalog_policy(get_setting("catalog_build_policy", None, db))


def catalog_build_hours(db: sqlite3.Connection | None = None) -> int | None:
    policy = get_catalog_build_policy(db)
    return CATALOG_POLICIES[policy]["hours"]


def get_catalog_refresh_policy(db: sqlite3.Connection | None = None) -> str:
    return normalize_catalog_policy(get_setting("catalog_refresh_policy", None, db))


def catalog_refresh_hours(db: sqlite3.Connection | None = None) -> int | None:
    policy = get_catalog_refresh_policy(db)
    return CATALOG_POLICIES[policy]["hours"]


def get_metadata_build_policy(db: sqlite3.Connection | None = None) -> str:
    stored = get_setting("metadata_build_policy", None, db)
    if stored is None:
        stored = get_setting("metadata_policy", None, db)
    return normalize_metadata_policy(stored)


def metadata_build_hours(db: sqlite3.Connection | None = None) -> int | None:
    policy = get_metadata_build_policy(db)
    return METADATA_POLICIES[policy]["hours"]


def get_metadata_refresh_policy(db: sqlite3.Connection | None = None) -> str:
    stored = get_setting("metadata_refresh_policy", None, db)
    if stored is None:
        return "paused"
    return normalize_metadata_policy(stored)


def metadata_refresh_hours(db: sqlite3.Connection | None = None) -> int | None:
    policy = get_metadata_refresh_policy(db)
    return METADATA_POLICIES[policy]["hours"]


def get_system_temp_alert_enabled(db: sqlite3.Connection | None = None) -> bool:
    return (get_setting("system_temp_alert_enabled", "0", db) or "0").strip() == "1"


def get_system_temp_alert_email(db: sqlite3.Connection | None = None) -> str:
    return (get_setting("system_temp_alert_email", "", db) or "").strip()


def get_system_temp_alert_threshold_c(db: sqlite3.Connection | None = None) -> float:
    raw = (get_setting("system_temp_alert_threshold_c", "80", db) or "80").strip()
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 80.0


def get_system_temp_alert_cooldown_minutes(db: sqlite3.Connection | None = None) -> int:
    raw = (get_setting("system_temp_alert_cooldown_minutes", "60", db) or "60").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 60


def get_system_temp_alert_last_sent_at(db: sqlite3.Connection | None = None) -> str | None:
    value = (get_setting("system_temp_alert_last_sent_at", "", db) or "").strip()
    return value or None


def normalize_assembly_source(value: str | None) -> str:
    candidate = (value or "all").strip().lower()
    return candidate if candidate in ASSEMBLY_SOURCES else "all"


DEFAULT_ASSEMBLY_SOURCE = normalize_assembly_source(os.environ.get("FETCHM_WEBAPP_DEFAULT_ASSEMBLY_SOURCE", "all"))
DISCOVERY_LIMIT_PER_SCOPE = normalize_discovery_limit(DISCOVERY_LIMIT_PER_SCOPE)


def make_discovery_scope_key(scope_value: str, target_rank: str) -> str:
    return f"{scope_value.strip()}|{normalize_taxon_rank(target_rank)}"


def make_internal_discovery_scope_key(scope_value: str, target_rank: str) -> str:
    return f"internal:{scope_value.strip()}|{normalize_taxon_rank(target_rank)}"


def discovery_scope_query_value(scope: DiscoveryScopeRecord) -> str:
    raw = scope.scope_value
    if raw.startswith("internal:"):
        raw = raw[len("internal:") :]
    return raw.split("|", 1)[0]


def is_bacterial_root_scope(scope: DiscoveryScopeRecord) -> bool:
    return discovery_scope_query_value(scope) == "2"


def should_partition_discovery_scope(scope: DiscoveryScopeRecord) -> bool:
    return scope.target_rank == "species" and is_bacterial_root_scope(scope)


def species_slug(value: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", normalize_species_name(value).lower()).strip("-")
    return base or f"species-{uuid.uuid4().hex[:8]}"


def species_dir(slug: str) -> Path:
    return SPECIES_DIR / slug


def species_tsv_path(slug: str) -> Path:
    return species_dir(slug) / "ncbi_dataset.tsv"


def metadata_taxon_dir(slug: str) -> Path:
    return METADATA_DIR / slug


def metadata_lock_path(species_id: int) -> Path:
    lock_dir = LOCKS_DIR / "metadata"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / f"{species_id}.lock"


def startup_recovery_lock_path() -> Path:
    lock_dir = LOCKS_DIR / "startup"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / "worker-recovery.lock"


def worker_heartbeat_dir() -> Path:
    heartbeat_dir = LOCKS_DIR / "workers"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)
    return heartbeat_dir


def worker_heartbeat_path(worker_name: str) -> Path:
    return worker_heartbeat_dir() / f"{worker_name.replace(':', '_')}.heartbeat"


def touch_worker_heartbeat(worker_name: str) -> None:
    path = worker_heartbeat_path(worker_name)
    now = time.time()
    path.touch(exist_ok=True)
    os.utime(path, (now, now))


def has_other_live_worker_heartbeat(worker_name: str) -> bool:
    own_name = worker_heartbeat_path(worker_name).name
    now = time.time()
    for path in worker_heartbeat_dir().glob("*.heartbeat"):
        if path.name == own_name:
            continue
        try:
            age = now - path.stat().st_mtime
        except FileNotFoundError:
            continue
        if age <= WORKER_HEARTBEAT_STALE_SECONDS:
            return True
    return False


def worker_heartbeat_is_live(worker_name: str) -> bool:
    path = worker_heartbeat_path(worker_name)
    try:
        age = time.time() - path.stat().st_mtime
    except FileNotFoundError:
        return False
    return age <= WORKER_HEARTBEAT_STALE_SECONDS


def acquire_startup_recovery_lock() -> Any | None:
    handle = startup_recovery_lock_path().open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def acquire_metadata_lock(species_id: int) -> Any | None:
    handle = metadata_lock_path(species_id).open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def metadata_lock_is_available(species_id: int) -> bool:
    handle = acquire_metadata_lock(species_id)
    if handle is None:
        return False
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()
    return True


def metadata_root_dir(slug: str) -> Path:
    return metadata_taxon_dir(slug) / "build"


def metadata_dataset_path(slug: str) -> Path:
    return metadata_taxon_dir(slug) / "metadata_output" / "ncbi_dataset_updated.tsv"


def metadata_clean_path(slug: str) -> Path:
    return metadata_taxon_dir(slug) / "metadata_output" / "ncbi_clean.csv"


def is_admin_user(user: sqlite3.Row | None) -> bool:
    if user is None:
        return False
    return str(user["username"]).lower() in ADMIN_USERS


def taxon_age_hours(species: SpeciesRecord) -> float | None:
    if not species.last_synced_at:
        return None
    try:
        age = utc_now_dt() - parse_utc(species.last_synced_at)
    except ValueError:
        return None
    return max(age.total_seconds() / 3600, 0)


def is_taxon_recent_enough(species: SpeciesRecord) -> bool:
    age_hours = taxon_age_hours(species)
    if age_hours is None:
        return False
    return age_hours <= TAXON_RECENT_HOURS


def is_taxon_very_old(species: SpeciesRecord) -> bool:
    age_hours = taxon_age_hours(species)
    if age_hours is None:
        return True
    return age_hours >= TAXON_VERY_OLD_HOURS


def taxon_freshness_label(species: SpeciesRecord) -> str:
    age_hours = taxon_age_hours(species)
    if age_hours is None:
        return "never synced"
    if age_hours < 24:
        return f"{int(age_hours)} hours old"
    return f"{int(age_hours // 24)} days old"


def job_requires_taxon_refresh(job: JobRecord) -> bool:
    return bool(job.filters and job.filters.get("input_source") == "taxon" and job.filters.get("refresh_before_run"))


def job_taxon_wait_state(job: JobRecord, db: sqlite3.Connection | None = None) -> tuple[str, SpeciesRecord | None]:
    if not job_requires_taxon_refresh(job):
        return "not_required", None
    if not job.filters:
        return "not_required", None
    taxon_id = job.filters.get("taxon_id")
    if not isinstance(taxon_id, int):
        try:
            taxon_id = int(taxon_id)
        except (TypeError, ValueError):
            return "not_required", None
    species = get_species_by_id(taxon_id, db)
    if species is None:
        return "missing", None
    try:
        created_at = parse_utc(job.created_at)
    except ValueError:
        created_at = utc_now_dt()
    try:
        last_synced_at = parse_utc(species.last_synced_at) if species.last_synced_at else None
    except ValueError:
        last_synced_at = None
    if species.status == "failed" and (last_synced_at is None or last_synced_at < created_at):
        return "failed", species
    if species.status != "ready":
        return "waiting", species
    if species.claimed_at and species.tsv_path:
        return "waiting", species
    if species.refresh_requested and not species.tsv_path:
        return "waiting", species
    if last_synced_at is None:
        return "waiting", species
    if last_synced_at < created_at:
        return "waiting", species
    return "ready", species


def get_user_by_id(user_id: int) -> sqlite3.Row | None:
    return get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def get_user_by_username(username: str) -> sqlite3.Row | None:
    return get_db().execute("SELECT * FROM users WHERE username = ?", (normalize_username(username),)).fetchone()


def get_user_by_email(email: str) -> sqlite3.Row | None:
    return get_db().execute("SELECT * FROM users WHERE email = ?", (normalize_email(email),)).fetchone()


def get_user_email_by_id(user_id: int, db: sqlite3.Connection | None = None) -> str | None:
    connection = db or get_db()
    row = connection.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
    return str(row["email"]) if row is not None else None


def create_user(username: str, email: str, password: str) -> sqlite3.Row:
    db = get_db()
    db.execute(
        """
        INSERT INTO users (username, email, password_hash, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (normalize_username(username), normalize_email(email), generate_password_hash(password), utc_now()),
    )
    db.commit()
    user = get_user_by_username(username)
    assert user is not None
    return user


def update_user_password(user_id: int, password: str) -> None:
    db = get_db()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (generate_password_hash(password), user_id))
    db.commit()


def current_user() -> sqlite3.Row | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(int(user_id))


def login_user(user: sqlite3.Row) -> None:
    session.clear()
    session["user_id"] = int(user["id"])
    session["username"] = str(user["username"])


def logout_user() -> None:
    session.clear()


def create_reset_token(user_id: int) -> str:
    db = get_db()
    db.execute(
        "UPDATE reset_tokens SET used_at = ? WHERE user_id = ? AND used_at IS NULL",
        (utc_now(), user_id),
    )
    token = secrets.token_urlsafe(32)
    created_at = datetime.now(timezone.utc)
    expires_at = created_at + timedelta(minutes=RESET_TOKEN_TTL_MINUTES)
    db.execute(
        """
        INSERT INTO reset_tokens (user_id, token, created_at, expires_at, used_at)
        VALUES (?, ?, ?, ?, NULL)
        """,
        (user_id, token, created_at.isoformat(), expires_at.isoformat()),
    )
    db.commit()
    return token


def get_valid_reset_token(token: str) -> sqlite3.Row | None:
    row = get_db().execute(
        """
        SELECT rt.*, u.username
        FROM reset_tokens rt
        JOIN users u ON u.id = rt.user_id
        WHERE rt.token = ?
        """,
        (token,),
    ).fetchone()
    if row is None or row["used_at"] is not None:
        return None
    if parse_utc(row["expires_at"]) < datetime.now(timezone.utc):
        return None
    return row


def mark_reset_token_used(token: str) -> None:
    db = get_db()
    db.execute("UPDATE reset_tokens SET used_at = ? WHERE token = ?", (utc_now(), token))
    db.commit()


def validate_passwords(password: str, confirm: str) -> str | None:
    if len(password) < 8:
        return "Password must be at least 8 characters."
    if password != confirm:
        return "Passwords do not match."
    return None


def mail_is_configured() -> bool:
    return bool(app.config["MAIL_SERVER"] and app.config["MAIL_FROM"])


def send_email(recipient: str, subject: str, lines: list[str]) -> None:
    if not mail_is_configured():
        raise RuntimeError("SMTP is not configured.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = app.config["MAIL_FROM"]
    message["To"] = recipient
    message.set_content("\n".join(lines))

    smtp_class = smtplib.SMTP_SSL if app.config["MAIL_USE_SSL"] else smtplib.SMTP
    with smtp_class(app.config["MAIL_SERVER"], app.config["MAIL_PORT"], timeout=30) as server:
        if not app.config["MAIL_USE_SSL"] and app.config["MAIL_USE_TLS"]:
            server.starttls()
        if app.config["MAIL_USERNAME"]:
            server.login(app.config["MAIL_USERNAME"], app.config["MAIL_PASSWORD"])
        server.send_message(message)


def send_reset_email(recipient: str, username: str, reset_url: str) -> None:
    send_email(
        recipient,
        "fetchM Web password reset",
        [
            f"Hello {username},",
            "",
            "A password reset was requested for your fetchM Web account.",
            "Use the link below to set a new password:",
            reset_url,
            "",
            f"This link expires in {RESET_TOKEN_TTL_MINUTES} minutes.",
            "If you did not request this reset, you can ignore this email.",
        ],
    )


def send_job_notification_email(job: JobRecord, recipient: str, event: str) -> None:
    if event == "submitted" and not app.config["MAIL_NOTIFY_JOB_SUBMITTED"]:
        return
    if event == "finished" and not app.config["MAIL_NOTIFY_JOB_FINISHED"]:
        return
    if event == "failed" and not app.config["MAIL_NOTIFY_JOB_FAILED"]:
        return

    status_label = {
        "submitted": "submitted",
        "finished": "completed",
        "failed": "failed",
    }[event]
    subject = f"fetchM job {status_label}: {job.id}"
    lines = [
        f"Hello {job.owner_username or 'fetchM user'},",
        "",
        f"Job {job.id} has been {status_label}.",
        f"Mode: {job.mode}",
        f"Input: {job.input_name}",
        f"Status: {job.status}",
    ]
    if job.return_code is not None:
        lines.append(f"Return code: {job.return_code}")
    lines.extend(
        [
            "",
            f"Created: {job.created_at}",
            f"Updated: {job.updated_at}",
        ]
    )
    if event == "submitted":
        lines.extend(["", "Your job is queued and will be picked up by the background worker shortly."])
    elif event == "finished":
        lines.extend(["", "Your outputs are ready for download from the fetchM web app."])
    elif event == "failed":
        lines.extend(["", "Check the job log in the fetchM web app for the failure details."])

    send_email(recipient, subject, lines)


def job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def row_to_job(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        id=str(row["id"]),
        mode=str(row["mode"]),
        status=str(row["status"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        input_name=str(row["input_name"]),
        input_path=str(row["input_path"]),
        output_dir=str(row["output_dir"]),
        log_path=str(row["log_path"]),
        command=json.loads(row["command_json"]),
        owner_user_id=row["owner_user_id"],
        owner_username=row["owner_username"],
        pid=row["pid"],
        return_code=row["return_code"],
        error=row["error"],
        filters=json.loads(row["filters_json"]) if row["filters_json"] else None,
        cancel_requested=bool(row["cancel_requested"]),
    )


def row_to_species(row: sqlite3.Row) -> SpeciesRecord:
    return SpeciesRecord(
        id=int(row["id"]),
        species_name=str(row["species_name"]),
        slug=str(row["slug"]),
        taxon_rank=normalize_taxon_rank(row["taxon_rank"]),
        claim_token=int(row["claim_token"] or 0),
        sync_attempt_count=int(row["sync_attempt_count"] or 0),
        sync_first_claimed_at=str(row["sync_first_claimed_at"]) if row["sync_first_claimed_at"] else None,
        claimed_by=str(row["claimed_by"]) if row["claimed_by"] else None,
        claimed_at=str(row["claimed_at"]) if row["claimed_at"] else None,
        assembly_source=normalize_assembly_source(row["assembly_source"]),
        status=str(row["status"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        query_name=str(row["query_name"]),
        taxon_id=row["taxon_id"],
        genome_count=row["genome_count"],
        tsv_path=str(row["tsv_path"]) if row["tsv_path"] else None,
        last_synced_at=str(row["last_synced_at"]) if row["last_synced_at"] else None,
        sync_error=row["sync_error"],
        refresh_requested=bool(row["refresh_requested"]),
        metadata_status=str(row["metadata_status"] or "missing"),
        metadata_path=str(row["metadata_path"]) if row["metadata_path"] else None,
        metadata_clean_path=str(row["metadata_clean_path"]) if row["metadata_clean_path"] else None,
        metadata_last_built_at=str(row["metadata_last_built_at"]) if row["metadata_last_built_at"] else None,
        metadata_error=str(row["metadata_error"]) if row["metadata_error"] else None,
        metadata_refresh_requested=bool(row["metadata_refresh_requested"]),
        metadata_claim_token=int(row["metadata_claim_token"] or 0),
        metadata_attempt_count=int(row["metadata_attempt_count"] or 0),
        metadata_first_claimed_at=str(row["metadata_first_claimed_at"]) if row["metadata_first_claimed_at"] else None,
        metadata_claimed_by=str(row["metadata_claimed_by"]) if row["metadata_claimed_by"] else None,
        metadata_claimed_at=str(row["metadata_claimed_at"]) if row["metadata_claimed_at"] else None,
        metadata_source_taxon_id=row["metadata_source_taxon_id"],
    )


def row_to_discovery_scope(row: sqlite3.Row) -> DiscoveryScopeRecord:
    return DiscoveryScopeRecord(
        id=int(row["id"]),
        scope_value=str(row["scope_value"]),
        scope_label=str(row["scope_label"]),
        target_rank=normalize_taxon_rank(row["target_rank"]),
        assembly_source=normalize_assembly_source(row["assembly_source"]),
        status=str(row["status"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        is_internal=bool(row["is_internal"]),
        discovered_species_count=int(row["discovered_species_count"] or 0),
        last_discovered_at=str(row["last_discovered_at"]) if row["last_discovered_at"] else None,
        last_error=str(row["last_error"]) if row["last_error"] else None,
        refresh_requested=bool(row["refresh_requested"]),
    )


def load_job(job_id: str, db: sqlite3.Connection | None = None) -> JobRecord:
    connection = db or get_db()
    row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise FileNotFoundError(job_id)
    return row_to_job(row)


def get_species_by_id(species_id: int, db: sqlite3.Connection | None = None) -> SpeciesRecord | None:
    connection = db or get_db()
    row = connection.execute("SELECT * FROM species WHERE id = ?", (species_id,)).fetchone()
    return row_to_species(row) if row is not None else None


def load_species(species_id: int, db: sqlite3.Connection | None = None) -> SpeciesRecord:
    species = get_species_by_id(species_id, db)
    if species is None:
        raise FileNotFoundError(str(species_id))
    return species


def get_discovery_scope_by_id(scope_id: int, db: sqlite3.Connection | None = None) -> DiscoveryScopeRecord | None:
    connection = db or get_db()
    row = connection.execute("SELECT * FROM discovery_scopes WHERE id = ?", (scope_id,)).fetchone()
    return row_to_discovery_scope(row) if row is not None else None


def load_discovery_scope(scope_id: int, db: sqlite3.Connection | None = None) -> DiscoveryScopeRecord:
    scope = get_discovery_scope_by_id(scope_id, db)
    if scope is None:
        raise FileNotFoundError(str(scope_id))
    return scope


def get_discovery_scope_by_value(scope_value: str, db: sqlite3.Connection | None = None) -> DiscoveryScopeRecord | None:
    connection = db or get_db()
    row = connection.execute("SELECT * FROM discovery_scopes WHERE scope_value = ?", (scope_value,)).fetchone()
    return row_to_discovery_scope(row) if row is not None else None


def get_species_by_name(species_name: str, db: sqlite3.Connection | None = None) -> SpeciesRecord | None:
    connection = db or get_db()
    normalized = normalize_species_name(species_name)
    row = connection.execute("SELECT * FROM species WHERE lower(species_name) = lower(?)", (normalized,)).fetchone()
    return row_to_species(row) if row is not None else None


def get_taxon_by_name(name: str, rank: str, db: sqlite3.Connection | None = None) -> SpeciesRecord | None:
    connection = db or get_db()
    normalized = normalize_species_name(name)
    row = connection.execute(
        "SELECT * FROM species WHERE lower(species_name) = lower(?) AND taxon_rank = ?",
        (normalized, normalize_taxon_rank(rank)),
    ).fetchone()
    return row_to_species(row) if row is not None else None


def save_species(species: SpeciesRecord, db: sqlite3.Connection | None = None) -> SpeciesRecord:
    connection = db or get_db()
    species_dir(species.slug).mkdir(parents=True, exist_ok=True)
    cursor = connection.execute(
        """
        INSERT INTO species (
            id, species_name, slug, taxon_rank, claim_token, sync_attempt_count, sync_first_claimed_at, assembly_source,
            status, created_at, updated_at, query_name, taxon_id, genome_count, tsv_path, last_synced_at, sync_error,
            refresh_requested, claimed_by, claimed_at, metadata_status, metadata_path, metadata_clean_path,
            metadata_last_built_at, metadata_error, metadata_refresh_requested, metadata_claim_token,
            metadata_attempt_count, metadata_first_claimed_at, metadata_claimed_by, metadata_claimed_at,
            metadata_source_taxon_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            species_name = excluded.species_name,
            slug = excluded.slug,
            taxon_rank = excluded.taxon_rank,
            claim_token = excluded.claim_token,
            sync_attempt_count = excluded.sync_attempt_count,
            sync_first_claimed_at = excluded.sync_first_claimed_at,
            assembly_source = excluded.assembly_source,
            status = excluded.status,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            query_name = excluded.query_name,
            taxon_id = excluded.taxon_id,
            genome_count = excluded.genome_count,
            tsv_path = excluded.tsv_path,
            last_synced_at = excluded.last_synced_at,
            sync_error = excluded.sync_error,
            refresh_requested = excluded.refresh_requested,
            claimed_by = excluded.claimed_by,
            claimed_at = excluded.claimed_at,
            metadata_status = excluded.metadata_status,
            metadata_path = excluded.metadata_path,
            metadata_clean_path = excluded.metadata_clean_path,
            metadata_last_built_at = excluded.metadata_last_built_at,
            metadata_error = excluded.metadata_error,
            metadata_refresh_requested = excluded.metadata_refresh_requested,
            metadata_claim_token = excluded.metadata_claim_token,
            metadata_attempt_count = excluded.metadata_attempt_count,
            metadata_first_claimed_at = excluded.metadata_first_claimed_at,
            metadata_claimed_by = excluded.metadata_claimed_by,
            metadata_claimed_at = excluded.metadata_claimed_at,
            metadata_source_taxon_id = excluded.metadata_source_taxon_id
        """,
        (
            species.id if species.id else None,
            species.species_name,
            species.slug,
            normalize_taxon_rank(species.taxon_rank),
            species.claim_token,
            species.sync_attempt_count,
            species.sync_first_claimed_at,
            normalize_assembly_source(species.assembly_source),
            species.status,
            species.created_at,
            species.updated_at,
            species.query_name,
            species.taxon_id,
            species.genome_count,
            species.tsv_path,
            species.last_synced_at,
            species.sync_error,
            int(species.refresh_requested),
            species.claimed_by,
            species.claimed_at,
            species.metadata_status,
            species.metadata_path,
            species.metadata_clean_path,
            species.metadata_last_built_at,
            species.metadata_error,
            int(species.metadata_refresh_requested),
            species.metadata_claim_token,
            species.metadata_attempt_count,
            species.metadata_first_claimed_at,
            species.metadata_claimed_by,
            species.metadata_claimed_at,
            species.metadata_source_taxon_id,
        ),
    )
    if not species.id:
        species.id = int(cursor.lastrowid)
    connection.commit()
    saved = get_species_by_id(species.id, connection)
    assert saved is not None
    return saved


def save_discovery_scope(scope: DiscoveryScopeRecord, db: sqlite3.Connection | None = None) -> DiscoveryScopeRecord:
    connection = db or get_db()
    cursor = connection.execute(
        """
        INSERT INTO discovery_scopes (
            id, scope_value, scope_label, target_rank, assembly_source, status, created_at, updated_at, is_internal,
            discovered_species_count, last_discovered_at, last_error, refresh_requested
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            scope_value = excluded.scope_value,
            scope_label = excluded.scope_label,
            target_rank = excluded.target_rank,
            assembly_source = excluded.assembly_source,
            status = excluded.status,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            is_internal = excluded.is_internal,
            discovered_species_count = excluded.discovered_species_count,
            last_discovered_at = excluded.last_discovered_at,
            last_error = excluded.last_error,
            refresh_requested = excluded.refresh_requested
        """,
        (
            scope.id if scope.id else None,
            scope.scope_value,
            scope.scope_label,
            normalize_taxon_rank(scope.target_rank),
            normalize_assembly_source(scope.assembly_source),
            scope.status,
            scope.created_at,
            scope.updated_at,
            int(scope.is_internal),
            scope.discovered_species_count,
            scope.last_discovered_at,
            scope.last_error,
            int(scope.refresh_requested),
        ),
    )
    if not scope.id:
        scope.id = int(cursor.lastrowid)
    connection.commit()
    saved = get_discovery_scope_by_id(scope.id, connection)
    assert saved is not None
    return saved


def list_all_species() -> list[SpeciesRecord]:
    rows = get_db().execute("SELECT * FROM species ORDER BY taxon_rank, species_name COLLATE NOCASE ASC").fetchall()
    return [row_to_species(row) for row in rows]


def list_available_species() -> list[SpeciesRecord]:
    rows = get_db().execute(
        """
        SELECT *
        FROM species
        WHERE status = 'ready'
          AND tsv_path IS NOT NULL
        ORDER BY taxon_rank, species_name COLLATE NOCASE ASC
        """
    ).fetchall()
    return [row_to_species(row) for row in rows]


def list_discovery_scopes() -> list[DiscoveryScopeRecord]:
    rows = get_db().execute(
        """
        SELECT *
        FROM discovery_scopes
        WHERE is_internal = 0
        ORDER BY scope_label COLLATE NOCASE ASC, scope_value ASC
        """
    ).fetchall()
    return [row_to_discovery_scope(row) for row in rows]


def save_job(job: JobRecord, db: sqlite3.Connection | None = None) -> None:
    connection = db or get_db()
    job_dir(job.id).mkdir(parents=True, exist_ok=True)
    connection.execute(
        """
        INSERT INTO jobs (
            id, mode, status, created_at, updated_at, input_name, input_path, output_dir, log_path,
            command_json, owner_user_id, owner_username, pid, return_code, error, filters_json,
            cancel_requested
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            mode = excluded.mode,
            status = excluded.status,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            input_name = excluded.input_name,
            input_path = excluded.input_path,
            output_dir = excluded.output_dir,
            log_path = excluded.log_path,
            command_json = excluded.command_json,
            owner_user_id = excluded.owner_user_id,
            owner_username = excluded.owner_username,
            pid = excluded.pid,
            return_code = excluded.return_code,
            error = excluded.error,
            filters_json = excluded.filters_json,
            cancel_requested = excluded.cancel_requested
        """,
        (
            job.id,
            job.mode,
            job.status,
            job.created_at,
            job.updated_at,
            job.input_name,
            job.input_path,
            job.output_dir,
            job.log_path,
            json.dumps(job.command),
            job.owner_user_id,
            job.owner_username,
            job.pid,
            job.return_code,
            job.error,
            json.dumps(job.filters) if job.filters is not None else None,
            int(job.cancel_requested),
        ),
    )
    connection.commit()


def list_jobs_for_user(user_id: int) -> list[JobRecord]:
    rows = get_db().execute(
        "SELECT * FROM jobs WHERE owner_user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall()
    return [row_to_job(row) for row in rows]


def build_public_home_metrics() -> dict[str, Any]:
    db = get_db()
    counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN taxon_rank = 'species' THEN 1 ELSE 0 END) AS species_total,
            SUM(CASE WHEN taxon_rank = 'species' AND tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS species_ready,
            SUM(CASE WHEN taxon_rank = 'genus' THEN 1 ELSE 0 END) AS genus_total,
            SUM(CASE WHEN taxon_rank = 'genus' AND tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS genus_ready,
            SUM(CASE WHEN tsv_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS genome_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS metadata_genome_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS metadata_taxa_ready
        FROM species
        """
    ).fetchone()
    report_counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_reports,
            COUNT(*) AS total_reports
        FROM problem_reports
        """
    ).fetchone()

    def gauge(value: int, total: int) -> int:
        if total <= 0:
            return 0
        return max(0, min(100, int(round((value / total) * 100))))

    species_ready = int(counts["species_ready"] or 0)
    species_total = int(counts["species_total"] or 0)
    genus_ready = int(counts["genus_ready"] or 0)
    genus_total = int(counts["genus_total"] or 0)
    metadata_genome_total = int(counts["metadata_genome_total"] or 0)
    genome_total = int(counts["genome_total"] or 0)
    return {
        "species_total": species_total,
        "species_ready": species_ready,
        "species_gauge": gauge(species_ready, species_total),
        "genus_total": genus_total,
        "genus_ready": genus_ready,
        "genus_gauge": gauge(genus_ready, genus_total),
        "genome_total": genome_total,
        "metadata_genome_total": metadata_genome_total,
        "genome_gauge": gauge(metadata_genome_total, genome_total),
        "metadata_taxa_ready": int(counts["metadata_taxa_ready"] or 0),
        "open_reports": int(report_counts["open_reports"] or 0),
        "total_reports": int(report_counts["total_reports"] or 0),
    }


def create_problem_report(
    *,
    user_id: int | None,
    username: str,
    email: str | None,
    taxon_id: int | None,
    taxon_name: str | None,
    requested_action: str | None,
    message: str,
) -> int:
    now = utc_now()
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO problem_reports (
            user_id, username, email, taxon_id, taxon_name, requested_action,
            message, status, admin_note, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'open', NULL, ?, ?)
        """,
        (
            user_id,
            username,
            email,
            taxon_id,
            taxon_name,
            requested_action,
            message,
            now,
            now,
        ),
    )
    db.commit()
    return int(cursor.lastrowid)


def list_problem_reports() -> list[sqlite3.Row]:
    return get_db().execute(
        """
        SELECT pr.*, s.taxon_rank
        FROM problem_reports pr
        LEFT JOIN species s ON s.id = pr.taxon_id
        ORDER BY
            CASE WHEN pr.status = 'open' THEN 0 ELSE 1 END,
            pr.created_at DESC,
            pr.id DESC
        """
    ).fetchall()


def send_problem_report_email(
    *,
    username: str,
    email: str | None,
    taxon_name: str | None,
    requested_action: str | None,
    message: str,
) -> None:
    recipient = app.config["MAIL_FROM"]
    if not recipient:
        raise RuntimeError("Problem report email recipient is not configured.")
    send_email(
        recipient,
        "fetchM Web problem report",
        [
            f"User: {username}",
            f"Email: {email or 'not provided'}",
            f"Taxon: {taxon_name or 'not selected'}",
            f"Requested action: {requested_action or 'not specified'}",
            "",
            message,
        ],
    )


def update_problem_report_status(report_id: int, status: str, admin_note: str | None = None) -> bool:
    normalized = status if status in {"open", "resolved"} else "open"
    cursor = get_db().execute(
        """
        UPDATE problem_reports
        SET status = ?,
            admin_note = COALESCE(?, admin_note),
            updated_at = ?
        WHERE id = ?
        """,
        (normalized, admin_note, utc_now(), report_id),
    )
    get_db().commit()
    return bool(cursor.rowcount)


def count_active_jobs_for_user(user_id: int) -> int:
    row = get_db().execute(
        """
        SELECT COUNT(*)
        FROM jobs
        WHERE owner_user_id = ?
          AND status IN ('queued', 'running')
        """,
        (user_id,),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def list_all_jobs() -> list[JobRecord]:
    rows = get_db().execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    return [row_to_job(row) for row in rows]


def list_all_users() -> list[sqlite3.Row]:
    return get_db().execute(
        """
        SELECT u.*,
               COUNT(j.id) AS job_count
        FROM users u
        LEFT JOIN jobs j ON j.owner_user_id = u.id
        GROUP BY u.id
        ORDER BY u.created_at DESC
        """
    ).fetchall()


def filter_admin_users(users: list[sqlite3.Row], query: str) -> list[sqlite3.Row]:
    if not query:
        return users
    needle = query.lower()
    return [
        user
        for user in users
        if needle in str(user["username"]).lower() or needle in str(user["email"]).lower()
    ]


def filter_admin_jobs(jobs: list[JobRecord], query: str, status: str) -> list[JobRecord]:
    filtered = jobs
    if query:
        needle = query.lower()
        filtered = [
            job
            for job in filtered
            if needle in job.id.lower()
            or needle in (job.owner_username or "").lower()
            or needle in job.mode.lower()
            or needle in job.input_name.lower()
        ]
    if status and status != "all":
        filtered = [job for job in filtered if job.status == status]
    return filtered


def format_eta_hours(hours: float | None) -> str:
    if hours is None or hours < 0:
        return "Unknown"
    if hours < 1:
        return "Under 1 hour"
    if hours < 48:
        return f"{int(round(hours))} hours"
    days = hours / 24
    if days < 14:
        return f"{days:.1f} days"
    return f"{int(round(days))} days"


def format_bytes(num_bytes: int | None) -> str:
    if num_bytes is None or num_bytes < 0:
        return "Unknown"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


def directory_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = Path(root) / name
            try:
                if file_path.is_symlink():
                    continue
                total += file_path.stat().st_size
            except OSError:
                continue
    return total


def format_claim_age(claimed_at: str | None, now: datetime | None = None) -> str:
    if not claimed_at:
        return "Unknown"
    reference = now or utc_now_dt()
    try:
        age = reference - parse_utc(claimed_at)
    except ValueError:
        return "Unknown"
    total_seconds = max(int(age.total_seconds()), 0)
    if total_seconds < 60:
        return f"{total_seconds}s"
    total_minutes = total_seconds // 60
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours, minutes = divmod(total_minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, rem_hours = divmod(hours, 24)
    return f"{days}d {rem_hours}h"


def build_backfill_dashboard() -> dict[str, Any]:
    now = utc_now_dt()
    last_hour = (now - timedelta(hours=1)).isoformat()
    last_6h = (now - timedelta(hours=6)).isoformat()
    last_7d = (now - timedelta(days=7)).isoformat()
    last_30d = (now - timedelta(days=30)).isoformat()
    db = get_db()
    disk_total, disk_used, disk_free = shutil.disk_usage(DATA_DIR)
    app_data_bytes = directory_size_bytes(DATA_DIR)
    db_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    jobs_bytes = directory_size_bytes(JOBS_DIR)
    species_bytes = directory_size_bytes(SPECIES_DIR)

    taxa_totals = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN taxon_rank = 'species' THEN 1 ELSE 0 END) AS species_total,
            SUM(CASE WHEN taxon_rank = 'genus' THEN 1 ELSE 0 END) AS genus_total,
            SUM(CASE WHEN tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS ready_total,
            SUM(CASE WHEN tsv_path IS NULL AND status = 'pending' THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN tsv_path IS NULL AND status = 'syncing' THEN 1 ELSE 0 END) AS syncing_total,
            SUM(CASE WHEN tsv_path IS NOT NULL AND claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS refreshing_total,
            SUM(CASE WHEN tsv_path IS NOT NULL AND refresh_requested = 1 AND claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued_total,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_total
        FROM species
        """
    ).fetchone()

    rank_rows = db.execute(
        """
        SELECT
            taxon_rank,
            COUNT(*) AS total,
            SUM(CASE WHEN tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS ready,
            SUM(CASE WHEN tsv_path IS NULL AND status = 'pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN tsv_path IS NULL AND status = 'syncing' THEN 1 ELSE 0 END) AS syncing,
            SUM(CASE WHEN tsv_path IS NOT NULL AND claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS refreshing,
            SUM(CASE WHEN tsv_path IS NOT NULL AND refresh_requested = 1 AND claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        GROUP BY taxon_rank
        ORDER BY taxon_rank
        """
    ).fetchall()

    discovery_totals = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN is_internal = 0 THEN 1 ELSE 0 END) AS public_total,
            SUM(CASE WHEN is_internal = 1 THEN 1 ELSE 0 END) AS internal_total,
            SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) AS ready_total,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN status = 'discovering' THEN 1 ELSE 0 END) AS discovering_total,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_total
        FROM discovery_scopes
        """
    ).fetchone()

    recent_taxa = db.execute(
        """
        SELECT
            SUM(CASE WHEN last_synced_at >= ? THEN 1 ELSE 0 END) AS last_hour,
            SUM(CASE WHEN last_synced_at >= ? THEN 1 ELSE 0 END) AS last_6h
        FROM species
        WHERE last_synced_at IS NOT NULL
        """,
        (last_hour, last_6h),
    ).fetchone()

    recent_scopes = db.execute(
        """
        SELECT
            SUM(CASE WHEN last_discovered_at >= ? THEN 1 ELSE 0 END) AS last_hour,
            SUM(CASE WHEN last_discovered_at >= ? THEN 1 ELSE 0 END) AS last_6h
        FROM discovery_scopes
        WHERE last_discovered_at IS NOT NULL
        """,
        (last_hour, last_6h),
    ).fetchone()

    sync_event_totals = db.execute(
        """
        SELECT
            SUM(CASE WHEN synced_at >= ? THEN COALESCE(delta_genome_count, 0) ELSE 0 END) AS genomes_added_last_7d,
            SUM(CASE WHEN synced_at >= ? THEN COALESCE(delta_genome_count, 0) ELSE 0 END) AS genomes_added_last_30d,
            SUM(CASE WHEN sync_kind = 'build' AND synced_at >= ? THEN 1 ELSE 0 END) AS build_events_last_30d,
            SUM(CASE WHEN sync_kind = 'refresh' AND synced_at >= ? THEN 1 ELSE 0 END) AS refresh_events_last_30d
        FROM taxon_sync_events
        """,
        (last_7d, last_30d, last_30d, last_30d),
    ).fetchone()

    latest_sync_event = db.execute(
        """
        SELECT
            species_name,
            taxon_rank,
            sync_kind,
            before_genome_count,
            after_genome_count,
            delta_genome_count,
            synced_at
        FROM taxon_sync_events
        ORDER BY synced_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()

    genome_totals = db.execute(
        """
        SELECT
            SUM(CASE WHEN tsv_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS catalog_genomes_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS metadata_genomes_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS metadata_taxa_ready_total,
            (SELECT COUNT(*) FROM assembly_metadata) AS metadata_stored_rows_total
        FROM species
        """
    ).fetchone()

    recent_taxa_added = db.execute(
        """
        SELECT
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS last_7d,
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS last_30d,
            SUM(CASE WHEN taxon_rank = 'species' AND created_at >= ? THEN 1 ELSE 0 END) AS species_last_30d,
            SUM(CASE WHEN taxon_rank = 'genus' AND created_at >= ? THEN 1 ELSE 0 END) AS genus_last_30d
        FROM species
        """
        ,
        (last_7d, last_30d, last_30d, last_30d),
    ).fetchone()

    syncing_rows = db.execute(
        """
        SELECT
            id,
            species_name,
            taxon_rank,
            query_name,
            assembly_source,
            sync_attempt_count,
            sync_first_claimed_at,
            claimed_by,
            claimed_at,
            updated_at,
            genome_count
        FROM species
        WHERE status = 'syncing'
           OR (status = 'ready' AND claimed_at IS NOT NULL AND tsv_path IS NOT NULL)
        ORDER BY claimed_at ASC, updated_at ASC, id ASC
        """
    ).fetchall()

    recent_ready_rate = 0.0
    if recent_taxa is not None:
        last_6h_count = int(recent_taxa["last_6h"] or 0)
        last_1h_count = int(recent_taxa["last_hour"] or 0)
        recent_ready_rate = max(float(last_1h_count), last_6h_count / 6 if last_6h_count else 0.0)

    pending_total = int(taxa_totals["pending_total"] or 0) if taxa_totals is not None else 0
    syncing_total = int(taxa_totals["syncing_total"] or 0) if taxa_totals is not None else 0
    eta_hours = ((pending_total + syncing_total) / recent_ready_rate) if recent_ready_rate > 0 else None

    progress_percent = 0
    total_taxa = int(taxa_totals["total"] or 0) if taxa_totals is not None else 0
    ready_taxa = int(taxa_totals["ready_total"] or 0) if taxa_totals is not None else 0
    if total_taxa > 0:
        progress_percent = int(round((ready_taxa / total_taxa) * 100))

    syncing_taxa: list[dict[str, Any]] = []
    for row in syncing_rows:
        claimed_at = str(row["claimed_at"]) if row["claimed_at"] else None
        first_claimed_at = str(row["sync_first_claimed_at"]) if row["sync_first_claimed_at"] else None
        age_label = format_claim_age(claimed_at, now)
        total_age_label = format_claim_age(first_claimed_at, now)
        age_minutes = 0
        total_age_minutes = 0
        if claimed_at:
            try:
                age_minutes = max(int((now - parse_utc(claimed_at)).total_seconds() // 60), 0)
            except ValueError:
                age_minutes = 0
        if first_claimed_at:
            try:
                total_age_minutes = max(int((now - parse_utc(first_claimed_at)).total_seconds() // 60), 0)
            except ValueError:
                total_age_minutes = 0
        attempt_count = int(row["sync_attempt_count"] or 0)
        syncing_taxa.append(
            {
                "id": row["id"],
                "species_name": row["species_name"],
                "taxon_rank": row["taxon_rank"],
                "query_name": row["query_name"],
                "assembly_source": row["assembly_source"],
                "claimed_by": row["claimed_by"] or "worker",
                "claimed_at": claimed_at,
                "claimed_age_label": age_label,
                "claimed_age_minutes": age_minutes,
                "sync_first_claimed_at": first_claimed_at,
                "total_age_label": total_age_label,
                "total_age_minutes": total_age_minutes,
                "attempt_count": attempt_count,
                "is_stale": total_age_minutes >= 30,
                "has_retries": attempt_count > 1,
                "genome_count": row["genome_count"],
                "refreshing_existing": claimed_at is not None and row["genome_count"] is not None and row["id"] is not None,
            }
        )

    return {
        "storage": {
            "disk_total_bytes": disk_total,
            "disk_used_bytes": disk_used,
            "disk_free_bytes": disk_free,
            "disk_used_percent": round((disk_used / disk_total) * 100, 1) if disk_total else 0.0,
            "disk_total_label": format_bytes(disk_total),
            "disk_used_label": format_bytes(disk_used),
            "disk_free_label": format_bytes(disk_free),
            "app_data_bytes": app_data_bytes,
            "app_data_label": format_bytes(app_data_bytes),
            "db_bytes": db_bytes,
            "db_label": format_bytes(db_bytes),
            "jobs_bytes": jobs_bytes,
            "jobs_label": format_bytes(jobs_bytes),
            "species_bytes": species_bytes,
            "species_label": format_bytes(species_bytes),
        },
        "taxa_totals": dict(taxa_totals) if taxa_totals is not None else {},
        "rank_breakdown": [dict(row) for row in rank_rows],
        "discovery_totals": dict(discovery_totals) if discovery_totals is not None else {},
        "recent_taxa": dict(recent_taxa) if recent_taxa is not None else {"last_hour": 0, "last_6h": 0},
        "recent_scopes": dict(recent_scopes) if recent_scopes is not None else {"last_hour": 0, "last_6h": 0},
        "syncing_taxa": syncing_taxa,
        "taxa_ready_rate_per_hour": recent_ready_rate,
        "eta_hours": eta_hours,
        "eta_label": format_eta_hours(eta_hours),
        "progress_percent": progress_percent,
        "discovery_policy": get_discovery_policy(db),
        "catalog_build_policy": get_catalog_build_policy(db),
        "catalog_refresh_policy": get_catalog_refresh_policy(db),
        "genome_totals": dict(genome_totals) if genome_totals is not None else {},
        "recent_taxa_added": dict(recent_taxa_added) if recent_taxa_added is not None else {"last_7d": 0, "last_30d": 0, "species_last_30d": 0, "genus_last_30d": 0},
        "sync_event_totals": dict(sync_event_totals) if sync_event_totals is not None else {"genomes_added_last_7d": 0, "genomes_added_last_30d": 0, "build_events_last_30d": 0, "refresh_events_last_30d": 0},
        "latest_sync_event": dict(latest_sync_event) if latest_sync_event is not None else None,
        "system_monitor": build_system_monitor(db),
    }


def build_metadata_dashboard() -> dict[str, Any]:
    db = get_db()
    totals = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN metadata_path IS NOT NULL THEN 1 ELSE 0 END) AS ready_total,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'pending' THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'building' THEN 1 ELSE 0 END) AS building_total,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS refreshing_total,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_refresh_requested = 1 AND metadata_claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued_total,
            SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS clean_ready_total
        FROM species
        WHERE tsv_path IS NOT NULL
        """
    ).fetchone()
    rank_rows = db.execute(
        """
        SELECT
            taxon_rank,
            COUNT(*) AS total,
            SUM(CASE WHEN metadata_path IS NOT NULL THEN 1 ELSE 0 END) AS ready,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'building' THEN 1 ELSE 0 END) AS building,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS refreshing,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_refresh_requested = 1 AND metadata_claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued,
            SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        WHERE tsv_path IS NOT NULL
        GROUP BY taxon_rank
        ORDER BY taxon_rank
        """
    ).fetchall()
    active_rows = db.execute(
        """
        SELECT
            id,
            species_name,
            taxon_rank,
            metadata_status,
            metadata_claimed_by,
            metadata_claimed_at,
            metadata_first_claimed_at,
            metadata_attempt_count,
            metadata_error,
            metadata_source_taxon_id
        FROM species
        WHERE metadata_status IN ('pending', 'building', 'failed')
           OR (metadata_status = 'ready' AND metadata_path IS NOT NULL AND (metadata_refresh_requested = 1 OR metadata_claimed_at IS NOT NULL))
        ORDER BY
            CASE
                WHEN metadata_status = 'ready' AND metadata_claimed_at IS NOT NULL THEN 0
                WHEN metadata_status = 'building' THEN 1
                WHEN metadata_status = 'ready' AND metadata_refresh_requested = 1 THEN 2
                WHEN metadata_status = 'pending' THEN 3
                ELSE 4
            END,
            updated_at ASC
        LIMIT 50
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    now = utc_now_dt()
    for row in active_rows:
        claimed_at = str(row["metadata_claimed_at"]) if row["metadata_claimed_at"] else None
        first_claimed_at = str(row["metadata_first_claimed_at"]) if row["metadata_first_claimed_at"] else None
        items.append(
            {
                "id": int(row["id"]),
                "species_name": str(row["species_name"]),
                "taxon_rank": str(row["taxon_rank"]),
                "status": str(row["metadata_status"]),
                "claimed_by": str(row["metadata_claimed_by"]) if row["metadata_claimed_by"] else None,
                "claimed_at": claimed_at,
                "claimed_age_label": format_claim_age(claimed_at, now),
                "total_age_label": format_claim_age(first_claimed_at, now),
                "attempt_count": int(row["metadata_attempt_count"] or 0),
                "error": str(row["metadata_error"]) if row["metadata_error"] else None,
                "source_taxon_id": row["metadata_source_taxon_id"],
                "refresh_queued": bool(row["metadata_status"] == "ready" and not row["metadata_claimed_at"]),
                "refreshing_existing": bool(row["metadata_status"] == "ready" and row["metadata_claimed_at"]),
            }
        )
    return {
        "totals": dict(totals) if totals is not None else {},
        "rank_breakdown": [dict(row) for row in rank_rows],
        "active_items": items,
        "metadata_build_policy": get_metadata_build_policy(db),
        "metadata_refresh_policy": get_metadata_refresh_policy(db),
    }


def admin_common_context(section: str) -> dict[str, Any]:
    return {
        "discovery_policies": DISCOVERY_POLICIES,
        "catalog_policies": CATALOG_POLICIES,
        "metadata_policies": METADATA_POLICIES,
        "taxon_ranks": TAXON_RANKS,
        "assembly_sources": ASSEMBLY_SOURCES,
        "admin_section": section,
    }


def nested_get(payload: dict[str, Any], *keys: str) -> Any:
    value: Any = payload
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
        if value is None:
            return None
    return value


def biosample_attribute(biosample: dict[str, Any], attribute_name: str) -> str | None:
    attributes = biosample.get("attributes", [])
    for attribute in attributes:
        if attribute.get("name") == attribute_name:
            value = attribute.get("value")
            if value:
                return str(value)
    direct_value = biosample.get(attribute_name)
    return str(direct_value) if direct_value else None


def build_species_tsv_row(payload: dict[str, Any]) -> dict[str, Any]:
    biosample = nested_get(payload, "assembly_info", "biosample") or {}
    assembly_info = payload.get("assembly_info", {}) or {}
    assembly_stats = payload.get("assembly_stats", {}) or {}
    organism = payload.get("organism", {}) or {}
    annotation_stats = nested_get(payload, "annotation_info", "stats", "gene_counts") or {}
    checkm_info = payload.get("checkm_info", {}) or {}
    return {
        "Assembly Accession": payload.get("accession") or payload.get("current_accession"),
        "Assembly Name": assembly_info.get("assembly_name"),
        "Organism Name": nested_get(payload, "organism", "organism_name")
        or nested_get(biosample, "description", "organism", "organism_name"),
        "Assembly Level": assembly_info.get("assembly_level") or assembly_info.get("assembly_level_name"),
        "Assembly Status": assembly_info.get("assembly_status"),
        "Assembly Release Date": assembly_info.get("release_date") or payload.get("release_date"),
        "ANI Check status": nested_get(payload, "average_nucleotide_identity", "taxonomy_check_status"),
        "Annotation Name": nested_get(payload, "annotation_info", "pipeline"),
        "Assembly BioProject Accession": assembly_info.get("bioproject_accession"),
        "Assembly BioSample Accession": biosample.get("accession"),
        "Organism Infraspecific Names Strain": nested_get(organism, "infraspecific_names", "strain")
        or biosample_attribute(biosample, "strain"),
        "Assembly Stats Total Sequence Length": assembly_stats.get("total_sequence_length"),
        "Assembly Stats Total Ungapped Length": assembly_stats.get("total_ungapped_length"),
        "Assembly Stats GC Percent": assembly_stats.get("gc_percent"),
        "Assembly Stats Number of Contigs": assembly_stats.get("number_of_contigs"),
        "Assembly Stats Number of Scaffolds": assembly_stats.get("number_of_scaffolds"),
        "Assembly Stats Contig N50": assembly_stats.get("contig_n50"),
        "Assembly Stats Scaffold N50": assembly_stats.get("scaffold_n50"),
        "Annotation Count Gene Total": annotation_stats.get("total"),
        "Annotation Count Gene Protein-coding": annotation_stats.get("protein_coding"),
        "Annotation Count Gene Pseudogene": annotation_stats.get("pseudogene"),
        "CheckM completeness": checkm_info.get("completeness"),
        "CheckM contamination": checkm_info.get("contamination"),
    }


def normalize_metadata_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "none", "nan"} else text


def is_meaningful_metadata_value(value: Any) -> bool:
    text = normalize_metadata_value(value)
    return bool(text and text.lower() not in {"absent", "unknown", "not provided", "not applicable", "missing"})


def numeric_summary(values: list[float]) -> dict[str, float] | None:
    clean_values = sorted(value for value in values if value is not None)
    if not clean_values:
        return None
    size = len(clean_values)
    midpoint = size // 2
    median = clean_values[midpoint] if size % 2 else (clean_values[midpoint - 1] + clean_values[midpoint]) / 2
    return {
        "min": clean_values[0],
        "mean": sum(clean_values) / size,
        "median": median,
        "max": clean_values[-1],
    }


def summarize_top_values(rows: list[dict[str, str]], field: str, *, limit: int = 8) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = normalize_metadata_value(row.get(field))
        if not is_meaningful_metadata_value(value):
            continue
        counter[value] += 1
    return counter.most_common(limit)


def summarize_year_span(rows: list[dict[str, str]]) -> tuple[int | None, int | None]:
    years: list[int] = []
    for row in rows:
        value = normalize_metadata_value(row.get("Collection Date"))
        match = re.search(r"\b(19|20)\d{2}\b", value)
        if match:
            years.append(int(match.group(0)))
    if not years:
        return None, None
    return min(years), max(years)


REPORT_MISSING_LABELS = {"absent", "unknown"}


def metadata_dataframe(rows: list[dict[str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.fillna("")


def metadata_value_counts(frame: pd.DataFrame, field: str, *, limit: int = 8) -> list[tuple[str, int]]:
    if field not in frame.columns:
        return []
    series = frame[field].astype(str).str.strip()
    series = series[
        series.ne("")
        & ~series.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing", "none", "nan"})
    ]
    counts = series.value_counts().head(limit)
    return [(str(index), int(value)) for index, value in counts.items()]


def metadata_numeric_series(frame: pd.DataFrame, field: str) -> pd.Series:
    if field not in frame.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(frame[field], errors="coerce").dropna()


def metadata_present_unknown_absent(frame: pd.DataFrame, field: str) -> dict[str, Any]:
    if field not in frame.columns:
        return {
            "field": field,
            "present": 0,
            "unknown": 0,
            "absent": int(len(frame)),
            "present_percent": 0.0,
            "unknown_percent": 0.0,
            "absent_percent": 100.0 if len(frame) else 0.0,
        }
    normalized = frame[field].astype(str).str.strip()
    lower = normalized.str.lower()
    present_mask = normalized.ne("") & ~lower.isin(REPORT_MISSING_LABELS)
    unknown_mask = lower.eq("unknown")
    absent_mask = ~present_mask & ~unknown_mask
    total = len(frame)
    present = int(present_mask.sum())
    unknown = int(unknown_mask.sum())
    absent = int(absent_mask.sum())
    return {
        "field": field,
        "present": present,
        "unknown": unknown,
        "absent": absent,
        "present_percent": round((present / total) * 100, 1) if total else 0.0,
        "unknown_percent": round((unknown / total) * 100, 1) if total else 0.0,
        "absent_percent": round((absent / total) * 100, 1) if total else 0.0,
    }


def metadata_years(frame: pd.DataFrame) -> pd.Series:
    if "Collection Date" not in frame.columns:
        return pd.Series(dtype="float64")
    years = frame["Collection Date"].astype(str).str.extract(r"((?:19|20)\d{2})")[0]
    return pd.to_numeric(years, errors="coerce").dropna()


def format_sequence_length(value: float | None) -> str:
    if value is None:
        return "N/A"
    if value >= 10000:
        return f"{value / 1000:,.1f} Kbp"
    return f"{value:,.0f} bp"


def read_cpu_usage_percent() -> float | None:
    def read_stat() -> tuple[int, int] | None:
        try:
            with open("/proc/stat", "r", encoding="utf-8") as handle:
                first = handle.readline().strip().split()
        except OSError:
            return None
        if len(first) < 8 or first[0] != "cpu":
            return None
        values = [int(item) for item in first[1:]]
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        total = sum(values)
        return total, idle

    start = read_stat()
    if start is None:
        return None
    time.sleep(0.15)
    end = read_stat()
    if end is None:
        return None
    total_delta = end[0] - start[0]
    idle_delta = end[1] - start[1]
    if total_delta <= 0:
        return None
    return round(max(0.0, min(100.0, ((total_delta - idle_delta) / total_delta) * 100)), 1)


def read_memory_usage() -> dict[str, Any]:
    meminfo: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                key, raw_value = line.split(":", 1)
                parts = raw_value.strip().split()
                if not parts:
                    continue
                meminfo[key] = int(parts[0]) * 1024
    except OSError:
        return {
            "total_bytes": 0,
            "used_bytes": 0,
            "available_bytes": 0,
            "used_percent": 0.0,
            "total_label": "N/A",
            "used_label": "N/A",
            "available_label": "N/A",
        }
    total = int(meminfo.get("MemTotal", 0))
    available = int(meminfo.get("MemAvailable", 0))
    used = max(total - available, 0)
    used_percent = round((used / total) * 100, 1) if total else 0.0
    return {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": available,
        "used_percent": used_percent,
        "total_label": format_bytes(total) if total else "N/A",
        "used_label": format_bytes(used) if total else "N/A",
        "available_label": format_bytes(available) if total else "N/A",
    }


def read_network_usage() -> dict[str, Any]:
    def read_dev() -> dict[str, dict[str, int]] | None:
        try:
            lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()
        except OSError:
            return None
        counters: dict[str, dict[str, int]] = {}
        for line in lines[2:]:
            if ":" not in line:
                continue
            iface, values = line.split(":", 1)
            iface = iface.strip()
            if not iface or iface == "lo":
                continue
            parts = values.split()
            if len(parts) < 16:
                continue
            try:
                counters[iface] = {
                    "rx_bytes": int(parts[0]),
                    "tx_bytes": int(parts[8]),
                }
            except ValueError:
                continue
        return counters

    start = read_dev()
    if not start:
        return {
            "interface_count": 0,
            "active_interfaces": [],
            "rx_rate_bytes": 0,
            "tx_rate_bytes": 0,
            "rx_rate_label": "N/A",
            "tx_rate_label": "N/A",
            "rx_total_bytes": 0,
            "tx_total_bytes": 0,
            "rx_total_label": "N/A",
            "tx_total_label": "N/A",
            "link_speed_mbps": None,
            "link_speed_label": "N/A",
            "duplex": None,
            "operstate": None,
            "primary_interface": None,
        }
    preferred_iface = next((name for name in start if name != "lo"), None)

    def read_sysfs_value(iface: str | None, filename: str) -> str | None:
        if not iface:
            return None
        path = Path(f"/sys/class/net/{iface}/{filename}")
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return value or None

    time.sleep(0.15)
    end = read_dev()
    if not end:
        end = start

    interfaces = sorted(set(start) | set(end))
    rx_start = sum(start.get(name, {}).get("rx_bytes", 0) for name in interfaces)
    tx_start = sum(start.get(name, {}).get("tx_bytes", 0) for name in interfaces)
    rx_end = sum(end.get(name, {}).get("rx_bytes", 0) for name in interfaces)
    tx_end = sum(end.get(name, {}).get("tx_bytes", 0) for name in interfaces)
    sample_seconds = 0.15
    rx_rate = max(0, int((rx_end - rx_start) / sample_seconds))
    tx_rate = max(0, int((tx_end - tx_start) / sample_seconds))
    link_speed_raw = read_sysfs_value(preferred_iface, "speed")
    duplex = read_sysfs_value(preferred_iface, "duplex")
    operstate = read_sysfs_value(preferred_iface, "operstate")
    link_speed_mbps = None
    if link_speed_raw:
        try:
            parsed_speed = int(link_speed_raw)
            if parsed_speed > 0:
                link_speed_mbps = parsed_speed
        except ValueError:
            link_speed_mbps = None
    if link_speed_mbps is None:
        link_speed_label = "N/A"
    elif link_speed_mbps >= 1000:
        link_speed_label = f"{link_speed_mbps / 1000:.1f} Gbps"
    else:
        link_speed_label = f"{link_speed_mbps} Mbps"

    return {
        "interface_count": len(interfaces),
        "active_interfaces": interfaces,
        "rx_rate_bytes": rx_rate,
        "tx_rate_bytes": tx_rate,
        "rx_rate_label": f"{format_bytes(rx_rate)}/s",
        "tx_rate_label": f"{format_bytes(tx_rate)}/s",
        "rx_total_bytes": rx_end,
        "tx_total_bytes": tx_end,
        "rx_total_label": format_bytes(rx_end),
        "tx_total_label": format_bytes(tx_end),
        "link_speed_mbps": link_speed_mbps,
        "link_speed_label": link_speed_label,
        "duplex": duplex,
        "operstate": operstate,
        "primary_interface": preferred_iface,
    }


def sensor_display_name(chip_name: str, label: str) -> str:
    low_chip = chip_name.lower()
    if "k10temp" in low_chip:
        return f"CPU {label}"
    if "nvme" in low_chip:
        return f"NVMe {label}"
    if chip_name:
        return f"{chip_name} {label}".strip()
    return label


def sensor_threshold_c(chip_name: str, fallback_threshold_c: float) -> float:
    low_chip = chip_name.lower()
    if "k10temp" in low_chip:
        return max(fallback_threshold_c, 80.0)
    if "nvme" in low_chip:
        return max(fallback_threshold_c, 70.0)
    return fallback_threshold_c


def read_system_temperatures(threshold_c: float) -> list[dict[str, Any]]:
    sensors: list[dict[str, Any]] = []
    for hwmon_dir in sorted(Path("/sys/class/hwmon").glob("hwmon*")):
        chip_name = ""
        try:
            chip_name = (hwmon_dir / "name").read_text(encoding="utf-8").strip()
        except OSError:
            chip_name = ""
        for input_path in sorted(hwmon_dir.glob("temp*_input")):
            try:
                raw_value = input_path.read_text(encoding="utf-8").strip()
                milli_c = int(raw_value)
            except (OSError, ValueError):
                continue
            label_path = Path(str(input_path).replace("_input", "_label"))
            try:
                label = label_path.read_text(encoding="utf-8").strip()
            except OSError:
                label = input_path.stem
            current_c = round(milli_c / 1000, 1)
            limit_c = sensor_threshold_c(chip_name, threshold_c)
            percent = round(max(0.0, min(100.0, (current_c / limit_c) * 100)), 1) if limit_c > 0 else 0.0
            sensors.append(
                {
                    "chip_name": chip_name or "sensor",
                    "label": sensor_display_name(chip_name or "sensor", label),
                    "current_c": current_c,
                    "threshold_c": limit_c,
                    "percent_of_threshold": percent,
                    "status": "failed" if current_c >= limit_c else ("queued" if current_c >= limit_c * 0.9 else "running"),
                }
            )
    return sensors


def maybe_send_temperature_alert(system_monitor: dict[str, Any], db: sqlite3.Connection) -> None:
    settings = system_monitor["alert_settings"]
    if not settings["enabled"] or not settings["email"]:
        return
    hottest = system_monitor.get("hottest_sensor")
    if not hottest or hottest["current_c"] < hottest["threshold_c"]:
        return
    last_sent_at = settings.get("last_sent_at")
    if last_sent_at:
        try:
            if parse_utc(last_sent_at) >= utc_now_dt() - timedelta(minutes=settings["cooldown_minutes"]):
                return
        except ValueError:
            pass
    try:
        send_email(
            settings["email"],
            "fetchM Web system temperature alert",
            [
                f"Hottest sensor: {hottest['label']}",
                f"Current temperature: {hottest['current_c']:.1f} C",
                f"Threshold: {hottest['threshold_c']:.1f} C",
                "",
                "This alert was generated from the admin system monitor.",
            ],
        )
        set_setting("system_temp_alert_last_sent_at", utc_now(), db)
    except Exception:
        return


def build_system_monitor(db: sqlite3.Connection) -> dict[str, Any]:
    threshold_c = get_system_temp_alert_threshold_c(db)
    sensors = read_system_temperatures(threshold_c)
    cpu_percent = read_cpu_usage_percent()
    memory = read_memory_usage()
    network = read_network_usage()
    hottest = max(sensors, key=lambda item: item["current_c"]) if sensors else None
    monitor = {
        "temperatures": sensors,
        "hottest_sensor": hottest,
        "cpu_percent": cpu_percent,
        "cpu_status": "failed" if cpu_percent is not None and cpu_percent >= 90 else ("queued" if cpu_percent is not None and cpu_percent >= 75 else "running"),
        "memory": memory,
        "memory_status": "failed" if memory["used_percent"] >= 90 else ("queued" if memory["used_percent"] >= 75 else "running"),
        "network": network,
        "alert_settings": {
            "enabled": get_system_temp_alert_enabled(db),
            "email": get_system_temp_alert_email(db),
            "threshold_c": threshold_c,
            "cooldown_minutes": get_system_temp_alert_cooldown_minutes(db),
            "last_sent_at": get_system_temp_alert_last_sent_at(db),
        },
    }
    maybe_send_temperature_alert(monitor, db)
    return monitor


def format_numeric_value(value: float | None, *, unit: str = "", sequence_length: bool = False) -> str:
    if value is None:
        return "N/A"
    if sequence_length:
        return format_sequence_length(value)
    if unit == "%":
        return f"{value:,.2f}%"
    if unit:
        return f"{value:,.2f}{unit}"
    return f"{value:,.2f}"


def emphasize_summary_text(text: str) -> str:
    emphasized = text
    replacements = [
        ("FetchM Web", "<strong>FetchM Web</strong>"),
        ("High quality", "<strong>High quality</strong>"),
        ("Medium quality", "<strong>Medium quality</strong>"),
        ("Low quality", "<strong>Low quality</strong>"),
        ("No CheckM mention", "<strong>No CheckM mention</strong>"),
        ("CheckM completeness", "<strong>CheckM completeness</strong>"),
        ("CheckM contamination", "<strong>CheckM contamination</strong>"),
        ("Genome-quality profiling", "<strong>Genome-quality profiling</strong>"),
        ("Quality-band analysis", "<strong>Quality-band analysis</strong>"),
    ]
    for source, target in replacements:
        emphasized = emphasized.replace(source, target)
    emphasized = re.sub(r"(\b\d[\d,]*(?:\.\d+)?(?:\sKbp|%| genomes| metadata columns)?)", r"<strong>\1</strong>", emphasized)
    emphasized = re.sub(r"(\b(?:19|20)\d{2}\b)", r"<strong>\1</strong>", emphasized)
    return emphasized


def metadata_distinct_count(frame: pd.DataFrame, field: str) -> int:
    if field not in frame.columns:
        return 0
    values = frame[field].astype(str).str.strip()
    values = values[
        values.ne("")
        & ~values.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing"})
    ]
    return int(values.nunique())


def build_metadata_insights(
    frame: pd.DataFrame,
    species: SpeciesRecord,
    *,
    total_rows: int,
    year_start: int | None,
    year_end: int | None,
    completeness_rows: list[dict[str, Any]],
) -> list[str]:
    insights: list[str] = []
    insights.append(
        f"This live report summarizes {total_rows:,} genomes for the selected {species.taxon_rank} "
        f"using the currently stored managed metadata."
    )
    if species.metadata_last_built_at:
        insights.append(f"The metadata artifact was last built on {species.metadata_last_built_at}.")

    completeness_map = {row["field"]: row for row in completeness_rows}
    for field in ["Collection Date", "Geographic Location", "Host", "Isolation Source"]:
        row = completeness_map.get(field)
        if not row:
            continue
        insights.append(
            f"{field} is informative for {row['present']:,} genomes ({row['present_percent']}%), "
            f"unknown for {row['unknown']:,}, and absent for {row['absent']:,}."
        )

    top_hosts = metadata_value_counts(frame, "Host", limit=3)
    if top_hosts:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_hosts)
        insights.append(f"The most represented hosts are {rendered}.")

    top_countries = metadata_value_counts(frame, "Country", limit=3)
    if top_countries:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_countries)
        insights.append(f"The leading country annotations are {rendered}.")

    if year_start and year_end:
        insights.append(
            f"Collection years currently span from {year_start} to {year_end}, enabling temporal comparisons."
        )

    completeness = metadata_numeric_series(frame, "CheckM completeness")
    if not completeness.empty:
        insights.append(
            f"Mean CheckM completeness is {completeness.mean():.2f}, with values ranging from "
            f"{completeness.min():.2f} to {completeness.max():.2f}."
        )

    return insights


def build_quality_bands(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if "CheckM completeness" not in frame.columns:
        return []
    completeness = pd.to_numeric(frame["CheckM completeness"], errors="coerce")
    total = int(len(frame))
    if total == 0:
        return []
    bands = [
        ("High quality (>= 95)", int((completeness >= 95).sum())),
        ("Medium quality (90-94.9)", int(((completeness >= 90) & (completeness < 95)).sum())),
        ("Low quality (< 90)", int((completeness < 90).sum())),
        ("No CheckM mention", int(completeness.isna().sum())),
    ]
    return [
        {
            "label": label,
            "count": count,
            "percent": round((count / total) * 100, 1) if total else 0.0,
        }
        for label, count in bands
    ]


def build_correlation_summaries(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if "Collection Date" not in frame.columns:
        return []
    years = pd.to_numeric(frame["Collection Date"].astype(str).str.extract(r"((?:19|20)\d{2})")[0], errors="coerce")
    correlation_targets = [
        ("Assembly sequence length", "Assembly Stats Total Sequence Length"),
        ("Total annotated genes", "Annotation Count Gene Total"),
        ("Protein-coding genes", "Annotation Count Gene Protein-coding"),
        ("Pseudogenes", "Annotation Count Gene Pseudogene"),
    ]
    summaries: list[dict[str, Any]] = []
    for label, field in correlation_targets:
        if field not in frame.columns:
            continue
        metric = pd.to_numeric(frame[field], errors="coerce")
        plot_frame = pd.DataFrame({"year": years, "metric": metric}).dropna()
        if len(plot_frame) < 5 or plot_frame["year"].nunique() < 2 or plot_frame["metric"].nunique() < 2:
            continue
        pearson = plot_frame["year"].corr(plot_frame["metric"], method="pearson")
        spearman = plot_frame["year"].corr(plot_frame["metric"], method="spearman")
        if pd.isna(pearson) or pd.isna(spearman):
            continue
        summaries.append(
            {
                "label": label,
                "n": int(len(plot_frame)),
                "pearson_r": round(float(pearson), 3),
                "spearman_rho": round(float(spearman), 3),
                "direction": "positive" if pearson > 0 else "negative",
            }
        )
    return summaries


def build_numeric_findings(frame: pd.DataFrame) -> list[str]:
    findings: list[str] = []
    numeric_targets = [
        ("Assembly Stats Total Sequence Length", "Genome length", "bp"),
        ("Assembly Stats Number of Contigs", "Contig count", ""),
        ("Assembly Stats Number of Scaffolds", "Scaffold count", ""),
        ("Assembly Stats GC Percent", "GC percent", "%"),
        ("CheckM contamination", "CheckM contamination", ""),
    ]
    for field, label, suffix in numeric_targets:
        series = metadata_numeric_series(frame, field)
        if series.empty:
            continue
        is_sequence_length = field == "Assembly Stats Total Sequence Length"
        findings.append(
            f"{label} ranges from {format_numeric_value(series.min(), unit=suffix, sequence_length=is_sequence_length)} "
            f"to {format_numeric_value(series.max(), unit=suffix, sequence_length=is_sequence_length)}, "
            f"with a median of {format_numeric_value(series.median(), unit=suffix, sequence_length=is_sequence_length)}."
        )
    return findings


def load_taxon_metadata_dataset(species: SpeciesRecord) -> tuple[list[dict[str, str]], list[str], pd.DataFrame]:
    clean_path = Path(species.metadata_clean_path or "")
    if not clean_path.exists():
        raise FileNotFoundError("Metadata output is not ready for this taxon yet.")

    with clean_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [{key: normalize_metadata_value(value) for key, value in row.items()} for row in reader]
        fieldnames = list(reader.fieldnames or [])
    frame = metadata_dataframe(rows)
    return rows, fieldnames, frame


def parse_optional_float(value: Any) -> float | None:
    text = normalize_metadata_value(value)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_optional_int(value: Any) -> int | None:
    text = normalize_metadata_value(value)
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def normalize_multiselect_values(source: Any, key: str) -> list[str]:
    values = []
    if hasattr(source, "getlist"):
        values = source.getlist(key)
    else:
        raw = source.get(key) if hasattr(source, "get") else None
        if isinstance(raw, list):
            values = raw
        elif raw is not None:
            values = [raw]
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = normalize_metadata_value(value)
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized


def build_sequence_filter_state(source: Any) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for key in SEQUENCE_FILTER_FIELDS:
        state[key] = normalize_multiselect_values(source, key)
    logic_value = normalize_metadata_value(source.get("filter_logic") if hasattr(source, "get") else None)
    state["filter_logic"] = logic_value if logic_value in {"and", "or"} else "and"
    state["checkm_min"] = parse_optional_float(source.get("checkm_min") if hasattr(source, "get") else None)
    state["checkm_max"] = parse_optional_float(source.get("checkm_max") if hasattr(source, "get") else None)
    state["contamination_max"] = parse_optional_float(
        source.get("contamination_max") if hasattr(source, "get") else None
    )
    state["year_from"] = parse_optional_int(source.get("year_from") if hasattr(source, "get") else None)
    state["year_to"] = parse_optional_int(source.get("year_to") if hasattr(source, "get") else None)
    state["genome_length_min_kbp"] = parse_optional_float(
        source.get("genome_length_min_kbp") if hasattr(source, "get") else None
    )
    state["genome_length_max_kbp"] = parse_optional_float(
        source.get("genome_length_max_kbp") if hasattr(source, "get") else None
    )
    state["contig_count_max"] = parse_optional_int(source.get("contig_count_max") if hasattr(source, "get") else None)
    state["scaffold_count_max"] = parse_optional_int(
        source.get("scaffold_count_max") if hasattr(source, "get") else None
    )
    return state


def build_sequence_filter_option_list(frame: pd.DataFrame, column: str, *, limit: int = 250) -> list[dict[str, Any]]:
    if column not in frame.columns:
        return []
    series = frame[column].astype(str).str.strip()
    series = series[
        series.ne("")
        & ~series.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing", "none", "nan"})
    ]
    counts = series.value_counts().head(limit)
    return [{"value": str(index), "count": int(value)} for index, value in counts.items()]


def sequence_filter_hidden_inputs(filters: dict[str, Any]) -> list[tuple[str, str]]:
    hidden_inputs: list[tuple[str, str]] = []
    hidden_inputs.append(("filter_logic", str(filters.get("filter_logic", "and"))))
    for key in SEQUENCE_FILTER_FIELDS:
        for value in filters.get(key, []):
            hidden_inputs.append((key, str(value)))
    for key in [
        "checkm_min",
        "checkm_max",
        "contamination_max",
        "year_from",
        "year_to",
        "genome_length_min_kbp",
        "genome_length_max_kbp",
        "contig_count_max",
        "scaffold_count_max",
    ]:
        value = filters.get(key)
        if value is not None and value != "":
            hidden_inputs.append((key, str(value)))
    return hidden_inputs


def normalize_sequence_group_field(value: Any) -> str:
    text = normalize_metadata_value(value)
    allowed = {"country", "host", "isolation_source", "assembly_level"}
    return text if text in allowed else "country"


def sequence_group_column(group_field: str) -> str:
    return {
        "country": "Country",
        "host": "Host",
        "isolation_source": "Isolation Source",
        "assembly_level": "Assembly Level",
    }.get(group_field, "Country")


def sequence_group_slug(value: Any) -> str:
    text = normalize_metadata_value(value)
    if not text:
        return "unassigned"
    slug = secure_filename(text)
    return slug or "unassigned"


def join_human_list(values: list[str]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} or {values[1]}"
    return f"{', '.join(values[:-1])}, or {values[-1]}"


def apply_sequence_filters(frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame.copy()
    logic_mode = filters.get("filter_logic", "and")
    predicate_masks: list[pd.Series] = []

    for key, config in SEQUENCE_FILTER_FIELDS.items():
        selected = filters.get(key) or []
        if not selected:
            continue
        column = config["column"]
        if column not in filtered.columns:
            predicate_masks.append(pd.Series(False, index=filtered.index))
            continue
        values = filtered[column].astype(str).str.strip()
        predicate_masks.append(values.isin(selected))

    checkm_min = filters.get("checkm_min")
    checkm_max = filters.get("checkm_max")
    if checkm_min is not None or checkm_max is not None:
        completeness = pd.to_numeric(
            filtered.get("CheckM completeness", pd.Series(index=filtered.index, dtype="float64")),
            errors="coerce",
        )
        range_mask = completeness.notna()
        if checkm_min is not None:
            range_mask &= completeness >= checkm_min
        if checkm_max is not None:
            range_mask &= completeness <= checkm_max
        predicate_masks.append(range_mask)

    contamination_max = filters.get("contamination_max")
    if contamination_max is not None:
        contamination = pd.to_numeric(
            filtered.get("CheckM contamination", pd.Series(index=filtered.index, dtype="float64")),
            errors="coerce",
        )
        predicate_masks.append(contamination.notna() & (contamination <= contamination_max))

    year_from = filters.get("year_from")
    year_to = filters.get("year_to")
    if year_from is not None or year_to is not None:
        years = pd.to_numeric(
            filtered.get("Collection Date", pd.Series(index=filtered.index, dtype="object"))
            .astype(str)
            .str.extract(r"((?:19|20)\d{2})")[0],
            errors="coerce",
        )
        year_mask = years.notna()
        if year_from is not None:
            year_mask &= years >= year_from
        if year_to is not None:
            year_mask &= years <= year_to
        predicate_masks.append(year_mask)

    length_min_kbp = filters.get("genome_length_min_kbp")
    length_max_kbp = filters.get("genome_length_max_kbp")
    if length_min_kbp is not None or length_max_kbp is not None:
        lengths = pd.to_numeric(
            filtered.get("Assembly Stats Total Sequence Length", pd.Series(index=filtered.index, dtype="float64")),
            errors="coerce",
        )
        length_mask = lengths.notna()
        if length_min_kbp is not None:
            length_mask &= lengths >= (length_min_kbp * 1000.0)
        if length_max_kbp is not None:
            length_mask &= lengths <= (length_max_kbp * 1000.0)
        predicate_masks.append(length_mask)

    contig_count_max = filters.get("contig_count_max")
    if contig_count_max is not None:
        contigs = pd.to_numeric(
            filtered.get("Assembly Stats Number of Contigs", pd.Series(index=filtered.index, dtype="float64")),
            errors="coerce",
        )
        predicate_masks.append(contigs.notna() & (contigs <= contig_count_max))

    scaffold_count_max = filters.get("scaffold_count_max")
    if scaffold_count_max is not None:
        scaffolds = pd.to_numeric(
            filtered.get("Assembly Stats Number of Scaffolds", pd.Series(index=filtered.index, dtype="float64")),
            errors="coerce",
        )
        predicate_masks.append(scaffolds.notna() & (scaffolds <= scaffold_count_max))

    if not predicate_masks:
        return filtered.copy()

    if logic_mode == "or":
        mask = pd.Series(False, index=filtered.index)
        for predicate in predicate_masks:
            mask |= predicate
    else:
        mask = pd.Series(True, index=filtered.index)
        for predicate in predicate_masks:
            mask &= predicate

    return filtered.loc[mask].copy()


def build_sequence_active_filter_summary(filters: dict[str, Any]) -> list[str]:
    parts: list[str] = []
    for key, config in SEQUENCE_FILTER_FIELDS.items():
        values = filters.get(key) or []
        if values:
            label = config["label"]
            if len(values) == 1:
                parts.append(f"{label}: {values[0]}")
            else:
                parts.append(f"{label}: {len(values)} selected")
    if filters.get("checkm_min") is not None or filters.get("checkm_max") is not None:
        low = filters.get("checkm_min")
        high = filters.get("checkm_max")
        if low is not None and high is not None:
            parts.append(f"CheckM: {low:g}-{high:g}")
        elif low is not None:
            parts.append(f"CheckM: >= {low:g}")
        elif high is not None:
            parts.append(f"CheckM: <= {high:g}")
    if filters.get("contamination_max") is not None:
        parts.append(f"Contamination: <= {filters['contamination_max']:g}")
    if filters.get("year_from") is not None or filters.get("year_to") is not None:
        low = filters.get("year_from")
        high = filters.get("year_to")
        if low is not None and high is not None:
            parts.append(f"Year: {low}-{high}")
        elif low is not None:
            parts.append(f"Year: >= {low}")
        elif high is not None:
            parts.append(f"Year: <= {high}")
    if filters.get("genome_length_min_kbp") is not None or filters.get("genome_length_max_kbp") is not None:
        low = filters.get("genome_length_min_kbp")
        high = filters.get("genome_length_max_kbp")
        if low is not None and high is not None:
            parts.append(f"Length: {low:g}-{high:g} Kbp")
        elif low is not None:
            parts.append(f"Length: >= {low:g} Kbp")
        elif high is not None:
            parts.append(f"Length: <= {high:g} Kbp")
    if filters.get("contig_count_max") is not None:
        parts.append(f"Contigs: <= {filters['contig_count_max']}")
    if filters.get("scaffold_count_max") is not None:
        parts.append(f"Scaffolds: <= {filters['scaffold_count_max']}")
    return parts


def build_sequence_filter_sentence(species: SpeciesRecord, filters: dict[str, Any]) -> str:
    parts: list[str] = []
    for key, config in SEQUENCE_FILTER_FIELDS.items():
        values = filters.get(key) or []
        if values:
            parts.append(f"{config['label']} is {join_human_list(values)}")
    if filters.get("checkm_min") is not None or filters.get("checkm_max") is not None:
        low = filters.get("checkm_min")
        high = filters.get("checkm_max")
        if low is not None and high is not None:
            parts.append(f"CheckM completeness is between {low:g} and {high:g}")
        elif low is not None:
            parts.append(f"CheckM completeness is at least {low:g}")
        elif high is not None:
            parts.append(f"CheckM completeness is at most {high:g}")
    if filters.get("contamination_max") is not None:
        parts.append(f"CheckM contamination is at most {filters['contamination_max']:g}")
    if filters.get("year_from") is not None or filters.get("year_to") is not None:
        low = filters.get("year_from")
        high = filters.get("year_to")
        if low is not None and high is not None:
            parts.append(f"Collection year is between {low} and {high}")
        elif low is not None:
            parts.append(f"Collection year is {low} or later")
        elif high is not None:
            parts.append(f"Collection year is {high} or earlier")
    if filters.get("genome_length_min_kbp") is not None or filters.get("genome_length_max_kbp") is not None:
        low = filters.get("genome_length_min_kbp")
        high = filters.get("genome_length_max_kbp")
        if low is not None and high is not None:
            parts.append(f"Genome length is between {low:g} and {high:g} Kbp")
        elif low is not None:
            parts.append(f"Genome length is at least {low:g} Kbp")
        elif high is not None:
            parts.append(f"Genome length is at most {high:g} Kbp")
    if filters.get("contig_count_max") is not None:
        parts.append(f"Contig count is at most {filters['contig_count_max']}")
    if filters.get("scaffold_count_max") is not None:
        parts.append(f"Scaffold count is at most {filters['scaffold_count_max']}")
    if not parts:
        return f"Showing all genomes currently represented in the stored metadata for {species.species_name}."
    if len(parts) == 1:
        joined = parts[0]
    elif filters.get("filter_logic") == "or":
        joined = join_human_list(parts)
    else:
        joined = ", ".join(parts[:-1]) + f", and {parts[-1]}"
    return f"Matching genomes where {joined}."


def build_sequence_filter_groups(frame: pd.DataFrame, species: SpeciesRecord) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for group in SEQUENCE_FILTER_GROUPS:
        if group["key"] == "species_diversity" and species.taxon_rank != "genus":
            continue
        fields: list[dict[str, Any]] = []
        for key in group["fields"]:
            config = SEQUENCE_FILTER_FIELDS[key]
            fields.append(
                {
                    "key": key,
                    "label": config["label"],
                    "column": config["column"],
                    "options": build_sequence_filter_option_list(frame, config["column"]),
                }
            )
        groups.append({"key": group["key"], "label": group["label"], "fields": fields})
    return groups


def metadata_sections_for_species(species: SpeciesRecord) -> dict[str, dict[str, str]]:
    if species.taxon_rank == "genus":
        return METADATA_SECTIONS
    return {key: value for key, value in METADATA_SECTIONS.items() if key != "species_diversity"}


def species_value_counts(frame: pd.DataFrame, *, limit: int | None = None) -> list[tuple[str, int]]:
    if "Organism Name" not in frame.columns:
        return []
    series = frame["Organism Name"].astype(str).str.strip().map(normalize_species_name)
    series = series[
        series.ne("")
        & ~series.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing", "none", "nan"})
    ]
    counts = series.value_counts()
    if limit is not None:
        counts = counts.head(limit)
    return [(str(index), int(value)) for index, value in counts.items()]


def build_species_diversity_summary(frame: pd.DataFrame) -> dict[str, Any]:
    counts = species_value_counts(frame)
    if not counts:
        return {
            "distinct_species_total": 0,
            "singleton_species_total": 0,
            "dominant_species": None,
            "dominant_species_count": 0,
            "top_five_share_percent": 0.0,
            "median_genomes_per_species": 0.0,
            "species_counts": [],
        }
    values = [count for _, count in counts]
    total = sum(values)
    dominant_species, dominant_count = counts[0]
    top_five_share = (sum(values[:5]) / total) * 100 if total else 0.0
    singleton_total = sum(1 for value in values if value == 1)
    return {
        "distinct_species_total": len(counts),
        "singleton_species_total": singleton_total,
        "dominant_species": dominant_species,
        "dominant_species_count": dominant_count,
        "top_five_share_percent": round(top_five_share, 1),
        "median_genomes_per_species": round(statistics.median(values), 1),
        "species_counts": counts,
    }


def build_taxon_sequence_dashboard(species: SpeciesRecord, source: Any | None = None) -> dict[str, Any]:
    rows, fieldnames, frame = load_taxon_metadata_dataset(species)
    filters = build_sequence_filter_state(source or {})
    filtered_frame = apply_sequence_filters(frame, filters)
    filtered_rows = [
        {column: normalize_metadata_value(value) for column, value in row.items()}
        for row in filtered_frame.fillna("").to_dict(orient="records")
    ]
    preview_columns = [column for column in SEQUENCE_PREVIEW_COLUMNS if column in filtered_frame.columns]
    preview_rows = [
        {column: row.get(column, "") for column in preview_columns}
        for row in filtered_rows[:100]
    ]
    filtered_year_start, filtered_year_end = summarize_year_span(filtered_rows)
    matched_total = len(filtered_rows)
    total_rows = len(rows)
    filter_groups = build_sequence_filter_groups(frame, species)
    filter_field_map = {
        field["key"]: field
        for group in filter_groups
        for field in group["fields"]
    }
    primary_filter_keys = [
        "species_name",
        "continent",
        "subcontinent",
        "country",
        "host",
        "host_disease",
        "isolation_source",
        "sample_type",
        "environment_broad",
        "environment_local",
        "environment_medium",
        "assembly_level",
    ]
    primary_filter_keys = [key for key in primary_filter_keys if key in filter_field_map]
    advanced_filter_keys = []
    sequence_lengths = metadata_numeric_series(filtered_frame, "Assembly Stats Total Sequence Length")
    estimated_total_bp = int(sequence_lengths.sum()) if not sequence_lengths.empty else 0
    estimated_compressed_bytes = int(estimated_total_bp * 0.32) if estimated_total_bp else 0
    mean_length_bp = int(sequence_lengths.mean()) if not sequence_lengths.empty else 0
    active_filters_summary = build_sequence_active_filter_summary(filters)
    return {
        "row_total": total_rows,
        "matched_row_total": matched_total,
        "match_percent": round((matched_total / total_rows) * 100, 1) if total_rows else 0.0,
        "filter_logic": filters.get("filter_logic", "and"),
        "filter_logic_label": "Match all filters" if filters.get("filter_logic", "and") == "and" else "Match any filter",
        "active_filters_summary": active_filters_summary,
        "active_filters_label": " | ".join(active_filters_summary) if active_filters_summary else "None",
        "distinct_country_count": metadata_distinct_count(filtered_frame, "Country"),
        "distinct_host_count": metadata_distinct_count(filtered_frame, "Host"),
        "distinct_isolation_source_count": metadata_distinct_count(filtered_frame, "Isolation Source"),
        "year_start": filtered_year_start,
        "year_end": filtered_year_end,
        "estimated_total_bp": estimated_total_bp,
        "estimated_total_bp_label": format_sequence_length(float(estimated_total_bp)) if estimated_total_bp else "Unknown",
        "estimated_total_bytes_label": format_bytes(estimated_total_bp) if estimated_total_bp else "Unknown",
        "estimated_compressed_bytes": estimated_compressed_bytes,
        "estimated_compressed_bytes_label": format_bytes(estimated_compressed_bytes) if estimated_compressed_bytes else "Unknown",
        "mean_length_bp": mean_length_bp,
        "mean_length_label": format_sequence_length(float(mean_length_bp)) if mean_length_bp else "Unknown",
        "top_countries": summarize_top_values(filtered_rows, "Country"),
        "top_hosts": summarize_top_values(filtered_rows, "Host"),
        "top_sources": summarize_top_values(filtered_rows, "Isolation Source"),
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
        "filter_sentence": build_sequence_filter_sentence(species, filters),
        "filters": filters,
        "filter_groups": filter_groups,
        "filter_field_map": filter_field_map,
        "primary_filter_keys": primary_filter_keys,
        "advanced_filter_keys": advanced_filter_keys,
        "filter_hidden_inputs": sequence_filter_hidden_inputs(filters),
        "active_filter_total": len(sequence_filter_hidden_inputs(filters)),
        "has_filters": bool(sequence_filter_hidden_inputs(filters)),
        "fieldnames": fieldnames,
        "filtered_frame": filtered_frame,
    }


def build_summary_paragraphs(
    species: SpeciesRecord,
    analysis: dict[str, Any],
) -> list[str]:
    paragraphs: list[str] = []
    row_total = int(analysis["row_total"])
    year_text = (
        f"from {analysis['year_start']} to {analysis['year_end']}"
        if analysis["year_start"] and analysis["year_end"]
        else "across an unresolved collection-year range"
    )
    paragraphs.append(
        f"For the selected {species.taxon_rank}, FetchM Web currently summarizes {row_total:,} genomes "
        f"using the stored managed metadata artifact, spanning {year_text}. The active dataset exposes "
        f"{analysis['column_total']} metadata columns, providing sufficient breadth to examine sampling context, "
        f"geographic structure, host association, and genome-quality composition within a single analytical view."
    )

    top_countries = analysis.get("top_countries") or []
    top_continents = analysis.get("top_continents") or []
    top_hosts = analysis.get("top_hosts") or []
    top_sources = analysis.get("top_sources") or []
    species_diversity = analysis.get("species_diversity") or {}
    if top_countries:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_countries[:3])
        paragraphs.append(
            f"Moreover, geographic representation is presently led by {rendered}, indicating that sampling density is concentrated "
            f"in a limited set of dominant locations rather than being evenly distributed across the catalog."
        )
    if top_continents:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_continents[:3])
        paragraphs.append(
            f"At the continental scale, the strongest signal is observed in {rendered}, providing a higher-level view "
            f"of how the current assembly collection is distributed across broad geographic regions."
        )
    if top_hosts:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_hosts[:3])
        paragraphs.append(
            f"In parallel, host annotations are dominated by {rendered}, highlighting the principal biological contexts represented "
            f"in the stored dataset and defining the clearest axes for host-associated comparison."
        )
    if top_sources:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_sources[:3])
        paragraphs.append(
            f"Likewise, isolation-source metadata most frequently records {rendered}, showing that the current assembly set is "
            f"anchored around a relatively small number of recurrent sampling origins."
        )
    if species.taxon_rank == "genus" and species_diversity.get("distinct_species_total"):
        dominant_species = species_diversity.get("dominant_species")
        dominant_species_count = species_diversity.get("dominant_species_count") or 0
        top_five_share_percent = species_diversity.get("top_five_share_percent") or 0.0
        singleton_species_total = species_diversity.get("singleton_species_total") or 0
        median_genomes_per_species = species_diversity.get("median_genomes_per_species") or 0.0
        paragraphs.append(
            f"At the species-diversity level, this genus currently spans {species_diversity['distinct_species_total']} represented species, "
            f"with a median of {median_genomes_per_species:g} genomes per species. "
            f"{dominant_species or 'The dominant species'} contributes {dominant_species_count:,} genomes, while the top five species together account for "
            f"{top_five_share_percent}% of the current genus-wide collection."
        )
        paragraphs.append(
            f"This structure also includes a substantial low-abundance tail, with {singleton_species_total} species represented by only a single genome. "
            f"Accordingly, the current genus summary reflects both a concentrated core of well-represented species and a broader set of sparsely sampled taxa "
            f"that may still be informative for diversity-aware downstream comparisons."
        )

    completeness = analysis.get("completeness")
    contamination = analysis.get("contamination")
    genome_length = analysis.get("genome_length")
    quality_bands = analysis.get("quality_bands") or []
    if completeness or contamination or genome_length:
        fragments: list[str] = []
        if genome_length:
            fragments.append(
                f"mean genome length was {format_sequence_length(genome_length['mean'])}"
            )
        if completeness:
            fragments.append(
                f"mean CheckM completeness was {completeness['mean']:.2f}"
            )
        if contamination:
            fragments.append(
                f"mean CheckM contamination was {contamination['mean']:.2f}"
            )
        paragraphs.append(
            "Taken together, genome-quality profiling showed that " + ", ".join(fragments) + "."
        )
    if quality_bands:
        dominant_band = max(quality_bands, key=lambda item: item["count"])
        paragraphs.append(
            f"Finally, quality-band analysis indicates that the dataset is currently dominated by the "
            f"'{dominant_band['label']}' class, which accounts for {dominant_band['percent']}% of genomes and "
            f"therefore sets the prevailing quality baseline for downstream interpretation."
        )

    return paragraphs


def make_plot_html(figure: Any, *, include_js: bool = False) -> str:
    return pio.to_html(
        figure,
        full_html=False,
        include_plotlyjs="cdn",
        config={"displayModeBar": False, "responsive": True},
    )


def style_figure(figure: Any) -> Any:
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.88)",
        font={"family": "Georgia, Times New Roman, serif", "size": 14, "color": "#1f2a1f"},
        title={"font": {"size": 20, "family": "Georgia, Times New Roman, serif"}},
        margin={"l": 24, "r": 24, "t": 72, "b": 28},
        coloraxis_colorbar={"title": "", "len": 0.8},
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(31,42,31,0.08)", zeroline=False)
    figure.update_yaxes(showgrid=False, zeroline=False)
    return figure


def add_temporal_overlay(figure: Any, frame: pd.DataFrame, *, x_field: str, y_field: str, color: str = "#184f46") -> Any:
    if len(frame) < 5 or frame[x_field].nunique() < 2 or frame[y_field].nunique() < 2:
        return figure
    try:
        pearson = pearsonr(frame[x_field], frame[y_field])
        spearman = spearmanr(frame[x_field], frame[y_field])
    except Exception:
        return figure
    if pd.isna(pearson.statistic) or pd.isna(spearman.statistic):
        return figure
    coefficients = pd.Series(frame[y_field]).astype(float)
    fit = pd.Series(frame[x_field]).astype(float)
    slope, intercept = np.polyfit(fit, coefficients, 1)
    x_min = float(fit.min())
    x_max = float(fit.max())
    trend_x = [x_min, x_max]
    trend_y = [slope * x_min + intercept, slope * x_max + intercept]
    figure.add_trace(
        go.Scatter(
            x=trend_x,
            y=trend_y,
            mode="lines",
            line={"color": color, "width": 3, "dash": "dash"},
            name="Trend",
            showlegend=False,
        )
    )
    figure.add_annotation(
        xref="paper",
        yref="paper",
        x=0.99,
        y=1.12,
        xanchor="right",
        yanchor="bottom",
        showarrow=False,
        text=(
            f"Pearson r={pearson.statistic:.2f} (p={pearson.pvalue:.2g})"
            f"<br>Spearman ρ={spearman.statistic:.2f} (p={spearman.pvalue:.2g})"
        ),
        font={"size": 19, "color": "#36504a"},
        bgcolor="rgba(255,250,242,0.9)",
        bordercolor="rgba(31,42,31,0.08)",
        borderwidth=1,
        borderpad=6,
    )
    return figure


def build_plot_bundle(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    plots: dict[str, dict[str, Any]] = {}

    def add_plot(key: str, figure: Any) -> None:
        styled = style_figure(figure)
        plots[key] = {
            "html": make_plot_html(styled, include_js=True),
            "figure": styled,
        }

    species_counts = species_value_counts(frame)
    if species_counts:
        top_species = species_counts[:20]
        species_frame = pd.DataFrame(top_species, columns=["Species", "Genomes"])
        if len(species_counts) > 20:
            other_total = sum(count for _, count in species_counts[20:])
            species_frame = pd.concat(
                [species_frame, pd.DataFrame([{"Species": "Other", "Genomes": other_total}])],
                ignore_index=True,
            )
        add_plot(
            "species_diversity_bar",
            px.bar(
                species_frame,
                x="Genomes",
                y="Species",
                orientation="h",
                title="Species diversity across the current genus",
                color="Genomes",
                color_continuous_scale="Tealgrn",
                text="Genomes",
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin={"l": 20, "r": 20, "t": 60, "b": 20},
            ),
        )
        cumulative = pd.DataFrame(top_species, columns=["Species", "Genomes"])
        cumulative["Rank"] = range(1, len(cumulative) + 1)
        cumulative["Cumulative share"] = (cumulative["Genomes"].cumsum() / sum(count for _, count in species_counts)) * 100
        add_plot(
            "species_cumulative_line",
            px.line(
                cumulative,
                x="Rank",
                y="Cumulative share",
                markers=True,
                title="Cumulative genus coverage by ranked species",
                labels={"Cumulative share": "Cumulative genome share (%)"},
            ).update_traces(line={"color": "#165c4e", "width": 4}, marker={"size": 8}).update_layout(
                margin={"l": 20, "r": 20, "t": 60, "b": 20}
            ),
        )

    if "Country" in frame.columns:
        countries = (
            frame["Country"]
            .astype(str)
            .str.strip()
        )
        countries = countries[
            countries.ne("")
            & ~countries.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing"})
        ]
        if not countries.empty:
            country_counts = countries.value_counts().head(15).reset_index()
            country_counts.columns = ["Country", "Genomes"]
            add_plot(
                "country_bar",
                px.bar(
                    country_counts,
                    x="Country",
                    y="Genomes",
                    title="Top countries represented",
                    color="Genomes",
                    color_continuous_scale="Tealgrn",
                    text="Genomes",
                ).update_traces(textposition="outside", cliponaxis=False).update_layout(
                    xaxis={"categoryorder": "total descending", "tickangle": -55},
                    margin={"l": 20, "r": 20, "t": 60, "b": 110},
                ),
            )
            add_plot(
                "geography_map",
                px.choropleth(
                    country_counts,
                    locations="Country",
                    locationmode="country names",
                    color="Genomes",
                    color_continuous_scale=[
                        (0.0, "#f8f4ea"),
                        (0.2, "#d6e7d0"),
                        (0.45, "#79b49e"),
                        (0.7, "#2f7a69"),
                        (1.0, "#173f45"),
                    ],
                    title="Geographic distribution",
                ).update_layout(margin={"l": 0, "r": 0, "t": 60, "b": 0}),
            )

    for key, field, title, scale in [
        ("continent_bar", "Continent", "Geographic distribution by continent", "Tealgrn"),
        ("subcontinent_bar", "Subcontinent", "Geographic distribution by subcontinent", "Viridis"),
    ]:
        if field not in frame.columns:
            continue
        values = frame[field].astype(str).str.strip()
        values = values[
            values.ne("")
            & ~values.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing"})
        ]
        if values.empty:
            continue
        counts = values.value_counts().head(15).reset_index()
        counts.columns = [field, "Genomes"]
        add_plot(
            key,
            px.bar(
                counts,
                x="Genomes",
                y=field,
                orientation="h",
                title=title,
                color="Genomes",
                color_continuous_scale=scale,
                text="Genomes",
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(yaxis={"categoryorder": "total ascending"}, margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    if "Host" in frame.columns:
        host_counts = (
            frame["Host"].astype(str).str.strip()
        )
        host_counts = host_counts[
            host_counts.ne("")
            & ~host_counts.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing"})
        ]
        if not host_counts.empty:
            host_counts = host_counts.value_counts().head(12).reset_index()
            host_counts.columns = ["Host", "Genomes"]
            add_plot(
                "host_bar",
            px.bar(
                host_counts,
                x="Genomes",
                y="Host",
                orientation="h",
                title="Top host annotations",
                color="Genomes",
                color_continuous_scale="Sunsetdark",
                text="Genomes",
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(yaxis={"categoryorder": "total ascending"}, margin={"l": 20, "r": 20, "t": 60, "b": 20}),
            )

    for key, field, title, scale in [
        ("source_bar", "Isolation Source", "Top isolation sources", "Mint"),
        ("environment_bar", "Environment (Broad Scale)", "Broad environmental contexts", "Teal"),
        ("assembly_level_bar", "Assembly Level", "Assembly level distribution", "Blues"),
        ("host_disease_bar", "Host Disease", "Host disease distribution", "Sunset"),
        ("sample_type_bar", "Sample Type", "Sample type distribution", "Burg"),
        ("environment_local_bar", "Environment (Local Scale)", "Local environmental contexts", "Emrld"),
        ("environment_medium_bar", "Environment Medium", "Environmental medium distribution", "Darkmint"),
    ]:
        if field not in frame.columns:
            continue
        values = frame[field].astype(str).str.strip()
        values = values[
            values.ne("")
            & ~values.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing"})
        ]
        if values.empty:
            continue
        counts = values.value_counts().head(12).reset_index()
        counts.columns = [field, "Genomes"]
        add_plot(
            key,
            px.bar(
                counts,
                x="Genomes",
                y=field,
                orientation="h",
                title=title,
                color="Genomes",
                color_continuous_scale=scale,
                text="Genomes",
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(yaxis={"categoryorder": "total ascending"}, margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    completeness = metadata_numeric_series(frame, "CheckM completeness")
    if not completeness.empty:
        add_plot(
            "completeness_hist",
                px.histogram(
                    x=completeness,
                    nbins=30,
                    title="CheckM completeness distribution",
                    labels={"x": "CheckM completeness", "y": "Genomes"},
                    color_discrete_sequence=["#165c4e"],
            ).update_traces(texttemplate="%{y}", textposition="outside").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    genome_length = metadata_numeric_series(frame, "Assembly Stats Total Sequence Length")
    if not genome_length.empty:
        add_plot(
            "genome_length_hist",
                px.histogram(
                    x=genome_length,
                    nbins=30,
                    title="Genome length distribution",
                    labels={"x": "Assembly sequence length", "y": "Genomes"},
                    color_discrete_sequence=["#b56a11"],
            ).update_traces(texttemplate="%{y}", textposition="outside").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    for key, field, title, color in [
        ("contig_hist", "Assembly Stats Number of Contigs", "Contig count distribution", "#5b4b8a"),
        ("scaffold_hist", "Assembly Stats Number of Scaffolds", "Scaffold count distribution", "#2b7a78"),
    ]:
        values = metadata_numeric_series(frame, field)
        if values.empty:
            continue
        add_plot(
            key,
            px.histogram(
                x=values,
                nbins=30,
                title=title,
                labels={"x": field, "y": "Genomes"},
                color_discrete_sequence=[color],
            ).update_traces(texttemplate="%{y}", textposition="outside").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    if "Collection Date" in frame.columns:
        years = pd.to_numeric(frame["Collection Date"].astype(str).str.extract(r"((?:19|20)\d{2})")[0], errors="coerce").dropna()
        if not years.empty:
            add_plot(
                "collection_year_hist",
                px.histogram(
                    x=years,
                    nbins=min(30, max(10, int(years.nunique()))),
                    title="Collection year distribution",
                    labels={"x": "Collection year", "y": "Genomes"},
                    color_discrete_sequence=["#2b6f62"],
                ).update_traces(texttemplate="%{y}", textposition="outside").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}),
            )

            year_counts = years.value_counts().sort_index().reset_index()
            year_counts.columns = ["Year", "Genomes"]
            add_plot(
                "temporal_count_line",
                add_temporal_overlay(px.line(
                    year_counts,
                    x="Year",
                    y="Genomes",
                    markers=True,
                    title="Genome counts through collection time",
                ).update_traces(line={"color": "#165c4e", "width": 4}, marker={"size": 9}, text=year_counts["Genomes"], textposition="top center").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}), year_counts, x_field="Year", y_field="Genomes", color="#0e3f3c"),
            )

    if "Collection Date" in frame.columns and "Assembly Stats Total Sequence Length" in frame.columns:
        years = pd.to_numeric(frame["Collection Date"].astype(str).str.extract(r"((?:19|20)\d{2})")[0], errors="coerce")
        lengths = pd.to_numeric(frame["Assembly Stats Total Sequence Length"], errors="coerce")
        trend_frame = pd.DataFrame({"Year": years, "Genome length": lengths}).dropna()
        if len(trend_frame) >= 5:
            add_plot(
                "year_length_scatter",
                add_temporal_overlay(px.scatter(
                    trend_frame,
                    x="Year",
                    y="Genome length",
                    title="Genome length over collection time",
                    color_discrete_sequence=["#7b2d1d"],
                    opacity=0.7,
                ).update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}), trend_frame, x_field="Year", y_field="Genome length", color="#8a3e22"),
            )

    for key, field, title, color in [
        ("year_gene_total_scatter", "Annotation Count Gene Total", "Annotated gene totals over collection time", "#8e2f1d"),
        ("year_gene_protein_scatter", "Annotation Count Gene Protein-coding", "Protein-coding genes over collection time", "#5f6b1f"),
        ("year_gene_pseudo_scatter", "Annotation Count Gene Pseudogene", "Pseudogenes over collection time", "#6a3dc4"),
    ]:
        if "Collection Date" not in frame.columns or field not in frame.columns:
            continue
        years = pd.to_numeric(frame["Collection Date"].astype(str).str.extract(r"((?:19|20)\d{2})")[0], errors="coerce")
        metric = pd.to_numeric(frame[field], errors="coerce")
        trend_frame = pd.DataFrame({"Year": years, field: metric}).dropna()
        if len(trend_frame) >= 5:
            add_plot(
                key,
                add_temporal_overlay(px.scatter(
                    trend_frame,
                    x="Year",
                    y=field,
                    title=title,
                    color_discrete_sequence=[color],
                    opacity=0.72,
                ).update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}), trend_frame, x_field="Year", y_field=field, color=color),
            )

    for key, field, title, color in [
        ("gene_total_hist", "Annotation Count Gene Total", "Annotated gene count distribution", "#8e2f1d"),
        ("gene_protein_hist", "Annotation Count Gene Protein-coding", "Protein-coding gene distribution", "#5f6b1f"),
        ("gene_pseudo_hist", "Annotation Count Gene Pseudogene", "Pseudogene distribution", "#6a3dc4"),
    ]:
        values = metadata_numeric_series(frame, field)
        if values.empty:
            continue
        add_plot(
            key,
            px.histogram(
                x=values,
                nbins=30,
                title=title,
                labels={"x": field, "y": "Genomes"},
                color_discrete_sequence=[color],
            ).update_traces(texttemplate="%{y}", textposition="outside").update_layout(margin={"l": 20, "r": 20, "t": 60, "b": 20}),
        )

    return plots


def build_analysis_bundle(species: SpeciesRecord, analysis: dict[str, Any]) -> bytes:
    clean_path = Path(species.metadata_clean_path or "")
    bundle = BytesIO()
    rows, _, frame = load_taxon_metadata_dataset(species)
    plots = build_plot_bundle(frame)
    summary_text = "\n\n".join(build_summary_paragraphs(species, analysis) + analysis.get("insights", []))
    summary_html = (
        "<html><head><meta charset='utf-8'><title>FetchM Web Summary</title>"
        "<style>body{font-family:Georgia,'Times New Roman',serif;max-width:980px;margin:40px auto;padding:0 24px;"
        "color:#1f2a1f;background:#fffaf2;}h1{font-size:2.2rem;}p{line-height:1.75;font-size:1.05rem;}"
        ".meta{color:#5f6a5f;font-size:0.95rem;margin-bottom:28px;}</style></head><body>"
        f"<h1>{species.species_name} Metadata Summary</h1>"
        f"<p class='meta'>{species.taxon_rank.capitalize()} · {analysis['row_total']:,} genomes · "
        f"{analysis['column_total']} metadata columns</p>"
        + "".join(f"<p>{paragraph}</p>" for paragraph in (analysis.get("summary_paragraphs") or []) + (analysis.get("insights") or []))
        + "</body></html>"
    )
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if clean_path.exists():
            archive.write(clean_path, arcname=f"{species.slug}/metadata/{clean_path.name}")
        archive.writestr(f"{species.slug}/reports/summary.txt", summary_text)
        archive.writestr(f"{species.slug}/reports/summary.html", summary_html)
        for plot_name, plot_payload in plots.items():
            archive.writestr(f"{species.slug}/figures/{plot_name}.html", plot_payload["html"])
            try:
                image_bytes = pio.to_image(plot_payload["figure"], format="png", width=1600, height=1000, scale=2)
            except Exception:
                image_bytes = None
            if image_bytes:
                archive.writestr(f"{species.slug}/figures/{plot_name}.png", image_bytes)
    bundle.seek(0)
    return bundle.getvalue()


def load_taxon_metadata_analysis(species: SpeciesRecord) -> dict[str, Any]:
    rows, fieldnames, frame = load_taxon_metadata_dataset(species)
    species_diversity = build_species_diversity_summary(frame)
    genome_lengths = [float(row["Assembly Stats Total Sequence Length"]) for row in rows if normalize_metadata_value(row.get("Assembly Stats Total Sequence Length")).replace(".", "", 1).isdigit()]
    completeness_values = [float(row["CheckM completeness"]) for row in rows if normalize_metadata_value(row.get("CheckM completeness")).replace(".", "", 1).isdigit()]
    contamination_values = [float(row["CheckM contamination"]) for row in rows if normalize_metadata_value(row.get("CheckM contamination")).replace(".", "", 1).isdigit()]
    contig_values = [float(row["Assembly Stats Number of Contigs"]) for row in rows if normalize_metadata_value(row.get("Assembly Stats Number of Contigs")).replace(".", "", 1).isdigit()]
    scaffold_values = [float(row["Assembly Stats Number of Scaffolds"]) for row in rows if normalize_metadata_value(row.get("Assembly Stats Number of Scaffolds")).replace(".", "", 1).isdigit()]

    core_coverage_fields = [
        "Host",
        "Geographic Location",
        "Country",
        "Continent",
        "Subcontinent",
        "Isolation Source",
        "Collection Date",
        "Environment (Broad Scale)",
        "Environment (Local Scale)",
        "Environment Medium",
        "Assembly Level",
        "Assembly Stats Number of Contigs",
    ]
    coverage = []
    total_rows = len(rows)
    completeness_rows: list[dict[str, Any]] = []
    for field in core_coverage_fields:
        present = sum(1 for row in rows if is_meaningful_metadata_value(row.get(field)))
        completeness_row = metadata_present_unknown_absent(frame, field)
        completeness_rows.append(completeness_row)
        coverage.append(
            {
                "field": field,
                "present": present,
                "missing": max(0, total_rows - present),
                "percent": round((present / total_rows) * 100, 1) if total_rows else 0.0,
            }
        )

    year_start, year_end = summarize_year_span(rows)
    summary_paragraphs = build_summary_paragraphs(species, {
        "row_total": total_rows,
        "column_total": len(fieldnames),
        "year_start": year_start,
        "year_end": year_end,
        "top_countries": summarize_top_values(rows, "Country"),
        "top_continents": metadata_value_counts(frame, "Continent"),
        "top_hosts": summarize_top_values(rows, "Host"),
        "top_sources": summarize_top_values(rows, "Isolation Source"),
        "genome_length": numeric_summary(genome_lengths),
        "completeness": numeric_summary(completeness_values),
        "contamination": numeric_summary(contamination_values),
        "quality_bands": build_quality_bands(frame),
        "species_diversity": species_diversity,
    })
    return {
        "row_total": total_rows,
        "column_total": len(fieldnames),
        "fieldnames": fieldnames,
        "year_start": year_start,
        "year_end": year_end,
        "distinct_host_count": metadata_distinct_count(frame, "Host"),
        "distinct_country_count": metadata_distinct_count(frame, "Country"),
        "distinct_continent_count": metadata_distinct_count(frame, "Continent"),
        "distinct_species_count": species_diversity["distinct_species_total"],
        "genome_length": numeric_summary(genome_lengths),
        "completeness": numeric_summary(completeness_values),
        "contamination": numeric_summary(contamination_values),
        "contigs": numeric_summary(contig_values),
        "scaffolds": numeric_summary(scaffold_values),
        "genome_length_display": format_sequence_length(numeric_summary(genome_lengths)["mean"]) if numeric_summary(genome_lengths) else "N/A",
        "top_hosts": summarize_top_values(rows, "Host"),
        "top_countries": summarize_top_values(rows, "Country"),
        "top_continents": metadata_value_counts(frame, "Continent"),
        "top_sources": summarize_top_values(rows, "Isolation Source"),
        "top_sample_types": summarize_top_values(rows, "Sample Type"),
        "top_environments": summarize_top_values(rows, "Environment (Broad Scale)"),
        "assembly_levels": summarize_top_values(rows, "Assembly Level"),
        "assembly_statuses": summarize_top_values(rows, "Assembly Status"),
        "top_subcontinents": metadata_value_counts(frame, "Subcontinent"),
        "top_host_diseases": metadata_value_counts(frame, "Host Disease"),
        "species_diversity": species_diversity,
        "quality_bands": build_quality_bands(frame),
        "coverage": coverage,
        "completeness_rows": completeness_rows,
        "insights": build_metadata_insights(
            frame,
            species,
            total_rows=total_rows,
            year_start=year_start,
            year_end=year_end,
            completeness_rows=completeness_rows,
        ),
        "summary_paragraphs": summary_paragraphs,
        "summary_paragraphs_html": [emphasize_summary_text(paragraph) for paragraph in summary_paragraphs],
        "numeric_findings": build_numeric_findings(frame),
        "correlations": build_correlation_summaries(frame),
        "plots": build_plot_bundle(frame),
    }


def write_species_tsv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SPECIES_TSV_COLUMNS, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in SPECIES_TSV_COLUMNS})
    temp_path.replace(output_path)


def fetch_species_dataset_rows(species: SpeciesRecord) -> tuple[list[dict[str, Any]], int | None]:
    command = [
        DATASETS_BINARY,
        "summary",
        "genome",
        "taxon",
        species.query_name,
        "--as-json-lines",
    ]
    datasets_source = ASSEMBLY_SOURCES[normalize_assembly_source(species.assembly_source)]["datasets_value"]
    if datasets_source:
        command.extend(["--assembly-source", str(datasets_source)])
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(stderr or "datasets CLI failed without an error message.")

    rows: list[dict[str, Any]] = []
    taxon_id: int | None = None
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        payload = json.loads(line)
        row = build_species_tsv_row(payload)
        if row["Assembly Accession"] and row["Organism Name"]:
            rows.append(row)
        if taxon_id is None:
            discovered_taxon = nested_get(payload, "organism", "tax_id")
            if isinstance(discovered_taxon, int):
                taxon_id = discovered_taxon

    if not rows:
        raise RuntimeError("No assemblies were returned for that species.")

    rows.sort(key=lambda item: (str(item.get("Organism Name") or ""), str(item.get("Assembly Accession") or "")))
    return rows, taxon_id


def taxonomy_count(payload: dict[str, Any], count_type: str) -> int:
    counts = nested_get(payload, "taxonomy", "counts") or payload.get("counts") or []
    for entry in counts:
        if entry.get("type") == count_type:
            try:
                return int(entry.get("count") or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def fetch_scope_taxon_candidates(
    scope: DiscoveryScopeRecord,
    *,
    rank_override: str | None = None,
    limit_override: str | None = None,
) -> list[tuple[str, int | None]]:
    requested_rank = normalize_taxon_rank(rank_override or scope.target_rank)
    command = [
        DATASETS_BINARY,
        "summary",
        "taxonomy",
        "taxon",
        discovery_scope_query_value(scope),
        "--children",
        "--rank",
        requested_rank,
        "--limit",
        normalize_discovery_limit(limit_override or DISCOVERY_LIMIT_PER_SCOPE),
        "--as-json-lines",
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(stderr or "datasets taxonomy discovery failed without an error message.")

    candidates: list[tuple[str, int | None]] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        payload = json.loads(line)
        taxonomy = payload.get("taxonomy", {}) or {}
        current_name = nested_get(taxonomy, "current_scientific_name", "name") or nested_get(payload, "current_scientific_name", "name")
        if not current_name:
            continue
        if taxonomy_count(payload, "COUNT_TYPE_ASSEMBLY") <= 0:
            continue
        tax_id = taxonomy.get("tax_id") or payload.get("tax_id")
        candidates.append((normalize_species_name(str(current_name)), int(tax_id) if isinstance(tax_id, int) else None))
    seen: set[str] = set()
    unique: list[tuple[str, int | None]] = []
    for name, tax_id in candidates:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, tax_id))
    return unique


def create_species(
    species_name: str,
    db: sqlite3.Connection | None = None,
    *,
    assembly_source: str = "all",
    taxon_rank: str = "species",
) -> SpeciesRecord:
    normalized = normalize_species_name(species_name)
    if len(normalized) < 3:
        raise ValueError("Taxon name must be at least 3 characters.")
    connection = db or get_db()
    normalized_rank = normalize_taxon_rank(taxon_rank)
    existing = get_taxon_by_name(normalized, normalized_rank, connection)
    if existing is not None:
        return existing
    created_at = utc_now()
    return save_species(
        SpeciesRecord(
            id=0,
            species_name=normalized,
            slug=species_slug(normalized),
            taxon_rank=normalized_rank,
            claim_token=0,
            sync_attempt_count=0,
            sync_first_claimed_at=None,
            assembly_source=normalize_assembly_source(assembly_source),
            status="pending",
            created_at=created_at,
            updated_at=created_at,
            query_name=normalized,
            refresh_requested=True,
            metadata_status="missing",
        ),
        connection,
    )


def create_discovery_scope(
    scope_value: str,
    *,
    label: str | None = None,
    assembly_source: str = "all",
    target_rank: str = "species",
    internal: bool = False,
    db: sqlite3.Connection | None = None,
) -> DiscoveryScopeRecord:
    connection = db or get_db()
    normalized = scope_value.strip()
    if not normalized:
        raise ValueError("Discovery scope cannot be empty.")
    normalized_rank = normalize_taxon_rank(target_rank)
    scope_key = (
        make_internal_discovery_scope_key(normalized, normalized_rank)
        if internal
        else make_discovery_scope_key(normalized, normalized_rank)
    )
    existing = get_discovery_scope_by_value(scope_key, connection)
    if existing is not None:
        return existing
    now = utc_now()
    return save_discovery_scope(
        DiscoveryScopeRecord(
            id=0,
            scope_value=scope_key,
            scope_label=label or normalized,
            target_rank=normalized_rank,
            assembly_source=normalize_assembly_source(assembly_source),
            status="pending",
            created_at=now,
            updated_at=now,
            is_internal=internal,
            refresh_requested=True,
        ),
        connection,
    )


def request_species_sync(species: SpeciesRecord, db: sqlite3.Connection | None = None) -> SpeciesRecord:
    if species.tsv_path and Path(species.tsv_path).exists() and species.status == "ready":
        species.status = "ready"
    else:
        species.status = "pending"
    species.updated_at = utc_now()
    species.refresh_requested = True
    species.sync_error = None
    species.sync_attempt_count = 0
    species.sync_first_claimed_at = None
    species.claimed_by = None
    species.claimed_at = None
    return save_species(species, db)


def request_species_metadata_build(species: SpeciesRecord, db: sqlite3.Connection | None = None) -> SpeciesRecord:
    if species.metadata_path and Path(species.metadata_path).exists() and species.metadata_status == "ready":
        species.metadata_status = "ready"
    else:
        species.metadata_status = "pending"
    species.metadata_error = None
    species.metadata_refresh_requested = True
    species.metadata_claimed_by = None
    species.metadata_claimed_at = None
    species.metadata_attempt_count = 0
    species.metadata_first_claimed_at = None
    species.updated_at = utc_now()
    return save_species(species, db)


def release_metadata_claim_rows(
    db: sqlite3.Connection,
    row_ids: list[int],
    *,
    error_message: str | None = None,
) -> int:
    if not row_ids:
        return 0
    placeholders = ", ".join("?" for _ in row_ids)
    params: list[Any] = [utc_now()]
    error_sql = "metadata_error"
    if error_message is not None:
        error_sql = "?"
        params.append(error_message)
    params.extend(row_ids)
    cursor = db.execute(
        f"""
        UPDATE species
        SET metadata_status = CASE WHEN metadata_path IS NOT NULL THEN 'ready' ELSE 'pending' END,
            updated_at = ?,
            metadata_claimed_by = NULL,
            metadata_claimed_at = NULL,
            metadata_first_claimed_at = NULL,
            metadata_attempt_count = 0,
            metadata_refresh_requested = CASE
                WHEN metadata_path IS NOT NULL THEN 1
                ELSE metadata_refresh_requested
            END,
            metadata_error = {error_sql}
        WHERE id IN ({placeholders})
        """,
        tuple(params),
    )
    return int(cursor.rowcount or 0)


def release_duplicate_metadata_claims_for_worker(
    db: sqlite3.Connection,
    worker_name: str,
) -> int:
    rows = db.execute(
        """
        SELECT id
        FROM species
        WHERE metadata_claimed_by = ?
          AND metadata_claimed_at IS NOT NULL
        ORDER BY metadata_claimed_at DESC, id DESC
        """,
        (worker_name,),
    ).fetchall()
    stale_ids = [int(row["id"]) for row in rows[1:]]
    return release_metadata_claim_rows(
        db,
        stale_ids,
        error_message="Orphaned metadata build claim was reset.",
    )


def release_stale_metadata_claims_for_dead_workers(db: sqlite3.Connection) -> int:
    rows = db.execute(
        """
        SELECT id, metadata_claimed_by
        FROM species
        WHERE metadata_claimed_at IS NOT NULL
          AND metadata_claimed_by IS NOT NULL
        """
    ).fetchall()
    stale_ids = [
        int(row["id"])
        for row in rows
        if row["metadata_claimed_by"] and not worker_heartbeat_is_live(str(row["metadata_claimed_by"]))
    ]
    return release_metadata_claim_rows(
        db,
        stale_ids,
        error_message="Stale metadata claim from a dead worker was reset.",
    )


def reset_stale_metadata_failures() -> int:
    db = get_db()
    cursor = db.execute(
        """
        UPDATE species
        SET metadata_status = CASE WHEN metadata_path IS NOT NULL THEN 'ready' ELSE 'pending' END,
            metadata_error = NULL,
            metadata_refresh_requested = 1,
            metadata_claimed_by = NULL,
            metadata_claimed_at = NULL,
            metadata_attempt_count = 0,
            metadata_first_claimed_at = NULL,
            updated_at = ?
        WHERE metadata_status = 'failed'
          AND metadata_error = 'Worker restarted while metadata build was running.'
          AND status = 'ready'
          AND tsv_path IS NOT NULL
        """,
        (utc_now(),),
    )
    db.commit()
    return int(cursor.rowcount or 0)


def request_discovery_scope_refresh(scope: DiscoveryScopeRecord, db: sqlite3.Connection | None = None) -> DiscoveryScopeRecord:
    scope.status = "pending"
    scope.updated_at = utc_now()
    scope.refresh_requested = True
    scope.last_error = None
    return save_discovery_scope(scope, db)


def sync_discovery_scopes_from_env(db: sqlite3.Connection) -> None:
    configured: list[tuple[str, str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    for item in DISCOVERY_SCOPES:
        parts = [part.strip() for part in item.split("|")]
        scope_value = parts[0] if parts else ""
        target_rank = normalize_taxon_rank(parts[1] if len(parts) > 1 else "species")
        scope_label = parts[2] if len(parts) > 2 and parts[2] else None
        if not scope_value:
            continue
        key = (scope_value, target_rank)
        if key in seen:
            continue
        seen.add(key)
        configured.append((scope_value, target_rank, scope_label))

    for scope_value, target_rank, scope_label in configured:
        scope_key = make_discovery_scope_key(scope_value, target_rank)
        existing = get_discovery_scope_by_value(scope_key, db)
        if existing is None:
            create_discovery_scope(
                scope_value,
                db=db,
                assembly_source=DEFAULT_ASSEMBLY_SOURCE,
                target_rank=target_rank,
                label=scope_label,
            )
            continue

        changed = False
        desired_label = scope_label or existing.scope_label
        if existing.scope_label != desired_label:
            existing.scope_label = desired_label
            changed = True
        if existing.assembly_source != DEFAULT_ASSEMBLY_SOURCE:
            existing.assembly_source = DEFAULT_ASSEMBLY_SOURCE
            changed = True
        if existing.target_rank != target_rank:
            existing.target_rank = target_rank
            changed = True
        if changed:
            existing.updated_at = utc_now()
            save_discovery_scope(existing, db)
            request_discovery_scope_refresh(existing, db)
    db.commit()


def schedule_due_species_syncs(build_hours: int | None, refresh_hours: int | None) -> None:
    now = datetime.now(timezone.utc)
    build_cutoff = now - timedelta(hours=build_hours) if build_hours is not None else None
    refresh_cutoff = now - timedelta(hours=refresh_hours) if refresh_hours is not None else None
    with get_sqlite_connection() as db:
        rows = db.execute("SELECT * FROM species").fetchall()
        for row in rows:
            last_synced_at = str(row["last_synced_at"]) if row["last_synced_at"] else None
            refresh_requested = bool(row["refresh_requested"])
            status = str(row["status"])
            tsv_path = str(row["tsv_path"]) if row["tsv_path"] else None
            has_tsv = bool(tsv_path and Path(tsv_path).exists())
            due = refresh_requested
            if not due and has_tsv:
                if refresh_hours is None:
                    due = False
                elif last_synced_at:
                    try:
                        due = parse_utc(last_synced_at) <= refresh_cutoff if refresh_cutoff is not None else False
                    except ValueError:
                        due = True
                else:
                    due = True
            elif not due and not has_tsv:
                if last_synced_at:
                    try:
                        due = parse_utc(last_synced_at) <= build_cutoff if build_cutoff is not None else False
                    except ValueError:
                        due = True
                else:
                    due = build_hours is not None and status == "pending"

            if not due or status == "syncing":
                continue

            if has_tsv and status == "ready":
                db.execute(
                    """
                    UPDATE species
                    SET refresh_requested = 1,
                        updated_at = ?,
                        sync_error = NULL,
                        claimed_by = NULL,
                        claimed_at = NULL
                    WHERE id = ?
                      AND status = 'ready'
                      AND claimed_at IS NULL
                    """,
                    (utc_now(), row["id"]),
                )
            else:
                db.execute(
                    """
                    UPDATE species
                    SET status = 'pending',
                        refresh_requested = 1,
                        updated_at = ?,
                        sync_error = NULL,
                        sync_attempt_count = 0,
                        sync_first_claimed_at = NULL,
                        claimed_by = NULL,
                        claimed_at = NULL
                    WHERE id = ?
                      AND status != 'syncing'
                      AND claimed_at IS NULL
                    """,
                    (utc_now(), row["id"]),
                )
        db.commit()


def schedule_due_metadata_builds(build_hours: int | None, refresh_hours: int | None) -> None:
    with get_sqlite_connection() as db:
        rows = db.execute("SELECT * FROM species").fetchall()
        build_cutoff = None
        refresh_cutoff = None
        if build_hours is not None:
            build_cutoff = datetime.now(timezone.utc) - timedelta(hours=build_hours)
        if refresh_hours is not None:
            refresh_cutoff = datetime.now(timezone.utc) - timedelta(hours=refresh_hours)
        for row in rows:
            status = str(row["status"])
            metadata_status = str(row["metadata_status"] or "missing")
            refresh_requested = bool(row["metadata_refresh_requested"])
            last_built_at = str(row["metadata_last_built_at"]) if row["metadata_last_built_at"] else None
            tsv_path = str(row["tsv_path"]) if row["tsv_path"] else None
            metadata_path = str(row["metadata_path"]) if row["metadata_path"] else None
            has_metadata = bool(metadata_path and Path(metadata_path).exists())
            due = False

            if status != "ready" or not tsv_path or not Path(tsv_path).exists():
                continue
            if metadata_status == "building":
                continue
            if has_metadata:
                if refresh_requested:
                    due = True
                elif refresh_hours is None:
                    continue
                elif last_built_at:
                    try:
                        due = parse_utc(last_built_at) <= refresh_cutoff if refresh_cutoff is not None else False
                    except ValueError:
                        due = True
                else:
                    due = True
            else:
                if refresh_requested:
                    due = True
                elif build_hours is None:
                    continue
                elif last_built_at:
                    try:
                        due = parse_utc(last_built_at) <= build_cutoff if build_cutoff is not None else False
                    except ValueError:
                        due = True
                else:
                    due = True

            if not due:
                continue

            if has_metadata and metadata_status == "ready":
                db.execute(
                    """
                    UPDATE species
                    SET metadata_refresh_requested = 1,
                        metadata_error = NULL,
                        metadata_claimed_by = NULL,
                        metadata_claimed_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                      AND metadata_status = 'ready'
                      AND metadata_claimed_at IS NULL
                    """,
                    (utc_now(), row["id"]),
                )
            else:
                db.execute(
                    """
                    UPDATE species
                    SET metadata_status = 'pending',
                        metadata_refresh_requested = 1,
                        metadata_error = NULL,
                        metadata_claimed_by = NULL,
                        metadata_claimed_at = NULL,
                        metadata_attempt_count = 0,
                        metadata_first_claimed_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                      AND metadata_status != 'building'
                      AND metadata_claimed_at IS NULL
                    """,
                    (utc_now(), row["id"]),
                )
        db.commit()


def schedule_due_discovery_scope_syncs(refresh_hours: int | None) -> None:
    if refresh_hours is None:
        return
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=refresh_hours)
    with get_sqlite_connection() as db:
        rows = db.execute("SELECT * FROM discovery_scopes").fetchall()
        for row in rows:
            last_discovered_at = str(row["last_discovered_at"]) if row["last_discovered_at"] else None
            refresh_requested = bool(row["refresh_requested"])
            status = str(row["status"])
            due = refresh_requested
            if not due and last_discovered_at:
                try:
                    due = parse_utc(last_discovered_at) <= cutoff
                except ValueError:
                    due = True
            elif not due and not last_discovered_at:
                due = True

            if not due or status == "discovering":
                continue

            db.execute(
                """
                UPDATE discovery_scopes
                SET status = 'pending', updated_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (utc_now(), row["id"]),
            )
        db.commit()


def claim_next_species_metadata_build(worker_name: str) -> SpeciesRecord | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        release_stale_metadata_claims_for_dead_workers(db)
        release_duplicate_metadata_claims_for_worker(db, worker_name)
        rows = db.execute(
            """
            SELECT *
            FROM species
            WHERE (
                    metadata_status = 'pending'
                    OR (metadata_status = 'ready' AND metadata_refresh_requested = 1 AND metadata_path IS NOT NULL)
                )
              AND status = 'ready'
              AND tsv_path IS NOT NULL
            ORDER BY
                CASE
                    WHEN metadata_status = 'pending' AND metadata_path IS NULL THEN 0
                    WHEN metadata_status = 'ready' AND metadata_refresh_requested = 1 THEN 1
                    ELSE 2
                END,
                CASE WHEN taxon_rank = 'genus' THEN 0 ELSE 1 END,
                CASE WHEN genome_count IS NULL THEN 1 ELSE 0 END,
                COALESCE(genome_count, 2147483647) ASC,
                updated_at ASC,
                created_at ASC
            LIMIT 24
            """
        ).fetchall()
        if not rows:
            db.commit()
            return None

        claimed_row: sqlite3.Row | None = None
        for row in rows:
            if not metadata_lock_is_available(int(row["id"])):
                continue
            claimed_at = utc_now()
            has_existing_metadata = bool(row["metadata_path"])
            metadata_status = "ready" if has_existing_metadata else "building"
            cursor = db.execute(
                """
                UPDATE species
                SET metadata_status = ?,
                    metadata_claimed_by = ?,
                    metadata_claimed_at = ?,
                    metadata_first_claimed_at = COALESCE(metadata_first_claimed_at, ?),
                    metadata_attempt_count = metadata_attempt_count + 1,
                    metadata_refresh_requested = 0,
                    metadata_claim_token = metadata_claim_token + 1,
                    updated_at = ?
                WHERE id = ?
                  AND metadata_claimed_at IS NULL
                  AND (
                        metadata_status = 'pending'
                        OR (metadata_status = 'ready' AND metadata_refresh_requested = 1 AND metadata_path IS NOT NULL)
                      )
                """,
                (metadata_status, worker_name, claimed_at, claimed_at, claimed_at, row["id"]),
            )
            if int(cursor.rowcount or 0) == 1:
                claimed_row = row
                break

        if claimed_row is None:
            db.commit()
            return None

        db.commit()
        claimed = get_species_by_id(int(claimed_row["id"]), db)
        assert claimed is not None
        return claimed


def claim_next_species_sync(worker_name: str) -> SpeciesRecord | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT *
            FROM species
            WHERE (
                    status = 'pending'
                    OR (status = 'ready' AND refresh_requested = 1 AND tsv_path IS NOT NULL)
                )
            ORDER BY
                CASE
                    WHEN status = 'pending' AND tsv_path IS NULL THEN 0
                    WHEN status = 'ready' AND refresh_requested = 1 THEN 1
                    ELSE 2
                END,
                CASE WHEN taxon_rank = 'species' THEN 0 ELSE 1 END,
                updated_at ASC,
                created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None

        claimed_at = utc_now()
        has_existing_tsv = bool(row["tsv_path"])
        status = "ready" if has_existing_tsv else "syncing"
        cursor = db.execute(
            """
            UPDATE species
            SET status = ?,
                updated_at = ?,
                claimed_by = ?,
                claimed_at = ?,
                sync_first_claimed_at = COALESCE(sync_first_claimed_at, ?),
                sync_attempt_count = sync_attempt_count + 1,
                refresh_requested = 0,
                claim_token = claim_token + 1
            WHERE id = ?
              AND claimed_at IS NULL
              AND (
                    status = 'pending'
                    OR (status = 'ready' AND refresh_requested = 1 AND tsv_path IS NOT NULL)
                  )
            """,
            (status, claimed_at, worker_name, claimed_at, claimed_at, row["id"]),
        )
        if int(cursor.rowcount or 0) != 1:
            db.commit()
            return None
        db.commit()
        claimed = get_species_by_id(int(row["id"]), db)
        assert claimed is not None
        return claimed


def claim_next_discovery_scope(worker_name: str) -> DiscoveryScopeRecord | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT * FROM discovery_scopes WHERE status = 'pending' ORDER BY updated_at ASC, created_at ASC LIMIT 1"
        ).fetchone()
        if row is None:
            db.commit()
            return None

        claimed_at = utc_now()
        db.execute(
            """
            UPDATE discovery_scopes
            SET status = 'discovering', updated_at = ?, claimed_by = ?, claimed_at = ?, refresh_requested = 0
            WHERE id = ?
            """,
            (claimed_at, worker_name, claimed_at, row["id"]),
        )
        db.commit()
        claimed = get_discovery_scope_by_id(int(row["id"]), db)
        assert claimed is not None
        return claimed


def sync_species_record(species: SpeciesRecord) -> None:
    try:
        initial = load_species(species.id)
        if initial.claim_token != species.claim_token:
            return
        before_genome_count = int(initial.genome_count) if initial.genome_count is not None else None
        sync_kind = "refresh" if initial.tsv_path and Path(initial.tsv_path).exists() else "build"
        rows, taxon_id = fetch_species_dataset_rows(initial)
        current = load_species(species.id)
        if current.claim_token != species.claim_token:
            return
        output_path = species_tsv_path(current.slug)
        write_species_tsv(rows, output_path)
        current.tsv_path = str(output_path)
        current.taxon_id = taxon_id
        current.genome_count = len(rows)
        current.status = "ready"
        current.last_synced_at = utc_now()
        current.updated_at = current.last_synced_at
        current.sync_error = None
        current.refresh_requested = False
        current.metadata_refresh_requested = True
        current.claimed_by = None
        current.claimed_at = None
        current.sync_attempt_count = 0
        current.sync_first_claimed_at = None
        save_species(current)
        record_taxon_sync_event(
            current,
            sync_kind=sync_kind,
            before_genome_count=before_genome_count,
            after_genome_count=current.genome_count,
            synced_at=current.last_synced_at,
        )
    except Exception as exc:
        current = load_species(species.id)
        if current.claim_token != species.claim_token:
            return
        current.updated_at = utc_now()
        has_existing_tsv = bool(current.tsv_path and Path(current.tsv_path).exists())
        if current.sync_attempt_count < SPECIES_MAX_AUTO_RETRIES:
            current.status = "pending"
            current.sync_error = (
                f"{exc} (auto-retrying {current.sync_attempt_count}/{SPECIES_MAX_AUTO_RETRIES})"
            )
        else:
            current.status = "ready" if has_existing_tsv else "failed"
            current.sync_error = str(exc)
        current.refresh_requested = False
        current.claimed_by = None
        current.claimed_at = None
        save_species(current)


def species_parent_genus_name(species: SpeciesRecord) -> str | None:
    if species.taxon_rank != "species":
        return None
    parts = species.species_name.split()
    return parts[0] if parts else None


def write_filtered_metadata_file(source_path: Path, output_path: Path, organism_name: str) -> int:
    if not source_path.exists():
        return 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    delimiter = "\t" if source_path.suffix.lower() == ".tsv" else ","
    with source_path.open("r", encoding="utf-8", newline="") as src_handle:
        reader = csv.DictReader(src_handle, delimiter=delimiter)
        fieldnames = reader.fieldnames or []
        with output_path.open("w", encoding="utf-8", newline="") as dest_handle:
            writer = csv.DictWriter(dest_handle, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            for row in reader:
                if normalize_species_name(str(row.get("Organism Name") or "")) != organism_name:
                    continue
                writer.writerow(row)
                count += 1
    return count


def derive_species_metadata_from_genus(species: SpeciesRecord) -> tuple[str, str, int, int] | None:
    genus_name = species_parent_genus_name(species)
    if not genus_name:
        return None
    genus = get_taxon_by_name(genus_name, "genus")
    if genus is None or genus.metadata_status != "ready":
        return None
    source_dataset = Path(genus.metadata_path or "")
    source_clean = Path(genus.metadata_clean_path or "")
    if not source_dataset.exists() or not source_clean.exists():
        return None

    stored_rows = load_taxon_metadata_rows(genus.id)
    if stored_rows:
        filtered_rows = [
            row
            for row in stored_rows.values()
            if normalize_species_name(str(row.get("Organism Name") or "")) == species.species_name
        ]
        if filtered_rows:
            save_taxon_metadata_rows(species.id, filtered_rows, refreshed_at=utc_now())
            updated_path, clean_path, clean_count = write_taxon_metadata_outputs(species.slug, filtered_rows)
            return str(updated_path), str(clean_path), int(genus.id), clean_count

    taxon_dir = metadata_taxon_dir(species.slug)
    shutil.rmtree(taxon_dir, ignore_errors=True)
    metadata_output_dir = taxon_dir / "metadata_output"
    updated_path = metadata_output_dir / "ncbi_dataset_updated.tsv"
    clean_path = metadata_output_dir / "ncbi_clean.csv"
    updated_count = write_filtered_metadata_file(source_dataset, updated_path, species.species_name)
    clean_count = write_filtered_metadata_file(source_clean, clean_path, species.species_name)
    if updated_count <= 0 or clean_count <= 0:
        shutil.rmtree(taxon_dir, ignore_errors=True)
        return None
    return str(updated_path), str(clean_path), int(genus.id), clean_count


def write_taxon_metadata_outputs(slug: str, rows: list[dict[str, Any]]) -> tuple[str, str, int]:
    if not rows:
        raise RuntimeError("No metadata rows are available to write.")
    taxon_dir = metadata_taxon_dir(slug)
    shutil.rmtree(taxon_dir, ignore_errors=True)
    metadata_output_dir = taxon_dir / "metadata_output"
    metadata_output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)

    updated_path = metadata_output_dir / "ncbi_dataset_updated.tsv"
    save_summary(df, str(updated_path))

    if "Assembly Accession" in df.columns:
        df_sorted = df.sort_values(
            by="Assembly Accession",
            key=lambda x: x.astype("string").str.startswith("GCF"),
            ascending=False,
        )
    else:
        df_sorted = df
    dedup_key = "Assembly Name" if "Assembly Name" in df_sorted.columns else "Assembly Accession"
    if dedup_key in df_sorted.columns:
        df_dedup = df_sorted.drop_duplicates(subset=[dedup_key], keep="first")
    else:
        df_dedup = df_sorted
    comprehensive_columns = list(df_dedup.columns)
    clean_df = add_geo_columns(df_dedup.copy())
    if "Geographic Location" in clean_df.columns:
        clean_df["Country"] = clean_df["Geographic Location"].apply(extract_country)
    clean_path = metadata_output_dir / "ncbi_clean.csv"
    output_columns = comprehensive_columns + ["Country", "Continent", "Subcontinent"]
    save_clean_data(clean_df, [column for column in output_columns if column in clean_df.columns], str(clean_path))
    clean_count = len(clean_df.index)
    return str(updated_path), str(clean_path), clean_count


def run_taxon_metadata_pipeline(species: SpeciesRecord) -> tuple[str, str, int | None, int]:
    if not species.tsv_path:
        raise RuntimeError("Managed TSV path is missing for metadata build.")

    df = load_data(species.tsv_path)
    df, _filter_summary = filter_data(df, None, ["all"])
    if "Assembly Accession" not in df.columns:
        raise RuntimeError("Managed TSV is missing Assembly Accession.")
    df = df.drop_duplicates(subset=["Assembly Accession"], keep="first").copy()
    if "Assembly BioSample Accession" not in df.columns:
        df["Assembly BioSample Accession"] = pd.NA

    stored_rows = load_taxon_metadata_rows(species.id)
    ordered_rows: list[tuple[str, dict[str, Any]]] = []
    new_tsv_rows: list[dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        accession = str(row.get("Assembly Accession") or "").strip()
        if not accession:
            continue
        if accession in stored_rows:
            ordered_rows.append(("stored", merge_tsv_record_with_stored_metadata(row, stored_rows[accession])))
        else:
            ordered_rows.append(("new", row))
            new_tsv_rows.append(row)

    biosample_ids = [
        biosample_id
        for biosample_id in {metadata_row_biosample_accession(row) for row in new_tsv_rows}
        if biosample_id
    ]
    biosample_records = fetch_biosample_metadata_records(biosample_ids)
    current_rows: list[dict[str, Any]] = []
    for row_type, row in ordered_rows:
        if row_type == "stored":
            current_rows.append(row)
            continue
        biosample_id = metadata_row_biosample_accession(row)
        current_rows.append(enrich_tsv_row_with_biosample_metadata(row, biosample_records.get(biosample_id)))

    refreshed_at = utc_now()
    save_taxon_metadata_rows(species.id, current_rows, refreshed_at=refreshed_at)
    updated_path, clean_path, clean_count = write_taxon_metadata_outputs(species.slug, current_rows)
    return str(updated_path), str(clean_path), None, clean_count


def build_species_metadata_record(species: SpeciesRecord) -> None:
    lock_handle = acquire_metadata_lock(species.id)
    if lock_handle is None:
        latest = load_species(species.id)
        if latest.metadata_claim_token == species.metadata_claim_token:
            latest.metadata_status = "ready" if latest.metadata_path and Path(latest.metadata_path).exists() else "pending"
            latest.metadata_refresh_requested = bool(latest.metadata_path)
            latest.metadata_claimed_by = None
            latest.metadata_claimed_at = None
            latest.metadata_first_claimed_at = None
            latest.metadata_attempt_count = 0
            latest.metadata_error = "Metadata build lock was unavailable; claim reset."
            latest.updated_at = utc_now()
            save_species(latest)
        return
    current = load_species(species.id)
    try:
        if current.metadata_claim_token != species.metadata_claim_token:
            return
        if current.status != "ready" or not current.tsv_path or not Path(current.tsv_path).exists():
            current.metadata_status = "failed"
            current.metadata_error = "Taxon TSV is not ready for metadata build."
            current.metadata_claimed_by = None
            current.metadata_claimed_at = None
            save_species(current)
            return
        if current.taxon_rank == "species":
            genus_name = species_parent_genus_name(current)
            if genus_name:
                genus = get_taxon_by_name(genus_name, "genus")
                if (
                    genus is not None
                    and genus.status == "ready"
                    and genus.tsv_path
                    and Path(genus.tsv_path).exists()
                    and genus.metadata_status in {"pending", "building"}
                ):
                    current.metadata_status = "pending"
                    current.metadata_refresh_requested = True
                    current.metadata_claimed_by = None
                    current.metadata_claimed_at = None
                    current.metadata_first_claimed_at = None
                    current.metadata_attempt_count = 0
                    current.metadata_error = "Waiting for parent genus metadata to be ready."
                    current.updated_at = utc_now()
                    save_species(current)
                    return
        try:
            derived = derive_species_metadata_from_genus(current)
            if derived is not None:
                metadata_path, clean_path, source_taxon_id, clean_count = derived
            else:
                metadata_path, clean_path, source_taxon_id, clean_count = run_taxon_metadata_pipeline(current)
            latest = load_species(species.id)
            if latest.metadata_claim_token != species.metadata_claim_token:
                return
            latest.metadata_status = "ready"
            latest.metadata_path = metadata_path
            latest.metadata_clean_path = clean_path
            latest.metadata_last_built_at = utc_now()
            latest.metadata_error = None
            latest.metadata_refresh_requested = False
            latest.metadata_claimed_by = None
            latest.metadata_claimed_at = None
            latest.metadata_first_claimed_at = None
            latest.metadata_attempt_count = 0
            latest.metadata_source_taxon_id = source_taxon_id
            latest.genome_count = latest.genome_count or clean_count
            latest.updated_at = latest.metadata_last_built_at
            save_species(latest)
        except Exception as exc:
            latest = load_species(species.id)
            if latest.metadata_claim_token != species.metadata_claim_token:
                return
            latest.updated_at = utc_now()
            has_existing_metadata = bool(latest.metadata_path and Path(latest.metadata_path).exists())
            if latest.metadata_attempt_count < METADATA_MAX_AUTO_RETRIES:
                latest.metadata_status = "pending"
                latest.metadata_error = f"{exc} (auto-retrying {latest.metadata_attempt_count}/{METADATA_MAX_AUTO_RETRIES})"
            else:
                latest.metadata_status = "ready" if has_existing_metadata else "failed"
                latest.metadata_error = str(exc)
            latest.metadata_refresh_requested = False
            latest.metadata_claimed_by = None
            latest.metadata_claimed_at = None
            save_species(latest)
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()


def sync_discovery_scope(scope: DiscoveryScopeRecord) -> None:
    latest = load_discovery_scope(scope.id)
    try:
        if should_partition_discovery_scope(latest):
            candidates = fetch_scope_taxon_candidates(latest, rank_override="genus", limit_override="all")
            discovered = 0
            with get_sqlite_connection() as db:
                for genus_name, taxon_id in candidates:
                    query_value = str(taxon_id) if taxon_id else genus_name
                    child = create_discovery_scope(
                        query_value,
                        db=db,
                        label=f"{genus_name} species",
                        assembly_source=latest.assembly_source,
                        target_rank="species",
                        internal=True,
                    )
                    changed = False
                    if child.scope_label != f"{genus_name} species":
                        child.scope_label = f"{genus_name} species"
                        changed = True
                    if child.assembly_source != latest.assembly_source:
                        child.assembly_source = latest.assembly_source
                        changed = True
                    if child.status == "ready" and not child.refresh_requested:
                        child = request_discovery_scope_refresh(child, db)
                    elif child.status in {"failed", "pending"}:
                        child = request_discovery_scope_refresh(child, db)
                    elif changed:
                        child.updated_at = utc_now()
                        save_discovery_scope(child, db)
                    discovered += 1

                latest.status = "ready"
                latest.updated_at = utc_now()
                latest.last_discovered_at = latest.updated_at
                latest.last_error = None
                latest.refresh_requested = False
                latest.discovered_species_count = discovered
                save_discovery_scope(latest, db)
            return

        candidates = fetch_scope_taxon_candidates(latest)
        discovered = 0
        with get_sqlite_connection() as db:
            for taxon_name, taxon_id in candidates:
                existing = get_taxon_by_name(taxon_name, latest.target_rank, db)
                if existing is None:
                    created = create_species(
                        taxon_name,
                        db,
                        assembly_source=latest.assembly_source,
                        taxon_rank=latest.target_rank,
                    )
                    created.taxon_id = taxon_id
                    created.query_name = str(taxon_id) if taxon_id else taxon_name
                    created.updated_at = utc_now()
                    save_species(created, db)
                    request_species_sync(created, db)
                    discovered += 1
                else:
                    changed = False
                    source_changed = False
                    if existing.assembly_source != latest.assembly_source:
                        existing.assembly_source = latest.assembly_source
                        changed = True
                        source_changed = True
                    if taxon_id and existing.taxon_id != taxon_id:
                        existing.taxon_id = taxon_id
                        existing.query_name = str(taxon_id)
                        changed = True
                    if changed:
                        existing.updated_at = utc_now()
                        save_species(existing, db)
                        if source_changed:
                            request_species_sync(existing, db)
            latest.status = "ready"
            latest.updated_at = utc_now()
            latest.last_discovered_at = latest.updated_at
            latest.last_error = None
            latest.refresh_requested = False
            latest.discovered_species_count = discovered
            save_discovery_scope(latest, db)
    except Exception as exc:
        latest.status = "failed"
        latest.updated_at = utc_now()
        latest.last_error = str(exc)
        latest.refresh_requested = False
        save_discovery_scope(latest)


def recover_species_for_worker_startup() -> None:
    with get_sqlite_connection() as db:
        db.execute(
            """
            UPDATE species
            SET status = CASE WHEN tsv_path IS NOT NULL THEN 'ready' ELSE 'failed' END,
                updated_at = ?,
                claimed_by = NULL,
                claimed_at = NULL,
                sync_first_claimed_at = NULL,
                sync_attempt_count = 0,
                sync_error = CASE
                    WHEN tsv_path IS NOT NULL THEN sync_error
                    ELSE 'Worker restarted while species sync was running.'
                END
            WHERE status = 'syncing'
               OR claimed_at IS NOT NULL
            """,
            (utc_now(),),
        )
        db.commit()


def recover_metadata_for_worker_startup() -> None:
    with get_sqlite_connection() as db:
        db.execute(
            """
            UPDATE species
            SET metadata_status = CASE WHEN metadata_path IS NOT NULL THEN 'ready' ELSE 'failed' END,
                updated_at = ?,
                metadata_claimed_by = NULL,
                metadata_claimed_at = NULL,
                metadata_first_claimed_at = NULL,
                metadata_attempt_count = 0,
                metadata_error = CASE
                    WHEN metadata_path IS NOT NULL THEN metadata_error
                    ELSE 'Worker restarted while metadata build was running.'
                END
            WHERE metadata_status = 'building'
               OR metadata_claimed_at IS NOT NULL
            """,
            (utc_now(),),
        )
        db.commit()


def recover_discovery_scopes_for_worker_startup() -> None:
    with get_sqlite_connection() as db:
        db.execute(
            """
            UPDATE discovery_scopes
            SET status = 'failed',
                updated_at = ?,
                last_error = 'Worker restarted while taxonomy discovery was running.'
            WHERE status = 'discovering'
            """,
            (utc_now(),),
        )
        db.commit()


def process_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def refresh_job(job: JobRecord) -> JobRecord:
    return load_job(job.id)


def build_command(mode: str, input_path: Path, output_dir: Path, form: Any) -> tuple[list[str], dict[str, Any]]:
    if mode != "seq":
        raise ValueError(f"Legacy CLI job mode '{mode}' is no longer supported.")

    command = ["integrated-sequence-download", str(input_path), str(output_dir)]
    filters: dict[str, Any] = {}

    for field in ("host", "year", "country", "cont", "subcont"):
        values = parse_multi_value(form.get(field, ""))
        if values:
            command.append(f"--{field}")
            command.extend(values)
            filters[field] = values

    retries = (form.get("retries") or "3").strip()
    retry_delay = (form.get("retry_delay") or "5").strip()
    command.extend(["--retries", retries, "--retry-delay", retry_delay])
    filters["retries"] = retries
    filters["retry_delay"] = retry_delay

    if form.get("check_only"):
        command.append("--check-only")
        filters["check_only"] = True

    return command, filters


def organize_sequence_outputs(job: JobRecord) -> None:
    filters = job.filters or {}
    if job.mode != "seq":
        return
    if normalize_metadata_value(filters.get("grouping_mode")) != "field":
        return

    input_path = Path(job.input_path)
    output_dir = Path(job.output_dir)
    if not input_path.exists() or not output_dir.exists():
        return

    group_field = normalize_sequence_group_field(filters.get("group_field"))
    group_column = sequence_group_column(group_field)
    try:
        frame = pd.read_csv(input_path).fillna("")
    except Exception as exc:
        append_job_log(job, f"[{utc_now()}] Grouped output setup skipped: could not read filtered metadata CSV ({exc}).\n")
        return

    if "Assembly Accession" not in frame.columns or group_column not in frame.columns:
        append_job_log(
            job,
            f"[{utc_now()}] Grouped output setup skipped: required column '{group_column}' was not present in the filtered metadata.\n",
        )
        return

    grouped_root = output_dir / f"grouped_by_{group_field}"
    grouped_root.mkdir(parents=True, exist_ok=True)

    accession_to_groups: dict[str, set[str]] = {}
    for _, row in frame.iterrows():
        accession = normalize_metadata_value(row.get("Assembly Accession"))
        if not accession:
            continue
        group_value = normalize_metadata_value(row.get(group_column)) or "Unassigned"
        accession_to_groups.setdefault(accession, set()).add(group_value)

    pattern = re.compile(r"^(GC[AF]_\d+\.\d+)_.*_genomic\.fna$")
    linked_total = 0
    for fasta_path in sorted(output_dir.glob("*.fna")):
        match = pattern.match(fasta_path.name)
        if not match:
            continue
        accession = match.group(1)
        group_values = accession_to_groups.get(accession)
        if not group_values:
            continue
        for group_value in sorted(group_values):
            target_dir = grouped_root / sequence_group_slug(group_value)
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / fasta_path.name
            if target_path.exists():
                continue
            try:
                os.link(fasta_path, target_path)
            except OSError:
                shutil.copy2(fasta_path, target_path)
            linked_total += 1

    summary_lines = [
        f"Grouping field: {group_field}",
        f"Metadata column: {group_column}",
        f"Grouped folders: {len([path for path in grouped_root.iterdir() if path.is_dir()])}",
        f"Linked sequence files: {linked_total}",
    ]
    (grouped_root / "grouping_summary.txt").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    for group_value, group_frame in frame.groupby(group_column, dropna=False):
        normalized_group = normalize_metadata_value(group_value) or "Unassigned"
        group_dir = grouped_root / sequence_group_slug(normalized_group)
        group_dir.mkdir(parents=True, exist_ok=True)
        group_frame.to_csv(group_dir / "metadata_subset.csv", index=False)

    append_job_log(
        job,
        f"[{utc_now()}] Grouped sequence output prepared in {grouped_root.name} using field '{group_column}'.\n",
    )


def append_job_log(job: JobRecord, message: str) -> None:
    path = Path(job.log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(message)


def is_cancel_requested(job_id: str) -> bool:
    with get_sqlite_connection() as db:
        row = db.execute("SELECT cancel_requested FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return bool(row["cancel_requested"]) if row else False


def claim_next_job(worker_name: str) -> JobRecord | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC").fetchall()
        if not rows:
            db.commit()
            return None

        for row in rows:
            job = row_to_job(row)
            wait_state, species = job_taxon_wait_state(job, db)
            if wait_state == "waiting":
                continue
            if wait_state in {"failed", "missing"}:
                failed_at = utc_now()
                reason = "Managed taxon refresh failed before job start."
                if wait_state == "missing":
                    reason = "Managed taxon was not found before job start."
                elif species is not None and species.sync_error:
                    reason = f"Managed taxon refresh failed before job start: {species.sync_error}"
                db.execute(
                    """
                    UPDATE jobs
                    SET status = 'failed', updated_at = ?, error = ?, return_code = 1
                    WHERE id = ?
                    """,
                    (failed_at, reason, row["id"]),
                )
                append_job_log(job, f"[{failed_at}] {reason}\n")
                continue

            claimed_at = utc_now()
            db.execute(
                """
                UPDATE jobs
                SET status = 'running', updated_at = ?, claimed_by = ?, claimed_at = ?
                WHERE id = ?
                """,
                (claimed_at, worker_name, claimed_at, row["id"]),
            )
            db.commit()
            return load_job(str(row["id"]), db)

        db.commit()
        return None


def launch_job(job: JobRecord) -> None:
    if job.mode == "seq":
        launch_integrated_sequence_job(job)
        return

    latest = load_job(job.id)
    latest.pid = None
    latest.updated_at = utc_now()
    latest.return_code = 1
    latest.status = "failed"
    latest.error = (
        "Legacy upload-based fetchM CLI jobs are no longer supported. "
        "Use the managed taxon metadata analysis and sequence download workflow."
    )
    save_job(latest)
    append_job_log(
        latest,
        f"[{utc_now()}] Legacy non-sequence CLI job rejected. "
        "Use the managed taxon workflow instead.\n",
    )
    notify_job_event(latest, "failed")


def build_integrated_sequence_namespace(job: JobRecord) -> argparse.Namespace:
    filters = job.filters or {}

    def parse_int_value(key: str, default: int) -> int:
        raw_value = normalize_metadata_value(filters.get(key))
        if not raw_value:
            return default
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return default

    def parse_float_value(key: str, default: float) -> float:
        raw_value = normalize_metadata_value(filters.get(key))
        if not raw_value:
            return default
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return default

    return argparse.Namespace(
        input=job.input_path,
        outdir=job.output_dir,
        host=None,
        year=None,
        country=None,
        cont=None,
        subcont=None,
        retries=parse_int_value("retries", 3),
        retry_delay=parse_float_value("retry_delay", 5.0),
        check_only=bool(filters.get("check_only")),
        download_workers=parse_int_value("download_workers", SEQUENCE_DOWNLOAD_WORKERS),
    )


def finalize_integrated_sequence_job(
    job: JobRecord,
    *,
    return_code: int,
    cancellation_honored: bool = False,
) -> None:
    latest = load_job(job.id)
    latest.pid = None
    latest.return_code = return_code
    latest.updated_at = utc_now()
    if latest.cancel_requested and cancellation_honored:
        latest.status = "cancelled"
    else:
        latest.status = "completed" if return_code == 0 else "failed"
    if latest.status == "completed":
        try:
            organize_sequence_outputs(latest)
        except Exception as exc:
            append_job_log(latest, f"[{utc_now()}] Grouped output organization failed: {exc}\n")
        try:
            prepare_sequence_download_artifacts(latest)
        except Exception as exc:
            append_job_log(latest, f"[{utc_now()}] Sequence download packaging failed: {exc}\n")
    save_job(latest)
    if latest.status == "completed":
        notify_job_event(latest, "finished")
    elif latest.status == "failed":
        notify_job_event(latest, "failed")


def launch_integrated_sequence_job(job: JobRecord) -> None:
    append_job_log(job, f"[{utc_now()}] Launching integrated sequence job {job.id}\n")
    append_job_log(job, "Integrated fetchM sequence download via web app worker.\n\n")

    if is_cancel_requested(job.id):
        append_job_log(job, f"[{utc_now()}] Cancellation requested before sequence download started.\n")
        finalize_integrated_sequence_job(job, return_code=1, cancellation_honored=True)
        return

    job.pid = None
    job.status = "running"
    job.updated_at = utc_now()
    save_job(job)

    namespace = build_integrated_sequence_namespace(job)
    log_handle = open(job.log_path, "a", encoding="utf-8")
    root_logger = logging.getLogger()
    file_handler = logging.StreamHandler(log_handle)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    previous_level = root_logger.level
    root_logger.addHandler(file_handler)
    root_logger.setLevel(min(previous_level, logging.INFO) if previous_level else logging.INFO)
    return_code = 0

    try:
        with contextlib.redirect_stdout(log_handle), contextlib.redirect_stderr(log_handle):
            run_sequence_downloads(
                namespace,
                input_path=job.input_path,
                output_folder=job.output_dir,
            )
        if is_cancel_requested(job.id):
            append_job_log(
                job,
                f"[{utc_now()}] Cancellation was requested while integrated sequence downloading was running. "
                "The current implementation completes the download before finalizing the job.\n",
            )
    except Exception as exc:
        return_code = 1
        append_job_log(job, f"[{utc_now()}] Integrated sequence download failed: {exc}\n")
    finally:
        root_logger.removeHandler(file_handler)
        root_logger.setLevel(previous_level)
        file_handler.flush()
        file_handler.close()
        log_handle.write(f"\n[{utc_now()}] Job finished with return code {return_code}\n")
        log_handle.close()

    finalize_integrated_sequence_job(job, return_code=return_code, cancellation_honored=False)


def request_job_cancellation(job: JobRecord) -> JobRecord:
    latest = load_job(job.id)
    latest.cancel_requested = True
    latest.updated_at = utc_now()
    if latest.status == "queued":
        latest.status = "cancelled"
        latest.pid = None
    save_job(latest)
    return latest


def notify_job_event(job: JobRecord, event: str, db: sqlite3.Connection | None = None) -> None:
    if job.owner_user_id is None:
        return
    recipient = get_user_email_by_id(job.owner_user_id, db)
    if not recipient:
        return
    try:
        send_job_notification_email(job, recipient, event)
    except Exception as exc:
        append_job_log(job, f"[{utc_now()}] Email notification failed for {event}: {exc}\n")


def recover_jobs_for_worker_startup() -> None:
    with get_sqlite_connection() as db:
        running_rows = db.execute("SELECT * FROM jobs WHERE status = 'running'").fetchall()
        now = utc_now()
        for row in running_rows:
            db.execute(
                """
                UPDATE jobs
                SET status = 'failed', pid = NULL, updated_at = ?, error = ?
                WHERE id = ?
                """,
                (now, "Worker restarted while the job was running.", row["id"]),
            )
            append_job_log(row_to_job(row), f"[{now}] Worker restarted before job completion.\n")
        db.commit()


def run_worker_loop() -> None:
    ensure_directories()
    worker_name = f"{os.uname().nodename}:{os.getpid()}"
    last_heartbeat = 0.0
    recovery_lock = acquire_startup_recovery_lock()
    with app.app_context():
        init_db()
        touch_worker_heartbeat(worker_name)
        last_heartbeat = time.time()
        if recovery_lock is not None and not has_other_live_worker_heartbeat(worker_name):
            try:
                recover_jobs_for_worker_startup()
                recover_species_for_worker_startup()
                recover_metadata_for_worker_startup()
                recover_discovery_scopes_for_worker_startup()
            finally:
                try:
                    fcntl.flock(recovery_lock.fileno(), fcntl.LOCK_UN)
                finally:
                    recovery_lock.close()

        while True:
            now = time.time()
            if now - last_heartbeat >= WORKER_HEARTBEAT_SECONDS:
                touch_worker_heartbeat(worker_name)
                last_heartbeat = now
            if WORKER_MODE in {"all", "sync"}:
                with get_sqlite_connection() as db:
                    catalog_build_schedule_hours = catalog_build_hours(db)
                    catalog_refresh_schedule_hours = catalog_refresh_hours(db)
                schedule_due_species_syncs(catalog_build_schedule_hours, catalog_refresh_schedule_hours)
                species = claim_next_species_sync(worker_name)
                if species is not None:
                    sync_species_record(species)
                    continue

            if WORKER_MODE in {"all", "sync", "metadata"}:
                with get_sqlite_connection() as db:
                    metadata_build_schedule_hours = metadata_build_hours(db)
                    metadata_refresh_schedule_hours = metadata_refresh_hours(db)
                schedule_due_metadata_builds(metadata_build_schedule_hours, metadata_refresh_schedule_hours)
                metadata_species = claim_next_species_metadata_build(worker_name)
                if metadata_species is not None:
                    build_species_metadata_record(metadata_species)
                    continue

            if WORKER_MODE in {"all", "sync", "jobs"}:
                job = claim_next_job(worker_name)
                if job is not None:
                    launch_job(job)
                    continue

            if WORKER_MODE in {"all", "discovery"}:
                with get_sqlite_connection() as db:
                    refresh_hours = discovery_refresh_hours(db)
                schedule_due_discovery_scope_syncs(refresh_hours)
                if refresh_hours is not None:
                    discovery_scope = claim_next_discovery_scope(worker_name)
                    if discovery_scope is not None:
                        sync_discovery_scope(discovery_scope)
                        continue

            time.sleep(WORKER_POLL_INTERVAL)


def collect_output_files(root: Path) -> list[str]:
    if not root.exists():
        return []
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
            files.append(str(path.relative_to(root)))
    return files


def prepare_sequence_download_artifacts(job: JobRecord) -> None:
    if job.mode != "seq":
        return

    output_root = Path(job.output_dir)
    if not output_root.exists():
        return

    fasta_files = sorted(
        path for path in output_root.glob("*.fna")
        if path.is_file()
    )

    if fasta_files:
        combined_path = output_root / "combined_sequences.fna"
        with combined_path.open("w", encoding="utf-8") as combined_handle:
            for fasta_path in fasta_files:
                combined_handle.write(fasta_path.read_text(encoding="utf-8"))
                if not combined_handle.tell():
                    continue
                combined_handle.write("\n")

    zip_path = output_root / "sequence_download_bundle.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output_root.rglob("*")):
            if not path.is_file():
                continue
            if path == zip_path:
                continue
            archive.write(path, arcname=str(path.relative_to(output_root)))


def summarize_sequence_download_assets(job: JobRecord, output_files: list[str]) -> dict[str, Any] | None:
    if job.mode != "seq":
        return None
    bundle_path = next((path for path in output_files if path == "sequence_download_bundle.zip"), None)
    combined_fasta_path = next((path for path in output_files if path == "combined_sequences.fna"), None)
    if not bundle_path and not combined_fasta_path:
        return None
    return {
        "bundle_path": bundle_path,
        "combined_fasta_path": combined_fasta_path,
    }


def summarize_grouped_sequence_outputs(job: JobRecord, output_files: list[str]) -> dict[str, Any] | None:
    filters = job.filters or {}
    if job.mode != "seq" or normalize_metadata_value(filters.get("grouping_mode")) != "field":
        return None
    group_field = normalize_sequence_group_field(filters.get("group_field"))
    grouped_prefix = f"grouped_by_{group_field}/"
    grouped_files = [path for path in output_files if path.startswith(grouped_prefix)]
    if not grouped_files:
        return {
            "group_field": group_field,
            "group_label": sequence_group_column(group_field),
            "group_count": 0,
            "group_names": [],
            "grouped_files": [],
            "summary_file": None,
        }

    group_names = sorted(
        {
            path[len(grouped_prefix) :].split("/", 1)[0]
            for path in grouped_files
            if "/" in path[len(grouped_prefix) :]
        }
    )
    summary_file = next((path for path in grouped_files if path.endswith("grouping_summary.txt")), None)
    visible_grouped_files = [path for path in grouped_files if path != summary_file]
    group_items: list[dict[str, Any]] = []
    for group_name in group_names:
        prefix = f"{grouped_prefix}{group_name}/"
        group_paths = [path for path in visible_grouped_files if path.startswith(prefix)]
        fasta_count = sum(1 for path in group_paths if path.endswith(".fna"))
        metadata_count = sum(1 for path in group_paths if path.endswith("metadata_subset.csv"))
        group_items.append(
            {
                "name": group_name,
                "slug": sequence_group_slug(group_name),
                "fasta_count": fasta_count,
                "metadata_count": metadata_count,
            }
        )
    return {
        "group_field": group_field,
        "group_label": sequence_group_column(group_field),
        "group_count": len(group_names),
        "group_names": group_names,
        "group_items": group_items,
        "grouped_files": visible_grouped_files,
        "summary_file": summary_file,
    }


def parse_log_timestamp(line: str) -> str | None:
    if line.startswith("[") and "]" in line:
        candidate = line[1 : line.index("]")]
        try:
            return datetime.fromisoformat(candidate).isoformat()
        except ValueError:
            pass

    prefix = line[:23]
    try:
        parsed = datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    except ValueError:
        return None


def format_elapsed_brief(seconds: float | int | None) -> str:
    if seconds is None:
        return "unknown"
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def job_last_activity_dt(job: JobRecord, log_text: str) -> datetime:
    candidates: list[datetime] = [parse_utc(job.updated_at)]
    log_path = Path(job.log_path)
    if log_path.exists():
        try:
            candidates.append(datetime.fromtimestamp(log_path.stat().st_mtime, tz=timezone.utc))
        except OSError:
            pass
    for line in reversed([line for line in log_text.splitlines() if line.strip()]):
        timestamp = parse_log_timestamp(line)
        if not timestamp:
            continue
        try:
            candidates.append(parse_utc(timestamp))
        except ValueError:
            pass
        break
    return max(candidates)


def job_stall_threshold_seconds(job: JobRecord) -> int:
    if job.status != "running":
        return 0
    if job.mode == "seq":
        return 10 * 60
    return 20 * 60


def line_message(line: str) -> str:
    if " - INFO - " in line:
        return line.split(" - INFO - ", 1)[1].strip()
    if " - WARNING - " in line:
        return line.split(" - WARNING - ", 1)[1].strip()
    if " - ERROR - " in line:
        return line.split(" - ERROR - ", 1)[1].strip()
    if "] " in line and line.startswith("["):
        return line.split("] ", 1)[1].strip()
    return line.strip()


def step_definitions_for_mode(mode: str) -> list[dict[str, Any]]:
    if mode == "seq":
        return [
            {"key": "queued", "label": "Queued", "matches": []},
            {"key": "worker", "label": "Worker started", "matches": ["Launching job"]},
            {"key": "input", "label": "Input loaded", "matches": ["No records match the provided filters.", "Wrote missing accessions", "Wrote an empty failure list", "Downloaded genome FASTA", "Genome FASTA already exists"]},
            {"key": "downloads", "label": "Sequence downloads", "matches": ["Downloaded genome FASTA", "Genome FASTA already exists", "Sequence downloading completed"]},
            {"key": "audit", "label": "Failure audit", "matches": ["Wrote failed or missing accessions", "No failed or missing accessions", "Wrote missing accessions", "Wrote an empty failure list"]},
            {"key": "finished", "label": "Finished", "matches": ["Job finished with return code 0"]},
        ]

    if mode == "run":
        return [
            {"key": "queued", "label": "Queued", "matches": []},
            {"key": "worker", "label": "Worker started", "matches": ["Launching job"]},
            {"key": "input", "label": "Input loaded and filtered", "matches": ["Data loaded successfully", "Data filtered with CheckM"]},
            {"key": "metadata", "label": "Metadata enrichment", "matches": ["Data saved to", "Output directories created"]},
            {"key": "summaries", "label": "Summaries and clean dataset", "matches": ["Metadata summary saved", "Assembly summary saved", "Annotation summary saved", "Filtered dataset saved"]},
            {"key": "figures", "label": "Figures generated", "matches": ["Bar plots saved", "Map plot saved", "Distribution plot saved", "Scatter plot saved"]},
            {"key": "downloads", "label": "Sequence downloads", "matches": ["Downloaded genome FASTA", "Genome FASTA already exists", "Sequence downloading completed"]},
            {"key": "finished", "label": "Finished", "matches": ["Script completed successfully.", "Job finished with return code 0"]},
        ]

    return [
        {"key": "queued", "label": "Queued", "matches": []},
        {"key": "worker", "label": "Worker started", "matches": ["Launching job"]},
        {"key": "input", "label": "Input loaded and filtered", "matches": ["Data loaded successfully", "Data filtered with CheckM"]},
        {"key": "metadata", "label": "Metadata enrichment", "matches": ["Data saved to", "Output directories created"]},
        {"key": "summaries", "label": "Summaries generated", "matches": ["Metadata summary saved", "Assembly summary saved", "Annotation summary saved", "Filtered dataset saved"]},
        {"key": "figures", "label": "Figures generated", "matches": ["Bar plots saved", "Map plot saved", "Distribution plot saved", "Scatter plot saved"]},
        {"key": "finished", "label": "Finished", "matches": ["Metadata generation completed. Sequence download not requested.", "Script completed successfully.", "Job finished with return code 0"]},
    ]


def summarize_job_progress(job: JobRecord, log_text: str) -> dict[str, Any]:
    steps = []
    definitions = step_definitions_for_mode(job.mode)
    lines = [line for line in log_text.splitlines() if line.strip()]

    for definition in definitions:
        timestamp = None
        detail = None
        for line in lines:
            if any(match in line for match in definition["matches"]):
                timestamp = parse_log_timestamp(line)
                detail = line_message(line)
                break
        steps.append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "timestamp": timestamp,
                "detail": detail,
                "state": "pending",
            }
        )

    if steps:
        steps[0]["timestamp"] = job.created_at
        steps[0]["detail"] = "Job was accepted and stored in the queue."
        steps[0]["state"] = "done"

    highest_completed = 0
    for index, step in enumerate(steps):
        if step["timestamp"]:
            highest_completed = index
            step["state"] = "done"

    if job.status == "running" and highest_completed + 1 < len(steps):
        steps[highest_completed + 1]["state"] = "current"
    elif job.status in {"queued"} and len(steps) > 1:
        steps[1]["state"] = "current"

    wait_state, wait_species = job_taxon_wait_state(job)

    if job.status == "completed":
        for step in steps:
            step["state"] = "done"
        percent = 100
        headline = "Job completed successfully."
    elif job.status == "cancelled":
        percent = max(10, int(((highest_completed + 1) / max(len(steps), 1)) * 100))
        headline = "Job was cancelled."
    elif job.status == "failed":
        percent = max(10, int(((highest_completed + 1) / max(len(steps), 1)) * 100))
        headline = "Job failed before finishing all pipeline steps."
    elif job.status == "running":
        percent = max(10, int(((highest_completed + 1) / max(len(steps), 1)) * 100))
        headline = steps[highest_completed + 1]["label"] if highest_completed + 1 < len(steps) else "Running"
    else:
        percent = 5
        headline = "Waiting for the background worker."
        if wait_state == "waiting" and wait_species is not None:
            headline = f"Waiting for managed taxon refresh: {wait_species.species_name}"
        elif wait_state == "failed":
            headline = "Managed taxon refresh failed before job start."

    failure_lines = []
    for line in lines:
        if " - ERROR - " in line or "Script failed:" in line or "failed:" in line.lower():
            failure_lines.append(line_message(line))

    failure_summary = None
    if job.status == "failed":
        failure_summary = failure_lines[-1] if failure_lines else job.error or "The job exited with an error. Check the log for details."
    elif wait_state == "failed" and wait_species is not None:
        failure_summary = wait_species.sync_error or "Managed taxon refresh failed before job start."

    recent_errors = failure_lines[-3:]
    snapshot: dict[str, Any] = {}
    if job.mode == "seq":
        output_dir = Path(job.output_dir)
        input_path = Path(job.input_path)
        total_targets = 0
        if input_path.exists():
            try:
                with input_path.open("r", encoding="utf-8", newline="") as handle:
                    total_targets = max(sum(1 for _ in handle) - 1, 0)
            except OSError:
                total_targets = 0
        downloaded_count = 0
        if output_dir.exists():
            downloaded_count = len(
                [
                    path
                    for path in output_dir.glob("*.fna")
                    if path.is_file() and path.name != "combined_sequences.fna"
                ]
            )
        snapshot = {
            "downloaded_count": downloaded_count,
            "total_targets": total_targets,
            "download_percent": round((downloaded_count / total_targets) * 100, 1) if total_targets else 0.0,
        }
    last_activity_dt = job_last_activity_dt(job, log_text)
    last_activity_age_seconds = max(0, int((utc_now_dt() - last_activity_dt).total_seconds()))
    stall_threshold_seconds = job_stall_threshold_seconds(job)
    stalled = bool(stall_threshold_seconds and last_activity_age_seconds >= stall_threshold_seconds)
    if stalled and job.status == "running":
        headline = f"Possibly stalled: {headline}"
    return {
        "percent": percent,
        "headline": headline,
        "steps": steps,
        "failure_summary": failure_summary,
        "recent_errors": recent_errors,
        "snapshot": snapshot,
        "last_activity_at": last_activity_dt.isoformat(),
        "last_activity_age_seconds": last_activity_age_seconds,
        "last_activity_age_label": format_elapsed_brief(last_activity_age_seconds),
        "stall_threshold_seconds": stall_threshold_seconds,
        "stalled": stalled,
    }


def cleanup_old_jobs(*, older_than_days: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    removable_statuses = {"completed", "failed", "cancelled"}
    removed = 0
    db = get_db()
    rows = db.execute("SELECT id, status, updated_at FROM jobs").fetchall()
    for row in rows:
        status = str(row["status"])
        if status not in removable_statuses:
            continue
        try:
            updated_at = parse_utc(str(row["updated_at"]))
        except ValueError:
            continue
        if updated_at > cutoff:
            continue
        db.execute("DELETE FROM jobs WHERE id = ?", (row["id"],))
        job_root = job_dir(str(row["id"]))
        if job_root.exists():
            for path in sorted(job_root.rglob("*"), reverse=True):
                if path.is_file() or path.is_symlink():
                    path.unlink(missing_ok=True)
                elif path.is_dir():
                    path.rmdir()
            job_root.rmdir()
        removed += 1
    db.commit()
    return removed


def list_metadata_claims(limit: int = 12) -> list[dict[str, Any]]:
    rows = get_db().execute(
        """
        SELECT id, species_name, taxon_rank, genome_count, metadata_status,
               metadata_claimed_by, metadata_claimed_at, metadata_error
        FROM species
        WHERE metadata_status = 'building'
           OR metadata_claimed_at IS NOT NULL
        ORDER BY COALESCE(metadata_claimed_at, updated_at) ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    now = utc_now_dt()
    for row in rows:
        claimed_at = str(row["metadata_claimed_at"]) if row["metadata_claimed_at"] else None
        age_seconds = None
        if claimed_at:
            try:
                age_seconds = int((now - parse_utc(claimed_at)).total_seconds())
            except ValueError:
                age_seconds = None
        worker_name = normalize_metadata_value(row["metadata_claimed_by"]) or None
        items.append(
            {
                "id": int(row["id"]),
                "species_name": str(row["species_name"]),
                "taxon_rank": normalize_taxon_rank(row["taxon_rank"]),
                "genome_count": int(row["genome_count"] or 0),
                "status": str(row["metadata_status"]),
                "worker_name": worker_name,
                "worker_live": worker_heartbeat_is_live(worker_name) if worker_name else False,
                "claimed_at": claimed_at,
                "age_seconds": age_seconds,
                "age_label": format_elapsed_brief(age_seconds),
                "error": str(row["metadata_error"]) if row["metadata_error"] else None,
            }
        )
    return items


def prune_stale_worker_heartbeats() -> int:
    removed = 0
    now = time.time()
    stale_cutoff_seconds = max(WORKER_HEARTBEAT_STALE_SECONDS * 20, 600.0)
    for path in worker_heartbeat_dir().glob("*.heartbeat"):
        try:
            age_seconds = now - path.stat().st_mtime
        except FileNotFoundError:
            continue
        if age_seconds < stale_cutoff_seconds:
            continue
        try:
            path.unlink()
            removed += 1
        except FileNotFoundError:
            continue
    return removed


def list_worker_snapshots(*, include_stale: bool = False) -> list[dict[str, Any]]:
    rows = get_db().execute(
        """
        SELECT claimed_by, COUNT(*) AS job_count
        FROM jobs
        WHERE status = 'running' AND claimed_by IS NOT NULL
        GROUP BY claimed_by
        """
    ).fetchall()
    running_jobs = {
        normalize_metadata_value(row["claimed_by"]): int(row["job_count"] or 0)
        for row in rows
        if normalize_metadata_value(row["claimed_by"])
    }
    rows = get_db().execute(
        """
        SELECT metadata_claimed_by, COUNT(*) AS build_count
        FROM species
        WHERE metadata_status = 'building' AND metadata_claimed_by IS NOT NULL
        GROUP BY metadata_claimed_by
        """
    ).fetchall()
    metadata_builds = {
        normalize_metadata_value(row["metadata_claimed_by"]): int(row["build_count"] or 0)
        for row in rows
        if normalize_metadata_value(row["metadata_claimed_by"])
    }

    snapshots: list[dict[str, Any]] = []
    now = time.time()
    for path in sorted(worker_heartbeat_dir().glob("*.heartbeat"), key=lambda item: item.name):
        try:
            age_seconds = int(max(0.0, now - path.stat().st_mtime))
        except FileNotFoundError:
            continue
        worker_name = path.stem.replace("_", ":", 1)
        alive = age_seconds <= WORKER_HEARTBEAT_STALE_SECONDS
        if not include_stale and not alive:
            continue
        snapshots.append(
            {
                "worker_name": worker_name,
                "alive": alive,
                "age_seconds": age_seconds,
                "age_label": format_elapsed_brief(age_seconds),
                "running_jobs": running_jobs.get(worker_name, 0),
                "metadata_builds": metadata_builds.get(worker_name, 0),
            }
        )
    return snapshots


def build_observability_dashboard() -> dict[str, Any]:
    stale_heartbeat_files_removed = prune_stale_worker_heartbeats()
    jobs = list_all_jobs()
    active_jobs = [job for job in jobs if job.status in {"queued", "running"}][:20]
    active_job_cards = []
    stalled_job_cards = []
    for job in active_jobs:
        log_path = Path(job.log_path)
        log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
        progress = summarize_job_progress(job, log_text)
        item = {"job": job, "progress": progress}
        active_job_cards.append(item)
        if progress.get("stalled"):
            stalled_job_cards.append(item)

    worker_snapshots = list_worker_snapshots(include_stale=False)
    stale_worker_snapshots = list_worker_snapshots(include_stale=True)
    metadata_claims = list_metadata_claims()
    return {
        "worker_snapshots": worker_snapshots,
        "stale_worker_total": max(0, len(stale_worker_snapshots) - len(worker_snapshots)),
        "stale_heartbeat_files_removed": stale_heartbeat_files_removed,
        "metadata_claims": metadata_claims,
        "active_job_cards": active_job_cards,
        "stalled_job_cards": stalled_job_cards,
        "live_worker_total": sum(1 for item in worker_snapshots if item["alive"]),
        "worker_total": len(worker_snapshots),
        "running_job_total": sum(1 for job in active_jobs if job.status == "running"),
        "queued_job_total": sum(1 for job in active_jobs if job.status == "queued"),
        "active_job_total": len(active_job_cards),
        "stalled_job_total": len(stalled_job_cards),
        "active_metadata_total": len(metadata_claims),
    }


def requeue_stuck_species_syncs(*, older_than_minutes: int) -> int:
    cutoff = utc_now_dt() - timedelta(minutes=older_than_minutes)
    updated_at = utc_now()
    db = get_db()
    rows = db.execute(
        """
        SELECT id
        FROM species
        WHERE claimed_at IS NOT NULL
          AND (status = 'syncing' OR (status = 'ready' AND tsv_path IS NOT NULL))
        """
    ).fetchall()

    requeued = 0
    for row in rows:
        species_id = int(row["id"])
        raw = db.execute("SELECT claimed_at FROM species WHERE id = ?", (species_id,)).fetchone()
        claimed_at_value = str(raw["claimed_at"]) if raw and raw["claimed_at"] else None
        if not claimed_at_value:
            continue
        try:
            claimed_at = parse_utc(claimed_at_value)
        except ValueError:
            claimed_at = cutoff - timedelta(seconds=1)
        if claimed_at > cutoff:
            continue
        db.execute(
            """
            UPDATE species
            SET status = CASE WHEN tsv_path IS NOT NULL THEN 'ready' ELSE 'pending' END,
                updated_at = ?,
                claimed_by = NULL,
                claimed_at = NULL,
                sync_first_claimed_at = NULL,
                sync_attempt_count = 0,
                refresh_requested = 1,
                claim_token = claim_token + 1,
                sync_error = COALESCE(sync_error, 'Requeued from admin after stuck syncing timeout.')
            WHERE id = ?
            """,
            (updated_at, species_id),
        )
        requeued += 1

    db.commit()
    return requeued


def is_authenticated() -> bool:
    return current_user() is not None


def require_auth() -> Any:
    if request.endpoint in PUBLIC_ENDPOINTS:
        return None
    if is_authenticated():
        return None
    return redirect(url_for("login", next=request.path))


def require_job_owner(job_id: str) -> JobRecord:
    try:
        job = refresh_job(load_job(job_id))
    except FileNotFoundError:
        abort(404)
    user = g.current_user
    if user is None:
        abort(404)
    if not is_admin_user(user) and job.owner_user_id != int(user["id"]):
        abort(404)
    return job


def require_admin() -> sqlite3.Row:
    user = g.current_user
    if user is None or not is_admin_user(user):
        abort(403)
    return user


@app.before_request
def load_current_user() -> None:
    g.current_user = current_user()
    g.is_admin = is_admin_user(g.current_user)


@app.before_request
def enforce_auth() -> Any:
    return require_auth()


@app.route("/login", methods=["GET", "POST"])
def login() -> Any:
    if g.current_user is not None:
        return redirect(url_for("index"))

    if request.method == "POST":
        username = normalize_username(request.form.get("username") or "")
        password = request.form.get("password") or ""
        user = get_user_by_username(username)
        if user is not None and check_password_hash(user["password_hash"], password):
            login_user(user)
            flash("Signed in.", "success")
            target = request.args.get("next") or url_for("index")
            return redirect(target)
        flash("Invalid username or password.", "error")

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register() -> Any:
    if g.current_user is not None:
        return redirect(url_for("index"))

    if request.method == "POST":
        username = normalize_username(request.form.get("username") or "")
        email = normalize_email(request.form.get("email") or "")
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if len(username) < 3:
            flash("Username must be at least 3 characters.", "error")
        elif "@" not in email or "." not in email:
            flash("Enter a valid email address.", "error")
        elif get_user_by_username(username) is not None:
            flash("That username is already taken.", "error")
        elif get_user_by_email(email) is not None:
            flash("That email address is already registered.", "error")
        else:
            password_error = validate_passwords(password, confirm)
            if password_error:
                flash(password_error, "error")
            else:
                user = create_user(username, email, password)
                login_user(user)
                flash("Account created.", "success")
                return redirect(url_for("index"))

    return render_template("register.html")


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password() -> Any:
    if request.method == "POST":
        identifier = (request.form.get("identifier") or "").strip()
        user = get_user_by_email(identifier) or get_user_by_username(identifier)
        if user is None:
            flash("No account matched that username or email.", "error")
        else:
            token = create_reset_token(int(user["id"]))
            reset_url = url_for("reset_password", token=token, _external=True)
            try:
                send_reset_email(str(user["email"]), str(user["username"]), reset_url)
                flash("Password reset email sent.", "success")
            except Exception as exc:
                flash(f"Password reset email could not be sent: {exc}", "error")
    return render_template("forgot_password.html", mail_configured=mail_is_configured())


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token: str) -> Any:
    token_row = get_valid_reset_token(token)
    if token_row is None:
        flash("That reset link is invalid or expired.", "error")
        return redirect(url_for("forgot_password"))

    if request.method == "POST":
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""
        password_error = validate_passwords(password, confirm)
        if password_error:
            flash(password_error, "error")
        else:
            update_user_password(int(token_row["user_id"]), password)
            mark_reset_token_used(token)
            flash("Password updated. You can sign in now.", "success")
            return redirect(url_for("login"))

    return render_template("reset_password.html", token=token, username=token_row["username"])


@app.route("/logout", methods=["POST"])
def logout() -> Any:
    logout_user()
    flash("Logged out.", "success")
    return redirect(url_for("login"))


@app.route("/")
def index() -> str:
    user = g.current_user
    assert user is not None
    jobs = list_jobs_for_user(int(user["id"]))
    taxa = list_available_species()
    taxa_json = [
        {
            "id": item.id,
            "species_name": item.species_name,
            "taxon_rank": item.taxon_rank,
            "genome_count": item.genome_count or 0,
            "assembly_source": item.assembly_source,
        }
        for item in taxa
    ]
    return render_template(
        "index_dashboard.html",
        jobs=jobs,
        modes=MODES,
        taxa=taxa,
        taxa_json=taxa_json,
        home_metrics=build_public_home_metrics(),
        taxon_recent_hours=TAXON_RECENT_HOURS,
        taxon_very_old_hours=TAXON_VERY_OLD_HOURS,
    )


@app.route("/report-problem", methods=["POST"])
def report_problem() -> Any:
    user = g.current_user
    assert user is not None
    message = (request.form.get("message") or "").strip()
    requested_action = (request.form.get("requested_action") or "").strip() or None
    selected_taxon_name = (request.form.get("selected_taxon_name") or "").strip() or None
    taxon_id_raw = (request.form.get("selected_taxon_id") or "").strip()
    taxon_id = int(taxon_id_raw) if taxon_id_raw.isdigit() else None
    taxon_name = selected_taxon_name
    if taxon_id is not None:
        species = get_species_by_id(taxon_id)
        if species is not None:
            taxon_name = species.species_name
    if len(message) < 12:
        flash("Please include a short description of the problem.", "error")
        return redirect(url_for("index"))

    create_problem_report(
        user_id=int(user["id"]),
        username=str(user["username"]),
        email=str(user["email"]) if user["email"] else None,
        taxon_id=taxon_id,
        taxon_name=taxon_name,
        requested_action=requested_action,
        message=message,
    )
    if mail_is_configured():
        try:
            send_problem_report_email(
                username=str(user["username"]),
                email=str(user["email"]) if user["email"] else None,
                taxon_name=taxon_name,
                requested_action=requested_action,
                message=message,
            )
        except Exception:
            pass
    flash("Problem report submitted. It is now visible in admin.", "success")
    return redirect(url_for("index"))


@app.route("/admin")
def admin_dashboard() -> str:
    require_admin()
    backfill = build_backfill_dashboard()
    return render_template(
        "admin_overview.html",
        backfill=backfill,
        metadata_dashboard=build_metadata_dashboard(),
        observability=build_observability_dashboard(),
        **admin_common_context("overview"),
    )


@app.route("/admin/catalog")
def admin_catalog() -> str:
    require_admin()
    return render_template(
        "admin_catalog.html",
        taxa=list_all_species(),
        **admin_common_context("catalog"),
    )


@app.route("/admin/discovery")
def admin_discovery() -> str:
    require_admin()
    return render_template(
        "admin_discovery.html",
        discovery_scopes=list_discovery_scopes(),
        **admin_common_context("discovery"),
    )


@app.route("/admin/metadata")
def admin_metadata() -> str:
    require_admin()
    return render_template(
        "admin_metadata.html",
        taxa=list_all_species(),
        metadata_dashboard=build_metadata_dashboard(),
        **admin_common_context("metadata"),
    )


@app.route("/admin/jobs")
def admin_jobs() -> str:
    require_admin()
    query = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "all").strip()
    return render_template(
        "admin_jobs.html",
        jobs=filter_admin_jobs(list_all_jobs(), query, status_filter),
        admin_query=query,
        status_filter=status_filter,
        **admin_common_context("jobs"),
    )


@app.route("/admin/users")
def admin_users() -> str:
    require_admin()
    query = (request.args.get("q") or "").strip()
    return render_template(
        "admin_users.html",
        users=filter_admin_users(list_all_users(), query),
        admin_query=query,
        **admin_common_context("users"),
    )


@app.route("/admin/problems")
def admin_problems() -> str:
    require_admin()
    return render_template(
        "admin_problems.html",
        reports=list_problem_reports(),
        **admin_common_context("problems"),
    )


@app.route("/admin/problems/<int:report_id>/status", methods=["POST"])
def admin_problem_status(report_id: int) -> Any:
    require_admin()
    status = (request.form.get("status") or "open").strip()
    note = (request.form.get("admin_note") or "").strip() or None
    if update_problem_report_status(report_id, status, note):
        flash(f"Problem report {report_id} updated.", "success")
    else:
        flash(f"Problem report {report_id} was not found.", "error")
    return redirect(url_for("admin_problems"))


@app.route("/admin/discovery-policy", methods=["POST"])
def admin_set_discovery_policy() -> Any:
    require_admin()
    policy = normalize_discovery_policy(request.form.get("discovery_policy"))
    set_setting("discovery_policy", policy)
    flash(f"Discovery policy set to {DISCOVERY_POLICIES[policy]['label']}.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/catalog-policy", methods=["POST"])
def admin_set_catalog_policy() -> Any:
    require_admin()
    build_policy = normalize_catalog_policy(request.form.get("catalog_build_policy"))
    refresh_policy = normalize_catalog_policy(request.form.get("catalog_refresh_policy"))
    set_setting("catalog_build_policy", build_policy)
    set_setting("catalog_refresh_policy", refresh_policy)
    if refresh_policy == "paused":
        db = get_db()
        db.execute(
            """
            UPDATE species
            SET status = CASE
                    WHEN tsv_path IS NOT NULL AND claimed_at IS NULL THEN 'ready'
                    ELSE status
                END,
                refresh_requested = 0,
                updated_at = ?
            WHERE tsv_path IS NOT NULL
              AND (refresh_requested = 1 OR status = 'pending')
            """,
            (utc_now(),),
        )
        db.commit()
    flash(
        f"Catalog build policy set to {CATALOG_POLICIES[build_policy]['label']} and refresh policy set to {CATALOG_POLICIES[refresh_policy]['label']}.",
        "success",
    )
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/metadata-policy", methods=["POST"])
def admin_set_metadata_policy() -> Any:
    require_admin()
    build_policy = normalize_metadata_policy(request.form.get("metadata_build_policy"))
    refresh_policy = normalize_metadata_policy(request.form.get("metadata_refresh_policy"))
    set_setting("metadata_build_policy", build_policy)
    set_setting("metadata_refresh_policy", refresh_policy)
    if refresh_policy == "paused":
        db = get_db()
        db.execute(
            """
            UPDATE species
            SET metadata_status = CASE
                    WHEN metadata_path IS NOT NULL AND metadata_claimed_at IS NULL THEN 'ready'
                    ELSE metadata_status
                END,
                metadata_refresh_requested = 0,
                updated_at = ?
            WHERE metadata_path IS NOT NULL
              AND (metadata_refresh_requested = 1 OR metadata_status = 'pending')
            """,
            (utc_now(),),
        )
        db.commit()
    flash(
        f"Metadata build policy set to {METADATA_POLICIES[build_policy]['label']} and refresh policy set to {METADATA_POLICIES[refresh_policy]['label']}.",
        "success",
    )
    return redirect(url_for("admin_metadata"))


@app.route("/admin/system-monitor", methods=["POST"])
def admin_set_system_monitor() -> Any:
    require_admin()
    enabled = "1" if request.form.get("system_temp_alert_enabled") == "1" else "0"
    email = (request.form.get("system_temp_alert_email") or "").strip()
    threshold_raw = (request.form.get("system_temp_alert_threshold_c") or "80").strip()
    cooldown_raw = (request.form.get("system_temp_alert_cooldown_minutes") or "60").strip()
    try:
        threshold = max(1.0, float(threshold_raw))
    except ValueError:
        threshold = 80.0
    try:
        cooldown = max(1, int(cooldown_raw))
    except ValueError:
        cooldown = 60
    set_setting("system_temp_alert_enabled", enabled)
    set_setting("system_temp_alert_email", email)
    set_setting("system_temp_alert_threshold_c", str(threshold))
    set_setting("system_temp_alert_cooldown_minutes", str(cooldown))
    flash("System monitor settings updated.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/species", methods=["POST"])
def admin_add_species() -> Any:
    require_admin()
    name = request.form.get("taxon_name") or ""
    taxon_rank = normalize_taxon_rank(request.form.get("taxon_rank"))
    assembly_source = normalize_assembly_source(request.form.get("assembly_source"))
    try:
        species = create_species(name, assembly_source=assembly_source, taxon_rank=taxon_rank)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("admin_catalog"))

    if species.assembly_source != assembly_source:
        species.assembly_source = assembly_source
        species.updated_at = utc_now()
        save_species(species)
    if species.status == "ready":
        flash(f"{species.species_name} is already available in the catalog.", "success")
    else:
        request_species_sync(species)
        flash(f"Added {species.taxon_rank} {species.species_name}. Background sync requested.", "success")
    return redirect(url_for("admin_catalog"))


@app.route("/admin/discovery-scopes", methods=["POST"])
def admin_add_discovery_scope() -> Any:
    require_admin()
    scope_value = request.form.get("scope_value") or ""
    scope_label = request.form.get("scope_label") or ""
    target_rank = normalize_taxon_rank(request.form.get("target_rank"))
    assembly_source = normalize_assembly_source(request.form.get("assembly_source"))
    try:
        scope = create_discovery_scope(
            scope_value,
            label=scope_label or None,
            assembly_source=assembly_source,
            target_rank=target_rank,
        )
        if scope.target_rank != target_rank:
            scope.target_rank = target_rank
            scope.updated_at = utc_now()
            save_discovery_scope(scope)
        if scope.assembly_source != assembly_source:
            scope.assembly_source = assembly_source
            scope.updated_at = utc_now()
            save_discovery_scope(scope)
        request_discovery_scope_refresh(scope)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("admin_discovery"))
    flash(f"Discovery scope {scope.scope_label} for {scope.target_rank} discovery added or refreshed.", "success")
    return redirect(url_for("admin_discovery"))


@app.route("/admin/discovery-scopes/<int:scope_id>/sync", methods=["POST"])
def admin_sync_discovery_scope(scope_id: int) -> Any:
    require_admin()
    scope = load_discovery_scope(scope_id)
    request_discovery_scope_refresh(scope)
    flash(f"Discovery refresh requested for {scope.scope_label}.", "success")
    return redirect(url_for("admin_discovery"))


@app.route("/admin/species/<int:species_id>/sync", methods=["POST"])
def admin_sync_species(species_id: int) -> Any:
    require_admin()
    species = load_species(species_id)
    request_species_sync(species)
    flash(f"Refresh requested for {species.species_name}.", "success")
    return redirect(url_for("admin_catalog"))


@app.route("/admin/species/sync-all", methods=["POST"])
def admin_sync_all_species() -> Any:
    require_admin()
    species_list = list_all_species()
    for species in species_list:
        request_species_sync(species)
    flash(f"Refresh requested for {len(species_list)} taxa.", "success")
    return redirect(url_for("admin_catalog"))


@app.route("/admin/species/requeue-stuck", methods=["POST"])
def admin_requeue_stuck_species() -> Any:
    require_admin()
    older_than_minutes = max(1, int(request.form.get("older_than_minutes") or "30"))
    requeued = requeue_stuck_species_syncs(older_than_minutes=older_than_minutes)
    flash(f"Requeued {requeued} stuck taxa older than {older_than_minutes} minutes.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/metadata/queue-missing", methods=["POST"])
def admin_queue_missing_metadata() -> Any:
    require_admin()
    queued = 0
    for species in list_all_species():
        if species.status != "ready" or not species.tsv_path:
            continue
        if species.metadata_status == "missing":
            request_species_metadata_build(species)
            queued += 1
    flash(f"Queued metadata builds for {queued} missing taxa.", "success")
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/refresh-all", methods=["POST"])
def admin_refresh_all_metadata() -> Any:
    require_admin()
    queued = 0
    for species in list_all_species():
        if species.status != "ready" or not species.tsv_path:
            continue
        request_species_metadata_build(species)
        queued += 1
    flash(f"Queued metadata refresh for {queued} taxa.", "success")
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/reset-stale-failures", methods=["POST"])
def admin_reset_stale_metadata_failures() -> Any:
    require_admin()
    reset_count = reset_stale_metadata_failures()
    flash(f"Reset {reset_count} stale metadata failures caused by worker restarts.", "success")
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/<int:species_id>/refresh", methods=["POST"])
def admin_refresh_species_metadata(species_id: int) -> Any:
    require_admin()
    species = load_species(species_id)
    request_species_metadata_build(species)
    flash(f"Queued metadata refresh for {species.species_name}.", "success")
    return redirect(url_for("admin_metadata"))


@app.route("/jobs", methods=["POST"])
def create_job() -> Any:
    user = g.current_user
    assert user is not None

    mode = request.form.get("mode", "").strip()
    if mode not in MODES:
        abort(400, "Unknown mode.")

    if mode == "metadata":
        metadata_input_mode = (request.form.get("metadata_input_mode") or "species").strip()
        if metadata_input_mode == "upload":
            flash("Upload-based legacy metadata jobs are no longer supported. Use the managed taxon workflow.", "error")
            return redirect(url_for("index"))
        if metadata_input_mode != "upload":
            refresh_before_run = (request.form.get("refresh_before_run") or "") == "1"
            taxon_id_raw = (request.form.get("taxon_id") or "").strip()
            if not taxon_id_raw:
                flash("Select a taxon from the managed catalog or switch to TSV upload.", "error")
                return redirect(url_for("index"))
            try:
                species = load_species(int(taxon_id_raw))
            except (ValueError, FileNotFoundError):
                flash("Selected taxon was not found.", "error")
                return redirect(url_for("index"))
            if species.status != "ready" or not species.tsv_path or not Path(species.tsv_path).exists():
                flash("That taxon is not ready yet. Wait for the background sync to finish.", "error")
                return redirect(url_for("index"))

            has_metadata = bool(
                species.metadata_clean_path
                and Path(species.metadata_clean_path).exists()
                and species.metadata_path
                and Path(species.metadata_path).exists()
            )

            if refresh_before_run:
                request_species_sync(species)
                if has_metadata:
                    flash(
                        f"Queued a fresh TSV refresh for {species.species_name}. "
                        "The current metadata summary is open now; reload it later for the refreshed view.",
                        "success",
                    )
                    return redirect(url_for("taxon_metadata", species_id=species.id))
                flash(
                    f"Queued a fresh TSV refresh for {species.species_name}. "
                    "Metadata analysis will be available after the refresh and metadata rebuild complete.",
                    "success",
                )
                return redirect(url_for("index"))

            if has_metadata:
                if is_taxon_very_old(species):
                    flash(
                        f"{species.species_name} is using stored metadata from a cached TSV that is {taxon_freshness_label(species)}. "
                        "Enable refresh if you need the newest genomes first.",
                        "warning",
                    )
                elif not is_taxon_recent_enough(species):
                    flash(
                        f"{species.species_name} is using stored metadata from a cached TSV that is {taxon_freshness_label(species)}.",
                        "warning",
                    )
                return redirect(url_for("taxon_metadata", species_id=species.id))

            request_species_metadata_build(species)
            flash(
                f"Metadata for {species.species_name} is not ready yet. A metadata build was queued from the current managed TSV.",
                "success",
            )
            return redirect(url_for("index"))

    if mode == "run":
        flash("The legacy full pipeline job is no longer supported. Use Metadata Analysis and Sequence Download separately.", "error")
        return redirect(url_for("index"))

    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("index"))

    job_id = uuid.uuid4().hex[:12]
    root = job_dir(job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    filters: dict[str, Any] = {}

    if mode == "metadata":
        metadata_input_mode = (request.form.get("metadata_input_mode") or "species").strip()
        if metadata_input_mode == "upload":
            uploaded = request.files.get("input_file")
            if not uploaded or not uploaded.filename:
                flash("Upload a TSV file or switch back to the species catalog.", "error")
                return redirect(url_for("index"))
            expected_extension = MODES["metadata"]["input_extension"]
            if not allowed_extension(uploaded.filename, expected_extension):
                flash(f"{mode} mode requires a {expected_extension} file.", "error")
                return redirect(url_for("index"))
            safe_name = secure_filename(uploaded.filename)
            input_path = uploads_dir / safe_name
            uploaded.save(input_path)
            filters["input_source"] = "upload"
        else:
            refresh_before_run = (request.form.get("refresh_before_run") or "") == "1"
            taxon_id_raw = (request.form.get("taxon_id") or "").strip()
            if not taxon_id_raw:
                flash("Select a taxon from the managed catalog or switch to TSV upload.", "error")
                return redirect(url_for("index"))
            try:
                species = load_species(int(taxon_id_raw))
            except (ValueError, FileNotFoundError):
                flash("Selected taxon was not found.", "error")
                return redirect(url_for("index"))
            if species.status != "ready" or not species.tsv_path or not Path(species.tsv_path).exists():
                flash("That taxon is not ready yet. Wait for the background sync to finish.", "error")
                return redirect(url_for("index"))
            safe_name = f"{species.species_name} ({species.taxon_rank}, managed TSV)"
            input_path = Path(species.tsv_path)
            filters["input_source"] = "taxon"
            filters["taxon_id"] = species.id
            filters["taxon_name"] = species.species_name
            filters["taxon_rank"] = species.taxon_rank
            filters["taxon_last_synced_at"] = species.last_synced_at
            filters["refresh_before_run"] = refresh_before_run
            if refresh_before_run:
                request_species_sync(species)
                flash(
                    f"Queued a fresh metadata sync for {species.species_name}. "
                    "Your job will start after that refresh completes.",
                    "success",
                )
            elif is_taxon_very_old(species):
                flash(
                    f"{species.species_name} is using a cached TSV that is {taxon_freshness_label(species)}. "
                    "Enable refresh before run if you need the newest genomes first.",
                    "warning",
                )
            elif not is_taxon_recent_enough(species):
                flash(
                    f"{species.species_name} is using a cached TSV that is {taxon_freshness_label(species)}.",
                    "warning",
                )
    else:
        uploaded = request.files.get("input_file")
        if not uploaded or not uploaded.filename:
            flash("An input file is required.", "error")
            return redirect(url_for("index"))

        expected_extension = MODES[mode]["input_extension"]
        if not allowed_extension(uploaded.filename, expected_extension):
            flash(f"{mode} mode requires a {expected_extension} file.", "error")
            return redirect(url_for("index"))

        safe_name = secure_filename(uploaded.filename)
        input_path = uploads_dir / safe_name
        uploaded.save(input_path)
        filters["input_source"] = "upload"

    command, command_filters = build_command(mode, input_path, outputs_dir, request.form)
    filters.update(command_filters)
    record = JobRecord(
        id=job_id,
        mode=mode,
        status="queued",
        created_at=utc_now(),
        updated_at=utc_now(),
        input_name=safe_name,
        input_path=str(input_path),
        output_dir=str(outputs_dir),
        log_path=str(root / LOG_FILE_NAME),
        command=command,
        owner_user_id=int(user["id"]),
        owner_username=str(user["username"]),
        filters=filters,
    )
    save_job(record)
    notify_job_event(record, "submitted")
    flash(f"Job {job_id} submitted.", "success")
    return redirect(url_for("job_detail", job_id=job_id))


@app.route("/jobs", methods=["GET"])
def user_jobs() -> str:
    user = g.current_user
    assert user is not None
    jobs = list_jobs_for_user(int(user["id"]))
    active_jobs = [job for job in jobs if job.status in {"queued", "running"}]
    finished_jobs = [job for job in jobs if job.status not in {"queued", "running"}]
    active_job_cards = []
    finished_job_cards = []
    for job in active_jobs:
        log_text = Path(job.log_path).read_text(encoding="utf-8") if Path(job.log_path).exists() else ""
        active_job_cards.append({"job": job, "progress": summarize_job_progress(job, log_text)})
    for job in finished_jobs:
        log_text = Path(job.log_path).read_text(encoding="utf-8") if Path(job.log_path).exists() else ""
        finished_job_cards.append({"job": job, "progress": summarize_job_progress(job, log_text)})
    return render_template(
        "user_jobs.html",
        jobs=jobs,
        active_jobs=active_jobs,
        finished_jobs=finished_jobs,
        active_job_cards=active_job_cards,
        finished_job_cards=finished_job_cards,
    )


@app.route("/jobs/<job_id>")
def job_detail(job_id: str) -> str:
    job = require_job_owner(job_id)
    log_text = Path(job.log_path).read_text(encoding="utf-8") if Path(job.log_path).exists() else ""
    output_files = collect_output_files(Path(job.output_dir))
    grouped_output_summary = summarize_grouped_sequence_outputs(job, output_files)
    sequence_download_assets = summarize_sequence_download_assets(job, output_files)
    progress = summarize_job_progress(job, log_text)
    return render_template(
        "job_detail.html",
        job=job,
        log_text=log_text[-40000:],
        output_files=output_files,
        grouped_output_summary=grouped_output_summary,
        sequence_download_assets=sequence_download_assets,
        progress=progress,
    )


def render_taxon_metadata_section(species_id: int, section: str) -> str:
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if species.status != "ready" or not species.tsv_path:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Metadata analysis is not ready for that taxon yet.", "error")
        return redirect(url_for("index"))
    metadata_sections = metadata_sections_for_species(species)
    if section not in metadata_sections:
        abort(404)
    analysis = load_taxon_metadata_analysis(species)
    return render_template(
        "taxon_metadata.html",
        species=species,
        analysis=analysis,
        active_section=section,
        metadata_sections=metadata_sections,
    )


@app.route("/taxa/<int:species_id>/metadata")
def taxon_metadata(species_id: int) -> str:
    return render_taxon_metadata_section(species_id, "summary")


@app.route("/taxa/<int:species_id>/metadata/<section>")
def taxon_metadata_section(species_id: int, section: str) -> str:
    return render_taxon_metadata_section(species_id, section)


@app.route("/taxa/<int:species_id>/metadata/download")
def download_taxon_metadata(species_id: int):
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Metadata download is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_metadata", species_id=species_id))
    clean_path = Path(species.metadata_clean_path)
    return send_from_directory(clean_path.parent, clean_path.name, as_attachment=True)


@app.route("/taxa/<int:species_id>/metadata/download-bundle")
def download_taxon_metadata_bundle(species_id: int):
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Metadata bundle is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_metadata", species_id=species_id))
    analysis = load_taxon_metadata_analysis(species)
    payload = build_analysis_bundle(species, analysis)
    return app.response_class(
        payload,
        mimetype="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={species.slug}_metadata_bundle.zip",
        },
    )


@app.route("/taxa/<int:species_id>/sequences")
def taxon_sequences(species_id: int) -> str:
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if species.status != "ready" or not species.tsv_path:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))

    metadata_ready = bool(
        species.metadata_clean_path
        and Path(species.metadata_clean_path).exists()
        and species.metadata_path
        and Path(species.metadata_path).exists()
    )
    if not metadata_ready:
        if species.metadata_status != "building":
            species = request_species_metadata_build(species)
        return render_template(
            "taxon_sequences_pending.html",
            species=species,
            pending_state_label=(species.metadata_status or "pending").capitalize(),
        )

    sequence_dashboard = build_taxon_sequence_dashboard(species, request.args)
    return render_template(
        "taxon_sequences.html",
        species=species,
        sequence_dashboard=sequence_dashboard,
        preview_columns=sequence_dashboard["preview_columns"],
        preview_rows=sequence_dashboard["preview_rows"],
    )


@app.route("/taxa/<int:species_id>/sequences/metadata.csv", methods=["POST"])
def download_taxon_sequence_metadata_subset(species_id: int):
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Combined metadata is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_sequences", species_id=species_id))
    sequence_dashboard = build_taxon_sequence_dashboard(species, request.form)
    filtered_frame = sequence_dashboard["filtered_frame"]
    if filtered_frame.empty:
        flash("The current filters do not match any genomes.", "error")
        return redirect(url_for("taxon_sequences", species_id=species_id, **request.form))
    payload = filtered_frame.to_csv(index=False)
    filename = f"{species.slug}_sequence_subset.csv"
    return app.response_class(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/taxa/<int:species_id>/sequences/jobs", methods=["POST"])
def create_taxon_sequence_job(species_id: int) -> Any:
    user = g.current_user
    assert user is not None
    species = load_species(species_id)
    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("taxon_sequences", species_id=species_id, **request.form))
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Combined metadata is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_sequences", species_id=species_id))

    sequence_dashboard = build_taxon_sequence_dashboard(species, request.form)
    filtered_frame = sequence_dashboard["filtered_frame"]
    if filtered_frame.empty:
        flash("The current filters do not match any genomes.", "error")
        return redirect(url_for("taxon_sequences", species_id=species_id, **request.form))

    grouping_mode = normalize_metadata_value(request.form.get("grouping_mode")) or "single"
    if grouping_mode not in {"single", "field"}:
        grouping_mode = "single"
    grouping_field = normalize_sequence_group_field(request.form.get("group_field"))

    job_id = uuid.uuid4().hex[:12]
    root = job_dir(job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    input_name = f"{species.species_name} filtered metadata.csv"
    input_path = uploads_dir / f"{species.slug}_sequence_subset.csv"
    filtered_frame.to_csv(input_path, index=False)

    class SequenceFormAdapter:
        def __init__(self, source: Any):
            self.source = source

        def get(self, key: str, default: Any = None) -> Any:
            return self.source.get(key, default)

        def getlist(self, key: str) -> list[Any]:
            if hasattr(self.source, "getlist"):
                return self.source.getlist(key)
            value = self.source.get(key)
            return [] if value is None else [value]

    command, command_filters = build_command("seq", input_path, outputs_dir, SequenceFormAdapter(request.form))
    filters = {
        "input_source": "taxon_sequences",
        "taxon_id": species.id,
        "taxon_name": species.species_name,
        "taxon_rank": species.taxon_rank,
        "matched_row_total": sequence_dashboard["matched_row_total"],
        "match_percent": sequence_dashboard["match_percent"],
        "sequence_filter_sentence": sequence_dashboard["filter_sentence"],
        "selected_filters": sequence_dashboard["filters"],
        "grouping_mode": grouping_mode,
        "group_field": grouping_field,
    }
    filters.update(command_filters)
    record = JobRecord(
        id=job_id,
        mode="seq",
        status="queued",
        created_at=utc_now(),
        updated_at=utc_now(),
        input_name=input_name,
        input_path=str(input_path),
        output_dir=str(outputs_dir),
        log_path=str(root / LOG_FILE_NAME),
        command=command,
        owner_user_id=int(user["id"]),
        owner_username=str(user["username"]),
        filters=filters,
    )
    save_job(record)
    notify_job_event(record, "submitted")
    flash(f"Sequence job {job_id} submitted for {sequence_dashboard['matched_row_total']:,} genomes.", "success")
    return redirect(url_for("job_detail", job_id=job_id))


@app.route("/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id: str) -> Any:
    job = require_job_owner(job_id)
    if job.status not in {"queued", "running"}:
        flash("Only queued or running jobs can be cancelled.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    request_job_cancellation(job)
    flash(f"Job {job_id} cancellation requested.", "success")
    return redirect(url_for("job_detail", job_id=job_id))


@app.route("/admin/jobs/<job_id>/cancel", methods=["POST"])
def admin_cancel_job(job_id: str) -> Any:
    require_admin()
    job = load_job(job_id)
    if job.status not in {"queued", "running"}:
        flash("Only queued or running jobs can be cancelled.", "error")
        return redirect(url_for("admin_jobs"))
    request_job_cancellation(job)
    flash(f"Admin cancellation requested for job {job_id}.", "success")
    return redirect(url_for("admin_jobs"))


@app.route("/admin/jobs/<job_id>/delete", methods=["POST"])
def admin_delete_job(job_id: str) -> Any:
    require_admin()
    job = load_job(job_id)
    if job.status in {"queued", "running"}:
        flash("Running or queued jobs must be stopped before deletion.", "error")
        return redirect(url_for("admin_jobs"))
    get_db().execute("DELETE FROM jobs WHERE id = ?", (job.id,))
    get_db().commit()
    job_root = job_dir(job.id)
    if job_root.exists():
        for path in sorted(job_root.rglob("*"), reverse=True):
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                path.rmdir()
        job_root.rmdir()
    flash(f"Deleted job {job.id} and its stored files.", "success")
    return redirect(url_for("admin_jobs"))


@app.route("/admin/jobs/cleanup", methods=["POST"])
def admin_cleanup_jobs() -> Any:
    require_admin()
    raw_days = (request.form.get("older_than_days") or "30").strip()
    try:
        older_than_days = max(1, int(raw_days))
    except ValueError:
        flash("Cleanup age must be a whole number of days.", "error")
        return redirect(url_for("admin_jobs"))
    removed = cleanup_old_jobs(older_than_days=older_than_days)
    flash(f"Removed {removed} old job records/directories older than {older_than_days} days.", "success")
    return redirect(url_for("admin_jobs"))


@app.route("/jobs/<job_id>/files/<path:relative_path>")
def download_output(job_id: str, relative_path: str) -> Any:
    job = require_job_owner(job_id)
    output_root = Path(job.output_dir).resolve()
    target = (output_root / relative_path).resolve()
    if output_root not in target.parents and target != output_root:
        abort(404)
    return send_from_directory(output_root, relative_path, as_attachment=True)


@app.route("/jobs/<job_id>/grouped-output.zip")
def download_grouped_output_bundle(job_id: str) -> Any:
    job = require_job_owner(job_id)
    output_root = Path(job.output_dir).resolve()
    grouped_output_summary = summarize_grouped_sequence_outputs(job, collect_output_files(output_root))
    if not grouped_output_summary or not grouped_output_summary["group_count"]:
        flash("Grouped sequence output is not available for that job.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    grouped_root = output_root / f"grouped_by_{grouped_output_summary['group_field']}"
    if not grouped_root.exists():
        flash("Grouped sequence output is not available for that job.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    bundle = BytesIO()
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(grouped_root.rglob("*")):
            if path.is_file():
                archive.write(path, arcname=str(path.relative_to(output_root)))
    bundle.seek(0)
    return app.response_class(
        bundle.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": f"attachment; filename={job.id}_grouped_output.zip"},
    )


@app.route("/jobs/<job_id>/grouped-output/<group_slug>.zip")
def download_grouped_output_group_bundle(job_id: str, group_slug: str) -> Any:
    job = require_job_owner(job_id)
    output_root = Path(job.output_dir).resolve()
    grouped_output_summary = summarize_grouped_sequence_outputs(job, collect_output_files(output_root))
    if not grouped_output_summary or not grouped_output_summary["group_count"]:
        flash("Grouped sequence output is not available for that job.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    group_item = next((item for item in grouped_output_summary["group_items"] if item["slug"] == group_slug), None)
    if group_item is None:
        abort(404)

    grouped_root = output_root / f"grouped_by_{grouped_output_summary['group_field']}" / group_item["name"]
    if not grouped_root.exists():
        abort(404)

    bundle = BytesIO()
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(grouped_root.rglob("*")):
            if path.is_file():
                archive.write(path, arcname=str(path.relative_to(output_root)))
    bundle.seek(0)
    return app.response_class(
        bundle.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": f"attachment; filename={job.id}_{group_slug}.zip"},
    )


def create_app() -> Flask:
    ensure_directories()
    with app.app_context():
        init_db()
    return app


if __name__ == "__main__":
    ensure_directories()
    with app.app_context():
        init_db()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        debug=os.environ.get("FETCHM_WEBAPP_DEBUG") == "1",
    )
