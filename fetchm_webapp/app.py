from __future__ import annotations

import argparse
import contextlib
import csv
import difflib
import fcntl
import html
import json
import logging
import os
import re
import secrets
import shlex
import shutil
import signal
import sqlite3
import smtplib
import statistics
import subprocess
import time
import threading
import uuid
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace as dataclass_replace
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from functools import lru_cache
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Callable, Mapping

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

from external_tools.quality_check import (
    DEFAULT_QUALITY_THRESHOLDS,
    QUALITY_PROFILES,
    build_quality_config,
    build_quality_display_command,
    build_quality_handoff,
    external_module_keys,
    list_quality_modules,
    quality_tool_status,
    validate_quality_runtime,
)
from global_insights import (
    generate_demo_snapshot,
    generate_global_insights_snapshot,
    run_standardization_simulator,
)

APP_VERSION = "2026.05-genus-v1.1"
APP_COMMIT = (os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "unknown").strip() or "unknown"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
JOBS_DIR = DATA_DIR / "jobs"
GLOBAL_INSIGHTS_DIR_NAME = "global_insights"
DATASET_VERSIONS_DIR_NAME = "dataset_versions"
SPECIES_DIR = DATA_DIR / "species"
METADATA_DIR = DATA_DIR / "metadata"
LOCKS_DIR = DATA_DIR / "locks"
DB_PATH = DATA_DIR / "fetchm_webapp.db"
STANDARDIZATION_DIR = BASE_DIR / "standardization"
ENV_FILE = BASE_DIR / ".env"
LOG_FILE_NAME = "job.log"
UPLOADS_DIR_NAME = "uploads"
OUTPUTS_DIR_NAME = "outputs"
from lib.fetchm_runtime.metadata import (
    COUNTRY_MAPPING,
    MISSING_VALUE_TOKENS,
    RequestRateLimiter,
    add_geo_columns,
    extract_country,
    fetch_metadata,
    filter_data,
    get_effective_sleep,
    load_data,
    normalize_country_name,
    save_clean_data,
    save_summary,
    standardize_date,
    standardize_host,
    standardize_isolation_source,
    standardize_location,
)
from lib.fetchm_runtime.sequence import DEFAULT_DOWNLOAD_WORKERS, SequenceDownloadCancelled, run_sequence_downloads
RESET_TOKEN_TTL_MINUTES = 60
PASSWORD_MIN_LENGTH = 10
DEFAULT_SECRET_KEY = "fetchm-dev-secret"
AUTH_RATE_LIMIT_WINDOW_SECONDS = 15 * 60
AUTH_RATE_LIMIT_MAX_ATTEMPTS = 10
MAX_UPLOAD_BYTES = int(os.environ.get("FETCHM_WEBAPP_MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))
ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".tsv"}
PUBLIC_ENDPOINTS = {
    "login",
    "register",
    "forgot_password",
    "reset_password",
    "healthz",
    "static",
    "index",
    "api_taxa_search",
    "taxon_metadata",
    "taxon_metadata_section",
    "download_taxon_metadata",
    "download_taxon_metadata_bundle",
    "global_insights",
    "download_global_insights_file",
}
_auth_rate_limit_lock = threading.Lock()
_auth_rate_limit_events: dict[tuple[str, str], list[float]] = {}

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
METADATA_FETCH_WORKERS = max(1, int(os.environ.get("FETCHM_WEBAPP_METADATA_FETCH_WORKERS", "1")))
METADATA_FETCH_BATCH_SIZE = max(
    1,
    int(
        os.environ.get(
            "FETCHM_WEBAPP_METADATA_FETCH_BATCH_SIZE",
            str(max(1, METADATA_FETCH_WORKERS * 4)),
        )
    ),
)
METADATA_CHUNK_MIN_ROWS = max(1, int(os.environ.get("FETCHM_WEBAPP_METADATA_CHUNK_MIN_ROWS", "50000")))
METADATA_CHUNK_SIZE = max(1, int(os.environ.get("FETCHM_WEBAPP_METADATA_CHUNK_SIZE", "5000")))
METADATA_CHUNK_PROGRESS_BUFFER_ROWS = max(
    0,
    int(os.environ.get("FETCHM_WEBAPP_METADATA_CHUNK_PROGRESS_BUFFER_ROWS", "20000")),
)
STANDARDIZATION_CHUNK_MIN_ROWS = max(
    1,
    int(os.environ.get("FETCHM_WEBAPP_STANDARDIZATION_CHUNK_MIN_ROWS", "50000")),
)
STANDARDIZATION_CHUNK_SIZE = max(
    1,
    int(os.environ.get("FETCHM_WEBAPP_STANDARDIZATION_CHUNK_SIZE", "10000")),
)
STANDARDIZATION_PARALLEL_CHUNK_MIN_ROWS = max(
    1,
    int(os.environ.get("FETCHM_WEBAPP_STANDARDIZATION_PARALLEL_CHUNK_MIN_ROWS", "200000")),
)
STANDARDIZATION_PARALLEL_CHUNK_SIZE = max(
    1,
    int(os.environ.get("FETCHM_WEBAPP_STANDARDIZATION_PARALLEL_CHUNK_SIZE", "200000")),
)
BIOSAMPLE_CACHE_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_BIOSAMPLE_CACHE_HOURS", "720")))
BIOSAMPLE_NEGATIVE_CACHE_HOURS = max(
    1, int(os.environ.get("FETCHM_WEBAPP_BIOSAMPLE_NEGATIVE_CACHE_HOURS", "168"))
)
SQLITE_VARIABLE_CHUNK_SIZE = 900
DISCOVERY_REFRESH_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_DISCOVERY_REFRESH_HOURS", "24")))
DISCOVERY_LIMIT_PER_SCOPE = os.environ.get("FETCHM_WEBAPP_DISCOVERY_LIMIT_PER_SCOPE", "100").strip() or "100"
DATASETS_BINARY = os.environ.get("FETCHM_WEBAPP_DATASETS_BIN", "datasets")
TAXONKIT_BINARY = os.environ.get("FETCHM_WEBAPP_TAXONKIT_BIN", "taxonkit")
TAXON_RECENT_HOURS = max(1, int(os.environ.get("FETCHM_WEBAPP_TAXON_RECENT_HOURS", "168")))
TAXON_VERY_OLD_HOURS = max(TAXON_RECENT_HOURS + 1, int(os.environ.get("FETCHM_WEBAPP_TAXON_VERY_OLD_HOURS", "720")))
SEQUENCE_DOWNLOAD_WORKERS = max(
    1, int(os.environ.get("FETCHM_WEBAPP_SEQUENCE_DOWNLOAD_WORKERS", "8"))
)
MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES = max(
    0,
    int(float(os.environ.get("FETCHM_WEBAPP_MIN_AVAILABLE_MEMORY_FOR_QUALITY_GIB", "12")) * 1024**3),
)
MAX_MEMORY_PERCENT_FOR_QUALITY = max(
    0.0,
    min(100.0, float(os.environ.get("FETCHM_WEBAPP_MAX_MEMORY_PERCENT_FOR_QUALITY", "88"))),
)
MAX_ACTIVE_QUALITY_JOBS = max(1, int(os.environ.get("FETCHM_WEBAPP_MAX_ACTIVE_QUALITY_JOBS", "1")))
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
    "host_sd": {"label": "Host (standardized)", "column": "Host_SD", "group": "Host"},
    "host": {"label": "Host (raw)", "column": "Host", "group": "Host"},
    "host_rank": {"label": "Host rank", "column": "Host_Rank", "group": "Host"},
    "host_class": {"label": "Host class", "column": "Host_Class", "group": "Host"},
    "host_order": {"label": "Host order", "column": "Host_Order", "group": "Host"},
    "host_family": {"label": "Host family", "column": "Host_Family", "group": "Host"},
    "host_genus": {"label": "Host genus", "column": "Host_Genus", "group": "Host"},
    "host_species": {"label": "Host species", "column": "Host_Species", "group": "Host"},
    "host_confidence": {"label": "Host confidence", "column": "Host_SD_Confidence", "group": "Host"},
    "host_method": {"label": "Host method", "column": "Host_SD_Method", "group": "Host"},
    "host_disease_sd": {"label": "Host disease (standardized)", "column": "Host_Disease_SD", "group": "Host"},
    "host_disease": {"label": "Host disease (raw)", "column": "Host Disease", "group": "Host"},
    "host_health_state_sd": {"label": "Host health state", "column": "Host_Health_State_SD", "group": "Host"},
    "isolation_source_sd": {
        "label": "Isolation source (standardized)",
        "column": "Isolation_Source_SD",
        "group": "Isolation and Environment",
    },
    "isolation_source": {"label": "Isolation source (raw)", "column": "Isolation Source", "group": "Isolation and Environment"},
    "sample_type_sd": {"label": "Sample type (standardized)", "column": "Sample_Type_SD", "group": "Isolation and Environment"},
    "sample_type": {"label": "Sample type (raw)", "column": "Sample Type", "group": "Isolation and Environment"},
    "isolation_site_sd": {"label": "Isolation site", "column": "Isolation_Site_SD", "group": "Isolation and Environment"},
    "environment_broad_sd": {
        "label": "Environment broad (standardized)",
        "column": "Environment_Broad_Scale_SD",
        "group": "Isolation and Environment",
    },
    "environment_broad": {
        "label": "Environment broad (raw)",
        "column": "Environment (Broad Scale)",
        "group": "Isolation and Environment",
    },
    "environment_local_sd": {
        "label": "Environment local (standardized)",
        "column": "Environment_Local_Scale_SD",
        "group": "Isolation and Environment",
    },
    "environment_local": {
        "label": "Environment local (raw)",
        "column": "Environment (Local Scale)",
        "group": "Isolation and Environment",
    },
    "environment_medium_sd": {
        "label": "Environment medium (standardized)",
        "column": "Environment_Medium_SD",
        "group": "Isolation and Environment",
    },
    "environment_medium": {
        "label": "Environment medium (raw)",
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
        "fields": [
            "host_sd",
            "host",
            "host_rank",
            "host_class",
            "host_order",
            "host_family",
            "host_genus",
            "host_species",
            "host_confidence",
            "host_method",
            "host_disease_sd",
            "host_disease",
            "host_health_state_sd",
        ],
    },
    {
        "key": "isolation_environment",
        "label": "Isolation and Environment",
        "fields": [
            "isolation_source_sd",
            "isolation_source",
            "sample_type_sd",
            "sample_type",
            "isolation_site_sd",
            "environment_broad_sd",
            "environment_broad",
            "environment_local_sd",
            "environment_local",
            "environment_medium_sd",
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
    "Host_SD",
    "Host",
    "Isolation_Source_SD",
    "Isolation Source",
    "Sample_Type_SD",
    "Environment_Medium_SD",
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


def parse_ncbi_api_keys() -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    raw_candidates = [
        os.environ.get("NCBI_API_KEY", ""),
        os.environ.get("NCBI_API_KEY_SECONDARY", ""),
        os.environ.get("FETCHM_WEBAPP_NCBI_API_KEYS", ""),
        os.environ.get("NCBI_API_KEYS", ""),
    ]
    for raw in raw_candidates:
        for part in str(raw or "").replace("\n", ",").split(","):
            key = part.strip()
            if not key or key in seen:
                continue
            seen.add(key)
            keys.append(key)
    return keys


NCBI_API_KEYS = parse_ncbi_api_keys()
_ncbi_api_key_lock = threading.Lock()
_ncbi_api_key_index = 0
_ncbi_rate_limiter_lock = threading.Lock()
_ncbi_rate_limiters: dict[str | None, RequestRateLimiter] = {}


def next_ncbi_api_key() -> str | None:
    if not NCBI_API_KEYS:
        return None
    candidates: list[tuple[float, float, int, str]] = []
    now = time.monotonic()
    with _ncbi_api_key_lock:
        global _ncbi_api_key_index
        start_index = _ncbi_api_key_index
        for offset, key in enumerate(NCBI_API_KEYS):
            rotated_index = (start_index + offset) % len(NCBI_API_KEYS)
            limiter = get_ncbi_rate_limiter(key)
            with limiter.lock:
                next_allowed_time = limiter.next_allowed_time
                interval_seconds = limiter.interval_seconds
            cooling_delay = max(0.0, next_allowed_time - now)
            candidates.append((cooling_delay, interval_seconds, rotated_index, key))
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        selected_key = candidates[0][3]
        selected_index = NCBI_API_KEYS.index(selected_key)
        _ncbi_api_key_index = selected_index + 1
        return selected_key


def get_ncbi_rate_limiter(api_key: str | None) -> RequestRateLimiter:
    limiter_key = api_key or "__no_key__"
    with _ncbi_rate_limiter_lock:
        limiter = _ncbi_rate_limiters.get(limiter_key)
        if limiter is not None:
            return limiter
        interval_seconds = get_effective_sleep(None, api_key)
        if METADATA_SLEEP_SECONDS > 0:
            interval_seconds = min(interval_seconds, METADATA_SLEEP_SECONDS) if api_key else METADATA_SLEEP_SECONDS
        limiter = RequestRateLimiter(interval_seconds)
        _ncbi_rate_limiters[limiter_key] = limiter
        return limiter

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

ASSEMBLY_FEATURE_COLUMNS = [
    "Assembly Level",
    "Assembly Status",
    "Assembly Stats Number of Contigs",
    "Assembly Stats Number of Scaffolds",
    "Assembly Stats Contig N50",
    "Assembly Stats Scaffold N50",
]

SEQUENCE_QC_DEFAULTS = DEFAULT_QUALITY_THRESHOLDS


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
    claimed_by: str | None = None
    claimed_at: str | None = None


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
    metadata_progress_total: int = 0
    metadata_progress_completed: int = 0
    metadata_progress_current_accession: str | None = None
    metadata_progress_updated_at: str | None = None
    assembly_backfill_status: str = "idle"
    assembly_backfill_requested_at: str | None = None
    assembly_backfill_claimed_by: str | None = None
    assembly_backfill_claimed_at: str | None = None
    assembly_backfill_last_built_at: str | None = None
    assembly_backfill_error: str | None = None
    is_live: bool = True
    staging_dataset_version_id: str | None = None
    live_status: str | None = None
    live_tsv_path: str | None = None
    live_metadata_status: str | None = None
    live_metadata_path: str | None = None
    live_metadata_clean_path: str | None = None
    live_genome_count: int | None = None
    live_taxon_id: int | None = None
    live_last_synced_at: str | None = None
    live_metadata_last_built_at: str | None = None
    live_updated_at: str | None = None


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
configured_secret = os.environ.get("FETCHM_WEBAPP_SECRET", DEFAULT_SECRET_KEY)
runtime_env = (os.environ.get("FETCHM_WEBAPP_ENV") or os.environ.get("FLASK_ENV") or "").strip().lower()
if configured_secret == DEFAULT_SECRET_KEY and runtime_env in {"prod", "production"}:
    raise RuntimeError("FETCHM_WEBAPP_SECRET must be set in production.")
app.config["SECRET_KEY"] = configured_secret
app.config["APP_VERSION"] = APP_VERSION
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("FETCHM_WEBAPP_SECURE_COOKIE") == "1"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
    hours=max(1, int(os.environ.get("FETCHM_WEBAPP_SESSION_HOURS", "12")))
)
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
    (DATA_DIR / DATASET_VERSIONS_DIR_NAME).mkdir(parents=True, exist_ok=True)
    SPECIES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)


def get_sqlite_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 120000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
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
            metadata_source_taxon_id INTEGER,
            assembly_backfill_status TEXT NOT NULL DEFAULT 'idle',
            assembly_backfill_requested_at TEXT,
            assembly_backfill_claimed_by TEXT,
            assembly_backfill_claimed_at TEXT,
            assembly_backfill_last_built_at TEXT,
            assembly_backfill_error TEXT,
            is_live INTEGER NOT NULL DEFAULT 1,
            staging_dataset_version_id TEXT,
            live_status TEXT,
            live_tsv_path TEXT,
            live_metadata_status TEXT,
            live_metadata_path TEXT,
            live_metadata_clean_path TEXT,
            live_genome_count INTEGER,
            live_taxon_id INTEGER,
            live_last_synced_at TEXT,
            live_metadata_last_built_at TEXT,
            live_updated_at TEXT
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

        CREATE TABLE IF NOT EXISTS dataset_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            verified_at TEXT,
            promoted_at TEXT,
            archived_at TEXT,
            root_path TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}',
            promoted_by INTEGER,
            error TEXT,
            FOREIGN KEY (promoted_by) REFERENCES users (id)
        );

        CREATE INDEX IF NOT EXISTS idx_dataset_versions_status_created
        ON dataset_versions (status, created_at DESC);

        CREATE TABLE IF NOT EXISTS dataset_update_pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            dataset_version_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_by INTEGER,
            requested_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            error TEXT,
            summary_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (requested_by) REFERENCES users (id),
            FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions (version_id)
        );

        CREATE INDEX IF NOT EXISTS idx_dataset_update_runs_status_requested
        ON dataset_update_pipeline_runs (status, requested_at DESC);

        CREATE TABLE IF NOT EXISTS dataset_update_pipeline_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            step_order INTEGER NOT NULL,
            status TEXT NOT NULL,
            queued_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            progress_json TEXT NOT NULL DEFAULT '{}',
            blockers_json TEXT NOT NULL DEFAULT '[]',
            error TEXT,
            FOREIGN KEY (run_id) REFERENCES dataset_update_pipeline_runs (run_id),
            UNIQUE(run_id, step_key)
        );

        CREATE INDEX IF NOT EXISTS idx_dataset_update_steps_status_order
        ON dataset_update_pipeline_steps (status, step_order);

        CREATE TABLE IF NOT EXISTS dataset_species_live_snapshots (
            dataset_version_id TEXT NOT NULL,
            species_id INTEGER NOT NULL,
            species_name TEXT NOT NULL,
            taxon_rank TEXT NOT NULL,
            is_live INTEGER NOT NULL,
            live_status TEXT,
            live_tsv_path TEXT,
            live_metadata_status TEXT,
            live_metadata_path TEXT,
            live_metadata_clean_path TEXT,
            live_genome_count INTEGER,
            live_taxon_id INTEGER,
            live_last_synced_at TEXT,
            live_metadata_last_built_at TEXT,
            live_updated_at TEXT,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (dataset_version_id, species_id),
            FOREIGN KEY (species_id) REFERENCES species (id)
        );

        CREATE TABLE IF NOT EXISTS dataset_metadata_species_search_snapshots (
            dataset_version_id TEXT NOT NULL,
            source_taxon_id INTEGER NOT NULL,
            source_taxon_name TEXT NOT NULL,
            species_name TEXT NOT NULL,
            search_name TEXT NOT NULL,
            genome_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (dataset_version_id, source_taxon_id, species_name),
            FOREIGN KEY (source_taxon_id) REFERENCES species (id)
        );

        CREATE TABLE IF NOT EXISTS global_insight_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            requested_by INTEGER,
            requested_at TEXT NOT NULL,
            claimed_by TEXT,
            claimed_at TEXT,
            completed_at TEXT,
            output_dir TEXT,
            is_demo INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            FOREIGN KEY (requested_by) REFERENCES users (id)
        );

        CREATE INDEX IF NOT EXISTS idx_global_insight_tasks_status
        ON global_insight_tasks (status, requested_at);

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

        CREATE TABLE IF NOT EXISTS metadata_species_search (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_taxon_id INTEGER NOT NULL,
            source_taxon_name TEXT NOT NULL,
            species_name TEXT NOT NULL,
            search_name TEXT NOT NULL,
            genome_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_taxon_id) REFERENCES species (id),
            UNIQUE(source_taxon_id, species_name)
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_species_search_name
        ON metadata_species_search (search_name);

        CREATE INDEX IF NOT EXISTS idx_metadata_species_search_source
        ON metadata_species_search (source_taxon_id, genome_count DESC);

        CREATE TABLE IF NOT EXISTS metadata_species_search_staging (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_version_id TEXT NOT NULL,
            source_taxon_id INTEGER NOT NULL,
            source_taxon_name TEXT NOT NULL,
            species_name TEXT NOT NULL,
            search_name TEXT NOT NULL,
            genome_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_taxon_id) REFERENCES species (id),
            UNIQUE(dataset_version_id, source_taxon_id, species_name)
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_species_search_staging_version
        ON metadata_species_search_staging (dataset_version_id, source_taxon_id, genome_count DESC);

        CREATE INDEX IF NOT EXISTS idx_metadata_species_search_staging_name
        ON metadata_species_search_staging (dataset_version_id, search_name);

        CREATE TABLE IF NOT EXISTS metadata_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            claimed_by TEXT,
            claimed_at TEXT,
            total_rows INTEGER NOT NULL DEFAULT 0,
            completed_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (species_id) REFERENCES species (id),
            UNIQUE(species_id, chunk_index)
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_chunks_status
        ON metadata_chunks (status, species_id, chunk_index);

        CREATE INDEX IF NOT EXISTS idx_metadata_chunks_species
        ON metadata_chunks (species_id, status);

        CREATE TABLE IF NOT EXISTS standardization_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_column TEXT NOT NULL,
            original_value TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            category TEXT NOT NULL,
            destination TEXT NOT NULL,
            proposed_value TEXT NOT NULL,
            ontology_id TEXT,
            method TEXT NOT NULL,
            confidence TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'approved',
            approved_by TEXT,
            approved_at TEXT NOT NULL,
            note TEXT,
            UNIQUE(source_column, normalized_value, destination)
        );

        CREATE INDEX IF NOT EXISTS idx_standardization_rules_status
        ON standardization_rules (status, destination);

        CREATE TABLE IF NOT EXISTS standardization_refresh_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            requested_at TEXT NOT NULL,
            claimed_by TEXT,
            claimed_at TEXT,
            completed_at TEXT,
            total_rows INTEGER NOT NULL DEFAULT 0,
            updated_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            FOREIGN KEY (species_id) REFERENCES species (id)
        );

        CREATE INDEX IF NOT EXISTS idx_standardization_refresh_tasks_status
        ON standardization_refresh_tasks (status, requested_at);

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

        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER,
            actor_username TEXT,
            action TEXT NOT NULL,
            target_type TEXT,
            target_id TEXT,
            request_path TEXT,
            request_method TEXT,
            ip_address TEXT,
            user_agent TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (actor_user_id) REFERENCES users (id)
        );

        CREATE INDEX IF NOT EXISTS idx_audit_log_created
        ON audit_log (created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_audit_log_actor_created
        ON audit_log (actor_user_id, created_at DESC);
        """
    )
    db.commit()
    ensure_job_columns(db)
    ensure_species_columns(db)
    ensure_metadata_chunk_table(db)
    ensure_standardization_refresh_table(db)
    ensure_discovery_scope_columns(db)
    load_approved_standardization_rules_into_memory(db)
    migrate_legacy_jobs(db)
    migrate_legacy_species(db)
    sync_discovery_scopes_from_env(db)
    ensure_default_settings(db)
    ensure_legacy_live_dataset_version(db)


def get_csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return str(token)


def validate_csrf_token() -> None:
    expected = session.get("_csrf_token")
    submitted = request.form.get("_csrf_token") or request.headers.get("X-CSRF-Token")
    if not expected or not submitted or not secrets.compare_digest(str(expected), str(submitted)):
        abort(400, "Invalid or missing CSRF token.")


@app.context_processor
def inject_security_context() -> dict[str, Any]:
    return {
        "app_version": APP_VERSION,
        "csrf_token": get_csrf_token,
    }


def client_ip_address() -> str:
    return (request.headers.get("X-Forwarded-For") or request.remote_addr or "unknown").split(",", 1)[0].strip()


def rate_limit_key(action: str) -> tuple[str, str]:
    return action, client_ip_address()


def auth_rate_limited(action: str) -> bool:
    now = time.time()
    cutoff = now - AUTH_RATE_LIMIT_WINDOW_SECONDS
    key = rate_limit_key(action)
    with _auth_rate_limit_lock:
        events = [timestamp for timestamp in _auth_rate_limit_events.get(key, []) if timestamp >= cutoff]
        limited = len(events) >= AUTH_RATE_LIMIT_MAX_ATTEMPTS
        events.append(now)
        _auth_rate_limit_events[key] = events
        return limited


def record_audit_event(
    action: str,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    try:
        user = getattr(g, "current_user", None)
        get_db().execute(
            """
            INSERT INTO audit_log (
                actor_user_id, actor_username, action, target_type, target_id,
                request_path, request_method, ip_address, user_agent, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user["id"]) if user is not None else None,
                str(user["username"]) if user is not None else None,
                action,
                target_type,
                target_id,
                request.path if request else None,
                request.method if request else None,
                client_ip_address() if request else None,
                request.headers.get("User-Agent", "") if request else None,
                json.dumps(dict(metadata or {}), sort_keys=True),
                utc_now(),
            ),
        )
        get_db().commit()
    except Exception:
        logging.exception("Could not write audit log event for %s", action)


def list_audit_log(limit: int = 200) -> list[sqlite3.Row]:
    return get_db().execute(
        """
        SELECT *
        FROM audit_log
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 1000)),),
    ).fetchall()


BIOSAMPLE_DATA_FIELDS = ("Isolation Source", "Collection Date", "Geographic Location", "Host")


def biosample_record_has_data(record: dict[str, Any]) -> bool:
    for key in BIOSAMPLE_DATA_FIELDS:
        value = record.get(key)
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
    for biosample_id, record in fetch_uncached_biosample_records(to_fetch).items():
        fetched[biosample_id] = (record, biosample_record_has_data(record))
    save_biosample_cache_records(fetched)
    records = dict(cached)
    for biosample_id, (record, _) in fetched.items():
        records[biosample_id] = record
    return records


def fetch_uncached_biosample_records(biosample_ids: list[str]) -> dict[str, dict[str, Any]]:
    unique_ids: list[str] = []
    seen_ids: set[str] = set()
    for biosample_id in biosample_ids:
        if not biosample_id or biosample_id in seen_ids:
            continue
        unique_ids.append(biosample_id)
        seen_ids.add(biosample_id)

    if not unique_ids:
        return {}
    if METADATA_FETCH_WORKERS <= 1 or len(unique_ids) == 1:
        return {
            biosample_id: fetch_metadata_record(biosample_id, METADATA_SLEEP_SECONDS)
            for biosample_id in unique_ids
        }

    records: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=METADATA_FETCH_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_metadata_record, biosample_id, METADATA_SLEEP_SECONDS): biosample_id
            for biosample_id in unique_ids
        }
        for future in as_completed(future_map):
            biosample_id = future_map[future]
            records[biosample_id] = future.result()
    return records


def fetch_metadata_record(biosample_id: str, sleep_time: float) -> dict[str, Any]:
    del sleep_time
    api_key = next_ncbi_api_key()
    metadata_tuple, status_info = fetch_metadata(
        biosample_id,
        api_key=api_key,
        email=None,
        persistent_cache=None,
        rate_limiter=get_ncbi_rate_limiter(api_key),
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


PRIMARY_METADATA_ALIASES = {
    "Geographic Location": [
        "BioSample GEO LOC Name",
        "BioSample Geographic Location Country AND OR SEA",
        "BioSample Geographic Location Country AND OR SEA Region",
        "BioSample Country",
    ],
    "Host": [
        "BioSample Host",
        "BioSample Specific Host",
        "BioSample NAT Host",
        "BioSample LAB Host",
        "BioSample Host Common Name",
        "BioSample Common Name",
    ],
    "Isolation Source": [
        "BioSample Isolation Source",
        "BioSample Isolation Site",
        "BioSample Source Name",
        "BioSample Source Type",
        "BioSample Source Material ID",
        "BioSample Source MAT ID",
        "BioSample ENV Material",
        "BioSample Environment Material",
    ],
    "Collection Date": [
        "BioSample Collection Date",
        "BioSample Collection Timestamp",
        "BioSample Colection Date",
        "BioSample Collection Date Remark",
        "BioSample Sampling Event Date Time Start",
        "BioSample Date Host Collection",
        "BioSample Isolation Date",
        "BioSample Harvest Date",
        "BioSample Specimen Collection Date",
        "BioSample DNA Isolation Date",
    ],
    "Sample Type": [
        "BioSample Sample Type",
        "BioSample Source Type",
        "BioSample Package",
        "BioSample ENV Package",
    ],
    "Host Disease": [
        "BioSample Host Disease",
        "BioSample Disease",
        "BioSample Diseases",
        "BioSample Study Disease",
        "BioSample Ifsac Category",
    ],
    "Host Health State": [
        "BioSample Host Health State",
        "BioSample Health State",
    ],
    "Environment (Broad Scale)": [
        "BioSample ENV Broad Scale",
        "BioSample Environment Biome",
        "BioSample ENV Biome",
        "BioSample Biome",
        "BioSample Metagenome Source",
        "BioSample Environment",
    ],
    "Environment (Local Scale)": [
        "BioSample ENV Local Scale",
        "BioSample Environment Feature",
        "BioSample ENV Feature",
        "BioSample Feature",
        "BioSample Coll Site GEO Feat",
    ],
    "Environment Medium": [
        "BioSample ENV Medium",
        "BioSample Environment Material",
        "BioSample ENV Material",
        "BioSample Material",
    ],
    "Lab Host": ["BioSample LAB Host"],
    "Isolation Site": [
        "BioSample Isolation Site",
        "BioSample Body Site",
        "BioSample Organism Part",
        "BioSample Tissue",
        "BioSample Host Tissue Sampled",
    ],
    "Latitude/Longitude": ["BioSample LAT LON"],
}

HOST_STANDARDIZATION_COLUMNS = [
    "Host_Original",
    "Host_Cleaned",
    "Host_SD",
    "Host_TaxID",
    "Host_Rank",
    "Host_Superkingdom",
    "Host_Phylum",
    "Host_Class",
    "Host_Order",
    "Host_Family",
    "Host_Genus",
    "Host_Species",
    "Host_Common_Name",
    "Host_Context_SD",
    "Host_Age_Group_SD",
    "Host_Production_Context_SD",
    "Host_Anatomical_Site_SD",
    "Host_Match_Method",
    "Host_Confidence",
    "Host_Review_Status",
    "Host_SD_Method",
    "Host_SD_Confidence",
]

SECONDARY_STANDARDIZATION_COLUMNS = [
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Isolation_Source_SD_Detail",
    "Isolation_Source_SD_Method",
    "Isolation_Source_Ontology_ID",
    "Isolation_Site_SD",
    "Isolation_Site_SD_Broad",
    "Isolation_Site_SD_Detail",
    "Isolation_Site_SD_Method",
    "Isolation_Site_Ontology_ID",
    "Environment_Broad_Scale_SD",
    "Environment_Broad_Scale_SD_Broad",
    "Environment_Broad_Scale_SD_Detail",
    "Environment_Broad_Scale_SD_Method",
    "Environment_Broad_Scale_Ontology_ID",
    "Environment_Local_Scale_SD",
    "Environment_Local_Scale_SD_Broad",
    "Environment_Local_Scale_SD_Detail",
    "Environment_Local_Scale_SD_Method",
    "Environment_Local_Scale_Ontology_ID",
    "Environment_Medium_SD",
    "Environment_Medium_SD_Broad",
    "Environment_Medium_SD_Detail",
    "Environment_Medium_SD_Method",
    "Environment_Medium_Ontology_ID",
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Sample_Type_SD_Detail",
    "Sample_Type_SD_Method",
    "Sample_Type_Ontology_ID",
    "Host_Disease_SD",
    "Host_Disease_SD_Broad",
    "Host_Disease_SD_Detail",
    "Host_Disease_SD_Method",
    "Host_Disease_Ontology_ID",
    "Host_Health_State_SD",
    "Host_Health_State_SD_Broad",
    "Host_Health_State_SD_Detail",
    "Host_Health_State_SD_Method",
    "Host_Health_State_Ontology_ID",
]

HOST_SYNONYMS = {
    "human": ("Homo sapiens", "9606"),
    "homosapiens": ("Homo sapiens", "9606"),
    "homo sapiens": ("Homo sapiens", "9606"),
    "h sapiens": ("Homo sapiens", "9606"),
    "h. sapiens": ("Homo sapiens", "9606"),
    "man": ("Homo sapiens", "9606"),
    "woman": ("Homo sapiens", "9606"),
    "mouse": ("Mus musculus", "10090"),
    "murine": ("Mus musculus", "10090"),
    "mus musculus": ("Mus musculus", "10090"),
    "cattle": ("Bos taurus", "9913"),
    "cow": ("Bos taurus", "9913"),
    "bovine": ("Bos taurus", "9913"),
    "bos taurus": ("Bos taurus", "9913"),
    "camel": ("Camelus dromedarius", "9838"),
    "dromedary": ("Camelus dromedarius", "9838"),
    "dromedary camel": ("Camelus dromedarius", "9838"),
    "camelus dromedarius": ("Camelus dromedarius", "9838"),
    "pig": ("Sus scrofa", "9823"),
    "pigs": ("Sus scrofa", "9823"),
    "swine": ("Sus scrofa", "9823"),
    "porcine": ("Sus scrofa", "9823"),
    "sus scrofa": ("Sus scrofa", "9823"),
    "wild boar": ("Sus scrofa", "9823"),
    "domestic pig": ("Sus scrofa domesticus", "9825"),
    "sus scrofa domesticus": ("Sus scrofa domesticus", "9825"),
    "sus scrofa domestica": ("Sus scrofa domesticus", "9825"),
    "sus scofa domesticus": ("Sus scrofa domesticus", "9825"),
    "chicken": ("Gallus gallus", "9031"),
    "broiler": ("Gallus gallus", "9031"),
    "gallus gallus": ("Gallus gallus", "9031"),
    "dog": ("Canis lupus familiaris", "9615"),
    "dogs": ("Canis lupus familiaris", "9615"),
    "canine": ("Canis lupus familiaris", "9615"),
    "canis lupus": ("Canis lupus", "9612"),
    "canis familiaris": ("Canis lupus familiaris", "9615"),
    "canis lupus familiaris": ("Canis lupus familiaris", "9615"),
    "cat": ("Felis catus", "9685"),
    "cats": ("Felis catus", "9685"),
    "feline": ("Felis catus", "9685"),
    "felis catus": ("Felis catus", "9685"),
    "turkey": ("Meleagris gallopavo", "9103"),
    "meleagris gallopavo": ("Meleagris gallopavo", "9103"),
    "horse": ("Equus caballus", "9796"),
    "equine": ("Equus caballus", "9796"),
    "equus caballus": ("Equus caballus", "9796"),
    "equus ferus caballus": ("Equus caballus", "9796"),
    "tibetan ass": ("Equus kiang", "94398"),
    "tibetan wild ass": ("Equus kiang", "94398"),
    "equus kiang": ("Equus kiang", "94398"),
    "sheep": ("Ovis aries", "9940"),
    "ovine": ("Ovis aries", "9940"),
    "ovis aries": ("Ovis aries", "9940"),
    "goat": ("Capra hircus", "9925"),
    "caprine": ("Capra hircus", "9925"),
    "capra hircus": ("Capra hircus", "9925"),
    "capra aegagrus hircus": ("Capra hircus", "9925"),
    "yak": ("Bos grunniens", "30521"),
    "tibetan yak": ("Bos grunniens", "30521"),
    "bos grunniens": ("Bos grunniens", "30521"),
    "tibetan antelope": ("Pantholops hodgsonii", "59538"),
    "pantholops hodgsonii": ("Pantholops hodgsonii", "59538"),
    "duck": ("Anas platyrhynchos", "8839"),
    "anas platyrhynchos": ("Anas platyrhynchos", "8839"),
    "goose": ("Anser anser", "8843"),
    "anser anser": ("Anser anser", "8843"),
    "salmo salar": ("Salmo salar", "8030"),
    "atlantic salmon": ("Salmo salar", "8030"),
    "oncorhynchus mykiss": ("Oncorhynchus mykiss", "8022"),
    "rainbow trout": ("Oncorhynchus mykiss", "8022"),
    "rabbit": ("Oryctolagus cuniculus", "9986"),
    "oryctolagus cuniculus": ("Oryctolagus cuniculus", "9986"),
    "rat": ("Rattus norvegicus", "10116"),
    "rattus norvegicus": ("Rattus norvegicus", "10116"),
    "rattus flavipectus": ("Rattus flavipectus", "69074"),
    "sciurus vulgaris": ("Sciurus vulgaris", "55149"),
    "red squirrel": ("Sciurus vulgaris", "55149"),
    "rhesus macaque": ("Macaca mulatta", "9544"),
    "macaca mulatta": ("Macaca mulatta", "9544"),
    "macaca fascicularis": ("Macaca fascicularis", "9541"),
    "honey bee": ("Apis mellifera", "7460"),
    "honeybee": ("Apis mellifera", "7460"),
    "apis mellifera": ("Apis mellifera", "7460"),
    "escherichia coli": ("Escherichia coli", "562"),
    "e coli": ("Escherichia coli", "562"),
    "e. coli": ("Escherichia coli", "562"),
    "salmonella enterica": ("Salmonella enterica", "28901"),
    "campylobacter jejuni": ("Campylobacter jejuni", "197"),
    "streptococcus pneumoniae": ("Streptococcus pneumoniae", "1313"),
    "saccharomyces cerevisiae": ("Saccharomyces cerevisiae", "4932"),
    "bakers yeast": ("Saccharomyces cerevisiae", "4932"),
    "baker s yeast": ("Saccharomyces cerevisiae", "4932"),
    "rice": ("Oryza sativa", "4530"),
    "oryza sativa": ("Oryza sativa", "4530"),
    "tomato": ("Solanum lycopersicum", "4081"),
    "solanum lycopersicum": ("Solanum lycopersicum", "4081"),
    "potato": ("Solanum tuberosum", "4113"),
    "solanum tuberosum": ("Solanum tuberosum", "4113"),
    "arabidopsis": ("Arabidopsis thaliana", "3702"),
    "arabidopsis thaliana": ("Arabidopsis thaliana", "3702"),
    "corn": ("Zea mays", "4577"),
    "maize": ("Zea mays", "4577"),
    "zea mays": ("Zea mays", "4577"),
    "soybean": ("Glycine max", "3847"),
    "glycine max": ("Glycine max", "3847"),
    "trifolium repens": ("Trifolium repens", "3899"),
    "white clover": ("Trifolium repens", "3899"),
    "medicago sativa": ("Medicago sativa", "3879"),
    "alfalfa": ("Medicago sativa", "3879"),
    "vitis vinifera": ("Vitis vinifera", "29760"),
    "grape": ("Vitis vinifera", "29760"),
    "nicotiana tabacum": ("Nicotiana tabacum", "4097"),
    "tobacco": ("Nicotiana tabacum", "4097"),
    "malus domestica": ("Malus domestica", "3750"),
    "apple": ("Malus domestica", "3750"),
    "pistacia vera": ("Pistacia vera", "55513"),
    "pistachio": ("Pistacia vera", "55513"),
    "phaseolus vulgaris": ("Phaseolus vulgaris", "3885"),
    "common bean": ("Phaseolus vulgaris", "3885"),
    "brassica oleracea": ("Brassica oleracea", "3712"),
    "allium cepa": ("Allium cepa", "4679"),
    "onion": ("Allium cepa", "4679"),
    "cicer arietinum": ("Cicer arietinum", "3827"),
    "chickpea": ("Cicer arietinum", "3827"),
    "triticum aestivum": ("Triticum aestivum", "4565"),
    "wheat": ("Triticum aestivum", "4565"),
    "citrus sinensis": ("Citrus sinensis", "2711"),
    "orange": ("Citrus sinensis", "2711"),
    "citrus": ("Citrus", "2706"),
    "macrocystis pyrifera": ("Macrocystis pyrifera", "35122"),
    "giant kelp": ("Macrocystis pyrifera", "35122"),
    "chroicocephalus novaehollandiae": ("Chroicocephalus novaehollandiae", "2547444"),
    "lepus europaeus": ("Lepus europaeus", "9983"),
    "european hare": ("Lepus europaeus", "9983"),
    "capsicum annuum": ("Capsicum annuum", "4072"),
    "sweet pepper": ("Capsicum annuum", "4072"),
    "penaeus vannamei": ("Penaeus vannamei", "6689"),
    "litopenaeus vannamei": ("Penaeus vannamei", "6689"),
    "white shrimp": ("Penaeus vannamei", "6689"),
    "pacific white shrimp": ("Penaeus vannamei", "6689"),
    "columba livia": ("Columba livia", "8932"),
    "pigeon": ("Columba livia", "8932"),
    "rock pigeon": ("Columba livia", "8932"),
    "domestic pigeon": ("Columba livia", "8932"),
    "lens culinaris": ("Lens culinaris", "3864"),
    "lentil": ("Lens culinaris", "3864"),
    "zostera marina": ("Zostera marina", "29655"),
    "prunus avium": ("Prunus avium", "42229"),
    "sweet cherry": ("Prunus avium", "42229"),
    "cervus canadensis": ("Cervus canadensis", "1574408"),
    "wapiti": ("Cervus canadensis", "1574408"),
    "cetonia aurata": ("Cetonia aurata", "290679"),
    "andrias davidianus": ("Andrias davidianus", "141262"),
    "chinese giant salamander": ("Andrias davidianus", "141262"),
    "ciconia ciconia": ("Ciconia ciconia", "8928"),
    "white stork": ("Ciconia ciconia", "8928"),
    "agama agama": ("Agama agama", "103336"),
    "oreochromis niloticus": ("Oreochromis niloticus", "8128"),
    "nile tilapia": ("Oreochromis niloticus", "8128"),
    "platygyra acuta": ("Platygyra acuta", "983579"),
    "oulastrea crispata": ("Oulastrea crispata", "154329"),
    "magallana gigas": ("Magallana gigas", "29159"),
    "crassostrea gigas": ("Magallana gigas", "29159"),
    "pacific oyster": ("Magallana gigas", "29159"),
    "procyon lotor": ("Procyon lotor", "9654"),
    "raccoon": ("Procyon lotor", "9654"),
    "enhydra lutris nereis": ("Enhydra lutris nereis", "1049777"),
    "drosophila melanogaster": ("Drosophila melanogaster", "7227"),
    "fruit fly": ("Drosophila melanogaster", "7227"),
    "hydropotes inermis": ("Hydropotes inermis", "9883"),
    "water deer": ("Hydropotes inermis", "9883"),
    "odocoileus virginianus": ("Odocoileus virginianus", "9874"),
    "white tailed deer": ("Odocoileus virginianus", "9874"),
    "bubalus bubalis": ("Bubalus bubalis", "89462"),
    "water buffalo": ("Bubalus bubalis", "89462"),
    "vicugna pacos": ("Vicugna pacos", "30538"),
    "alpaca": ("Vicugna pacos", "30538"),
    "ursus americanus": ("Ursus americanus", "9643"),
    "american black bear": ("Ursus americanus", "9643"),
    "crassostrea virginica": ("Crassostrea virginica", "6565"),
    "eastern oyster": ("Crassostrea virginica", "6565"),
    "stomoxys": ("Stomoxys", "35569"),
    "skeletonema costatum": ("Skeletonema costatum", "2843"),
    "vicia sativa": ("Vicia sativa", "3908"),
    "common vetch": ("Vicia sativa", "3908"),
}

HOST_CONTEXT_SYNONYMS = {
    "patient": ("Homo sapiens", "9606"),
    "human patient": ("Homo sapiens", "9606"),
    "human listeriosis": ("Homo sapiens", "9606"),
    "healthy people": ("Homo sapiens", "9606"),
    "non hospitalized person": ("Homo sapiens", "9606"),
    "hospital patients": ("Homo sapiens", "9606"),
    "infant": ("Homo sapiens", "9606"),
    "infants": ("Homo sapiens", "9606"),
    "child": ("Homo sapiens", "9606"),
    "adult": ("Homo sapiens", "9606"),
    "homo": ("Homo sapiens", "9606"),
    "male": ("Homo sapiens", "9606"),
    "female": ("Homo sapiens", "9606"),
    "mother": ("Homo sapiens", "9606"),
    "neonate": ("Homo sapiens", "9606"),
    "calf": ("Bos taurus", "9913"),
    "calves": ("Bos taurus", "9913"),
    "holstein": ("Bos taurus", "9913"),
    "tibetan sheep": ("Ovis aries", "9940"),
    "dairy cow": ("Bos taurus", "9913"),
    "steer": ("Bos taurus", "9913"),
    "heifer": ("Bos taurus", "9913"),
    "young chicken": ("Gallus gallus", "9031"),
    "chickens": ("Gallus gallus", "9031"),
    "young turkey": ("Meleagris gallopavo", "9103"),
}

HOST_BROAD_SYNONYMS = {
    "avian": ("Aves", "8782"),
    "aves": ("Aves", "8782"),
    "poultry": ("Aves", "8782"),
    "fish": ("Actinopterygii", "7898"),
    "fly": ("Diptera", "7147"),
    "flies": ("Diptera", "7147"),
    "mosquito": ("Culicidae", "7157"),
    "mosquitoes": ("Culicidae", "7157"),
    "crow": ("Corvus", "30420"),
    "gull": ("Laridae", "8911"),
    "blue mussel": ("Mytilus edulis complex", "6579"),
    "oyster": ("Ostreidae", "6563"),
    "oysters": ("Ostreidae", "6563"),
    "rodent": ("Rodentia", "9989"),
    "deer": ("Cervidae", "9850"),
    "bovinae": ("Bovinae", "27592"),
    "hedgehog": ("Erinaceidae", "9363"),
    "hedgehogs": ("Erinaceidae", "9363"),
    "animal": ("Metazoa", "33208"),
    "pepper": ("Capsicum", "4071"),
}

# Keep substring matching limited to curated aliases. Large TaxonKit imports are
# exact-match rules; scanning them as substring regexes makes refreshes slow and
# can over-match scientific names embedded in free text.
HOST_SUBSTRING_SYNONYMS = dict(HOST_SYNONYMS)

SAMPLE_TYPE_SYNONYMS = {
    "blood": "blood",
    "feces": "feces/stool",
    "faeces": "feces/stool",
    "fecal": "feces/stool",
    "faecal": "feces/stool",
    "stool": "feces/stool",
    "urine": "urine",
    "sputum": "sputum",
    "saliva": "saliva",
    "swab": "swab",
    "stool swab": "fecal/stool swab",
    "fecal swab": "fecal/stool swab",
    "faecal swab": "fecal/stool swab",
    "rectal swab": "rectal swab",
    "rectum swab": "rectal swab",
    "swab rectum": "rectal swab",
    "stool/rectal swab": "rectal swab",
    "rectal swab/stool": "rectal swab",
    "anal swab": "perianal/anal swab",
    "perianal swab": "perianal/anal swab",
    "perirectal swab": "perianal/anal swab",
    "nasal swab": "nasal swab",
    "nose swab": "nasal swab",
    "np swab": "nasopharyngeal swab",
    "nasopharyngeal swab": "nasopharyngeal swab",
    "oropharyngeal swab": "oropharyngeal swab",
    "throat swab": "throat swab",
    "wound swab": "wound swab",
    "wound skin swab": "wound/skin swab",
    "swab wound": "wound swab",
    "swab wound": "wound swab",
    "swab_wound": "wound swab",
    "pus swab": "wound/pus swab",
    "pus wound swab": "wound/pus swab",
    "oral swab": "oral swab",
    "tonsil swab": "tonsil swab",
    "sinus swab": "sinus swab",
    "patient sinus swab": "sinus swab",
    "intrauterine swab": "intrauterine swab",
    "airsac swab": "air sac swab",
    "vaginal swab": "vaginal swab",
    "urethral swab": "urethral swab",
    "cloacal swab": "cloacal swab",
    "skin swab": "skin swab",
    "ear swab": "ear swab",
    "eye swab": "eye swab",
    "surveillance swab": "surveillance swab",
    "screening swab": "surveillance swab",
    "body swab": "body swab",
    "environmental swab": "environmental swab",
    "environmental swab sponge": "environmental swab",
    "food contact surface": "food-contact surface",
    "food-contact surface": "food-contact surface",
    "non food contact surface": "non-food-contact surface",
    "non-food-contact surface": "non-food-contact surface",
    "environmental non food contact surface": "non-food-contact surface",
    "environmental food contact surface": "food-contact surface",
    "carcass swab": "carcass swab",
    "brisket swab": "carcass swab",
    "rump swab": "carcass swab",
    "drag swab": "drag swab",
    "chicken drag swab": "drag swab",
    "hatchery swab": "hatchery swab",
    "food": "food",
    "food product": "food product",
    "food products": "food product",
    "food source": "food product",
    "food processing environment": "food processing environment",
    "food production environment": "food processing environment",
    "food producing environment": "food processing environment",
    "food processing facility": "food processing environment",
    "food industry environment": "food processing environment",
    "food producing environment surface": "food processing environment",
    "ready to eat food": "ready-to-eat food",
    "rte food": "ready-to-eat food",
    "finished rte food product dairy": "dairy food",
    "dairy food": "dairy food",
    "pet food": "pet food",
    "finished pet food": "pet food",
    "raw pet food": "pet food",
    "frozen raw pet food": "pet food",
    "fermented food": "fermented food",
    "meat": "meat",
    "seafood": "seafood",
    "milk": "milk",
    "dairy milk": "milk",
    "patient clinical": "patient/clinical",
    "clinical patient": "patient/clinical",
    "mixed culture": "mixed culture",
    "mixed host and symbiont culture": "mixed culture",
    "mix_culture": "mixed culture",
    "bacterial culture": "bacterial culture",
    "bacteria culture": "bacterial culture",
    "bacterial cell culture": "bacterial culture",
    "e.coli": "bacterial culture",
    "ecoli": "bacterial culture",
    "e coli": "bacterial culture",
    "e. coli": "bacterial culture",
    "escherichia coli": "bacterial culture",
    "e.coli culture": "bacterial culture",
    "ecoli culture": "bacterial culture",
    "e coli culture": "bacterial culture",
    "e. coli culture": "bacterial culture",
    "escherichia coli culture": "bacterial culture",
    "e.coli isolate": "pure/single culture",
    "ecoli isolate": "pure/single culture",
    "e coli isolate": "pure/single culture",
    "e. coli isolate": "pure/single culture",
    "escherichia coli isolate": "pure/single culture",
    "e coli jm109": "pure/single culture",
    "ecoli jm109": "pure/single culture",
    "dh5alpha": "pure/single culture",
    "dh5 alpha": "pure/single culture",
    "dh10b": "pure/single culture",
    "top10": "pure/single culture",
    "xl1 blue": "pure/single culture",
    "xl1 blue mrf": "pure/single culture",
    "solr": "pure/single culture",
    "bl21": "pure/single culture",
    "jm109": "pure/single culture",
    "atcc strain": "pure/single culture",
    "control strain": "pure/single culture",
    "vaccine strain": "pure/single culture",
    "lab strain": "pure/single culture",
    "laboratory strain": "pure/single culture",
    "zymobiomics microbial community standard strain": "pure/single culture",
    "pure culture": "pure/single culture",
    "pure cultures of bacteria": "pure/single culture",
    "pure culture of bacteria": "pure/single culture",
    "pure bacterial culture": "pure/single culture",
    "pure culture one microbial species": "pure/single culture",
    "bacterial strain pure culture": "pure/single culture",
    "bacterial pure culture": "pure/single culture",
    "isolated pure culture": "pure/single culture",
    "pure isolate culture": "pure/single culture",
    "axenic culture": "pure/single culture",
    "monoculture": "pure/single culture",
    "bacterial monoculture": "pure/single culture",
    "bacterial monoisolate": "pure/single culture",
    "monoisolate": "pure/single culture",
    "single culture": "pure/single culture",
    "strain culture": "pure/single culture",
    "cultured isolate": "pure/single culture",
    "microbial isolate culture": "pure/single culture",
    "cell culture": "cell culture",
    "celll culture": "cell culture",
    "metagenomic assembly": "metagenomic assembly",
    "metagenome assembly": "metagenomic assembly",
    "enrichment culture": "enrichment culture",
    "liquid culture": "liquid culture",
    "isolate in liquid culture": "liquid culture",
    "plate culture": "plate culture",
    "laboratory culture": "laboratory culture",
    "filtered clone culture": "clone culture",
    "microbial culture": "microbial culture",
    "microbe culture": "microbial culture",
    "unicyanobacterial culture": "unicyanobacterial culture",
    "leaf tissue": "leaf tissue",
    "intestinal content": "gut content",
    "intestinal contents": "gut content",
    "gut content": "gut content",
    "gut contents": "gut content",
    "intestinal sample": "gut content",
    "cloacal sample": "cloacal sample",
    "manure": "manure",
    "saline water": "saline water",
    "shucked": "processed shellfish tissue",
    "p trap": "trap sample",
    "p-trap": "trap sample",
}

ISOLATION_SOURCE_SYNONYMS = {
    "clinical": "clinical sample",
    "clinical sample": "clinical sample",
    "clinical material": "clinical sample",
    "clinical specimen": "clinical sample",
    "sterile site": "sterile body site",
    "normally sterile site": "sterile body site",
    "hospital": "healthcare facility",
    "intensive care unit": "healthcare facility",
    "nursing home": "healthcare facility",
    "food": "food/food product",
    "foods": "food/food product",
    "food product": "food/food product",
    "ready to eat food": "ready-to-eat food",
    "rte food": "ready-to-eat food",
    "milk": "milk",
    "dairy milk": "milk",
    "dairy": "dairy food",
    "cheese": "dairy food",
    "yogurt": "dairy food",
    "yoghurt": "dairy food",
    "meat": "meat product",
    "beef": "beef/meat product",
    "pork": "pork/meat product",
    "poultry": "host-associated context",
    "chicken meat": "poultry meat/product",
    "turkey meat": "turkey meat/product",
    "seafood": "seafood/aquatic food product",
    "fish product": "seafood/aquatic food product",
    "shrimp product": "seafood/aquatic food product",
    "oyster product": "seafood/aquatic food product",
    "shellfish product": "seafood/aquatic food product",
    "vegetable": "plant/produce food product",
    "vegetables": "plant/produce food product",
    "fruit": "plant/produce food product",
    "produce": "plant/produce food product",
    "spinach": "plant/produce food product",
    "lettuce": "plant/produce food product",
    "cantaloupe": "plant/produce food product",
    "papaya": "plant/produce food product",
    "strawberry": "plant/produce food product",
    "almond": "plant/produce food product",
    "kimchi": "fermented food",
    "root": "plant-associated material",
    "rhizosphere": "plant-associated material",
    "leaf": "plant-associated material",
    "leaf tissue": "plant-associated material",
    "nodule": "plant-associated material",
}

ENVIRONMENT_MEDIUM_SYNONYMS = {
    "soil": "soil",
    "sediment": "sediment",
    "water": "water",
    "river water": "river water",
    "water river": "river water",
    "lake water": "lake water",
    "pond water": "pond water",
    "water pond": "pond water",
    "surface water": "surface water",
    "canal water": "canal water",
    "creek water": "creek water",
    "dam water": "reservoir/dam water",
    "hot spring water": "hot spring water",
    "irrigation water": "irrigation water",
    "hospital wastewater": "hospital wastewater",
    "hospital waste water": "hospital wastewater",
    "domestic wastewater": "domestic wastewater",
    "domestic waste water": "domestic wastewater",
    "wastewater": "wastewater",
    "seawater": "seawater",
    "marine water": "seawater",
    "baltic sea water": "seawater",
    "freshwater": "freshwater",
    "fresh water stream": "freshwater",
    "non tidal fresh water river": "river water",
    "environment": "environmental sample",
    "environmental": "environmental sample",
    "environmental sample": "environmental sample",
    "natural free living": "natural/free-living",
    "natural / free living": "natural/free-living",
    "natural / free-living": "natural/free-living",
    "subsurface shale": "subsurface shale",
    "marine sediment": "marine sediment",
    "estuarine water": "estuarine water",
    "estuarine open water surface layer": "estuarine water",
    "oyster pond": "Oyster pond",
}

ISOLATION_SITE_SYNONYMS = {
    "blood": "blood",
    "blood culture": "blood",
    "urine": "urine",
    "sputum": "sputum",
    "respiratory": "respiratory tract",
    "respiratory tract": "respiratory tract",
    "tracheal aspirate": "tracheal aspirate",
    "tracheal secretion": "tracheal aspirate",
    "bronchoalveolar lavage": "bronchoalveolar lavage fluid",
    "bronchoalveolar lavage fluid": "bronchoalveolar lavage fluid",
    "balf": "bronchoalveolar lavage fluid",
    "nasopharynx": "nasopharynx/oropharynx",
    "nasopharyngeal": "nasopharynx/oropharynx",
    "oropharynx": "nasopharynx/oropharynx",
    "oropharyngeal": "nasopharynx/oropharynx",
    "pharynx": "nasopharynx/oropharynx",
    "pharyngeal": "nasopharynx/oropharynx",
    "throat": "nasopharynx/oropharynx",
    "nose": "nasal site",
    "nasal": "nasal site",
    "nares": "nasal site",
    "nasal cavity": "nasal site",
    "oral": "oral cavity",
    "oral cavity": "oral cavity",
    "dental plaque": "dental plaque",
    "saliva": "saliva",
    "skin": "skin",
    "wound": "wound",
    "abscess": "abscess",
    "pus": "pus",
    "liver abscess": "liver abscess",
    "pleural fluid": "pleural fluid",
    "bodily fluid": "body fluid",
    "fluid": "body fluid",
    "urethra": "urethra/penis",
    "urethral": "urethra/penis",
    "penis": "urethra/penis",
    "penis urethra": "urethra/penis",
    "vagina": "vaginal site",
    "vaginal": "vaginal site",
    "cervix": "cervix",
    "ectocervical mucosa": "cervix",
    "intestine": "gut content",
    "intestinal tract": "gut content",
    "gastrointestinal tract": "gut content",
    "gut": "gut content",
    "gut content": "gut content",
    "intestinal content": "gut content",
    "cecum": "gut content",
    "cecal content": "gut content",
    "caecum": "gut content",
    "caecal content": "gut content",
    "stomach": "stomach",
    "gastric biopsy": "gastric biopsy",
    "rumen": "rumen",
    "manure": "manure",
    "feces": "feces/stool",
    "faeces": "feces/stool",
    "stool": "feces/stool",
    "liver": "liver",
    "brain": "brain",
    "kidney": "kidney",
    "spleen": "spleen",
    "bone": "bone",
    "eye": "eye",
    "ear": "ear",
    "placenta": "placenta",
    "lymph node": "lymph node",
    "root": "root",
    "rhizosphere": "rhizosphere",
    "leaves": "leaf tissue",
    "leaf": "leaf tissue",
    "leaf tissue": "leaf tissue",
    "plant": "plant-associated material",
    "nodule": "root nodule",
}

ENVIRONMENT_BROAD_SYNONYMS = {
    "human-associated": "host-associated environment",
    "host-associated": "host-associated environment",
    "animal-associated": "host-associated environment",
    "plant-associated": "plant-associated environment",
    "plant associated": "plant-associated environment",
    "rhizosphere": "plant-associated environment",
    "root": "plant-associated environment",
    "soil": "terrestrial environment",
    "terrestrial": "terrestrial environment",
    "freshwater": "freshwater environment",
    "fresh water": "freshwater environment",
    "river": "freshwater environment",
    "lake": "freshwater environment",
    "pond": "freshwater environment",
    "marine": "marine environment",
    "sea": "marine environment",
    "seawater": "marine environment",
    "estuary": "estuarine environment",
    "estuarine": "estuarine environment",
    "wastewater": "wastewater/sewage",
    "sewage": "wastewater/sewage",
    "activated sludge": "wastewater/sewage",
    "food": "food-associated environment",
    "food product": "food-associated environment",
    "dairy": "food-associated environment",
    "meat": "food-associated environment",
    "seafood": "food-associated environment",
    "clinical": "clinical/host-associated material",
    "patient clinical": "clinical/host-associated material",
    "hospital": "healthcare-associated environment",
    "healthcare": "healthcare-associated environment",
    "laboratory": "laboratory environment",
    "built environment": "built environment",
    "air": "air/built environment",
    "sediment": "aquatic sediment environment",
    "geologic": "geologic/extreme environment",
    "hydrothermal": "geologic/extreme environment",
    "hot spring": "geologic/extreme environment",
}

ENVIRONMENT_LOCAL_SYNONYMS = {
    "river": "river",
    "stream": "stream",
    "lake": "lake",
    "pond": "pond",
    "canal": "canal",
    "irrigation canal": "irrigation canal",
    "estuary": "estuary",
    "hot spring": "hot spring",
    "groundwater": "groundwater",
    "surface layer": "surface water layer",
    "hospital": "healthcare facility",
    "hospital environment": "healthcare facility",
    "icu": "intensive care unit",
    "intensive care unit": "intensive care unit",
    "nursing home": "long-term care facility",
    "long term care facility": "long-term care facility",
    "farm": "farm",
    "dairy farm": "dairy farm",
    "poultry farm": "poultry farm",
    "food processing facility": "food processing facility",
    "food processing environment": "food processing facility",
    "food-contact surface": "food-contact surface",
    "non-food-contact surface": "non-food-contact surface",
    "cleanroom": "cleanroom",
    "cleanroom floor": "cleanroom floor",
    "laboratory": "laboratory",
    "cave": "cave",
    "cold seep": "cold seep",
    "glacier": "glacier",
    "subsurface shale": "subsurface shale",
}

HOST_DISEASE_SYNONYMS = {
    "healthy": "healthy/no disease reported",
    "healthy human": "healthy/no disease reported",
    "normal": "healthy/no disease reported",
    "none": "healthy/no disease reported",
    "not applicable": "",
    "diarrhea": "diarrheal disease",
    "diarrhoea": "diarrheal disease",
    "gastroenteritis": "gastroenteritis",
    "colitis": "colitis",
    "urinary tract infection": "urinary tract infection",
    "uti": "urinary tract infection",
    "pneumonia": "pneumonia",
    "respiratory infection": "respiratory infection",
    "wound infection": "wound infection",
    "infection": "infection",
    "intra abdominal infection": "intra-abdominal infection",
    "itra abdominal tract infection": "intra-abdominal infection",
    "ssti": "skin and soft tissue infection",
    "skin and soft tissue infection": "skin and soft tissue infection",
    "osteomyelitis": "osteomyelitis",
    "endocarditis": "endocarditis",
    "bronchiectasis": "bronchiectasis",
    "non cf bronchiectasis": "bronchiectasis",
    "leg infection": "skin and soft tissue infection",
    "infection of surgical site": "surgical site infection",
    "sepsis": "sepsis/bacteremia",
    "bacteremia": "sepsis/bacteremia",
    "bloodstream infection": "sepsis/bacteremia",
    "meningitis": "meningitis",
    "periodontitis": "periodontal disease",
    "dental caries": "dental caries",
    "cystic fibrosis": "cystic fibrosis",
    "cf": "cystic fibrosis",
    "inflammatory bowel disease": "inflammatory bowel disease",
    "ibd": "inflammatory bowel disease",
    "crohn disease": "inflammatory bowel disease",
    "ulcerative colitis": "inflammatory bowel disease",
    "mastitis": "mastitis",
    "aborted fetus": "abortion/reproductive disorder",
    "aborted fetuses": "abortion/reproductive disorder",
    "aborted calf": "abortion/reproductive disorder",
    "aborted donkey": "abortion/reproductive disorder",
    "aborted bovine fetus": "abortion/reproductive disorder",
    "aborted piglet fetus": "abortion/reproductive disorder",
    "aborted tissues of horses": "abortion/reproductive disorder",
    "aborted uteroplacental unit": "abortion/reproductive disorder",
    "aborted uteroplancental unit": "abortion/reproductive disorder",
    "septicemia in a late term aborted caprine fetus": "sepsis/bacteremia",
    "leukemia": "leukemia",
    "leukemia cell line": "leukemia",
    "myelogenous leukemia cell line": "leukemia",
    "chronic myeloid leukemia": "leukemia",
    "listeriosis": "listeriosis",
    "salmonellosis": "salmonellosis",
    "campylobacteriosis": "campylobacteriosis",
}

HOST_HEALTH_STATE_SYNONYMS = {
    "healthy": "healthy",
    "healthy group": "healthy",
    "clinically healthy": "healthy",
    "healthy no disease reported": "healthy",
    "healthy/no disease reported": "healthy",
    "control": "healthy",
    "healthy control": "healthy",
    "normal": "healthy",
    "asymptomatic": "asymptomatic",
    "symptomatic": "symptomatic",
    "diseased": "diseased",
    "disease": "diseased",
    "infection": "diseased",
    "community acquired": "diseased",
    "hospital acquired": "diseased",
    "contact case": "exposure/contact context",
    "sick": "diseased",
    "patient": "patient",
    "hospitalized": "hospitalized",
    "non hospitalized": "non-hospitalized",
    "not hospitalized": "non-hospitalized",
    "convalescent": "convalescent",
}

CONTROLLED_CATEGORY_ONTOLOGY_IDS = {
    "blood": "UBERON:0000178",
    "feces/stool": "UBERON:0001988",
    "urine": "UBERON:0001088",
    "sputum": "UBERON:0007311",
    "saliva": "UBERON:0001836",
    "soil": "ENVO:00001998",
    "sediment": "ENVO:00002007",
    "water": "ENVO:00002006",
    "wastewater": "ENVO:00002001",
    "seawater": "ENVO:00002149",
    "freshwater": "ENVO:00002011",
    "environmental sample": "ENVO:00010483",
}

STANDARDIZATION_SPELLING_CORRECTIONS = {
    "homo sapines": "homo sapiens",
    "homo sapien": "homo sapiens",
    "homosapien": "homo sapiens",
    "homosapiens": "homo sapiens",
    "homo-sapiens": "homo sapiens",
    "sus scofa": "sus scrofa",
    "sus scofa domesticus": "sus scrofa domesticus",
    "e coli": "escherichia coli",
    "e. coli": "escherichia coli",
    "faeces": "feces",
    "faecal": "fecal",
    "waste water": "wastewater",
    "sea water": "seawater",
    "fresh water": "freshwater",
}

STANDARDIZATION_MISSING_TOKENS = {
    "absent",
    "unknown",
    "not known",
    "not available",
    "not provided",
    "not collected",
    "not applicable",
    "not recorded",
    "not determined",
    "none",
    "restricted access",
    "missing",
    "mising",
    "misisng",
    "na",
    "n a",
    "-",
}

HOST_NOT_IDENTIFIABLE_TOKENS = {
    "20 jun 2016",
    "65",
    "74",
    "113",
    "318",
    "dh10b",
    "dh10b life technologies",
    "dh10b phage resistant",
    "dh5alpha",
    "e.coli",
    "e coli",
    "e coli jm109",
    "emdh10b",
    "ew",
    "guangdong microbial culture collection center gdmcc",
    "invasive pneumococcal disease",
    "instituto de productos lacteos de asturias ipla csic",
    "isolate",
    "lab",
    "lab strain",
    "lab-strain",
    "lori",
    "nist mixed microbial rm strain",
    "parent",
    "petroleum microbiology laboratory",
    "pet",
    "pets",
    "companion pet",
    "canada saskatchewan",
    "seth lab strain",
    "solr",
    "solr stratagene kanamycin resistant",
    "tbg",
    "top10",
    "ucc strain",
    "uliege",
    "unassigned viruses",
    "vaccine strain",
    "xl1 blue mrf",
    "zymobiomics microbial community standard strain",
}

HOST_TAXONOMY_PRIORITY_TOKENS = {
    "actinobacteria",
    "bacteria",
    "prawn",
    "prawns",
    "salmon",
    "water buffalo",
    "water deer",
}

LAB_MICROBIAL_CONTEXT_LABEL = "lab bacterial strain/culture"
E_COLI_CONTEXT_LABEL = "Escherichia coli/lab bacterial culture"

MICROBIAL_SELF_DESCRIPTOR_TERMS = (
    "acinetobacter",
    "bacillus",
    "bacteria",
    "bacterium",
    "campylobacter",
    "clostridioides",
    "clostridium",
    "enterobacter",
    "enterococcus",
    "escherichia",
    "escherichia coli",
    "helicobacter",
    "klebsiella",
    "lactobacillus",
    "legionella",
    "listeria",
    "microbe",
    "microbial",
    "mycobacterium",
    "neisseria",
    "pseudomonas",
    "salmonella",
    "salmonella enterica",
    "shigella",
    "staphylococcus",
    "streptococcus",
    "vibrio",
    "wolbachia",
    "yersinia",
)

MICROBIAL_CONTEXT_WORD_PATTERN = re.compile(
    r"\b(?:"
    r"axenic|bl21|clone|cloned|competent cells?|culture|cultured|expression host|"
    r"host strain|isolate|isolated|jm109|lab(?:oratory)? strain|monoisolate|"
    r"pure culture|strain|strains"
    r")\b",
    re.IGNORECASE,
)


NON_HOST_SOURCE_HINTS = {
    "food",
    "food product",
    "seafood",
    "shrimp",
    "dairy product",
    "milk",
    "milk product",
    "milk products",
    "milk powder",
    "soil",
    "stool",
    "feces",
    "faeces",
    "fecal",
    "faecal",
    "meat",
    "pork",
    "beef",
    "water",
    "wastewater",
    "environment",
    "environmental",
    "environmental sample",
    "hospital environment",
    "icu environment",
    "farm environment",
    "metagenome",
    "soil metagenome",
    "freshwater metagenome",
    "indoor metagenome",
    "marine metagenome",
    "algae",
    "marine algae",
    "marine red algae",
    "not applicable genepio 0001619",
    "subsurface shale",
    "p trap",
    "p-trap",
    "natural free living",
    "natural / free living",
    "natural / free-living",
    "non host associated",
    "non-host associated",
    "land crag",
    "u bend",
    "u-bend",
    "laboratory",
    "in vitro",
    "resource islands",
    "ww outflow samariterstift",
    "free living",
    "marl pit",
    "dairy waste",
    "rivers natural pond",
    "unopened",
    "seeds",
    "leafy green",
    "leafy vegetable",
}

NON_HOST_SOURCE_PATTERN = re.compile(
    r"\b(?:stool|feces|faeces|fecal|faecal|meat|pork|beef|food|environment|environmental|"
    r"metagenome|soil|water|wastewater|swab|sample|algae)\b",
    re.IGNORECASE,
)

HOST_CONTEXT_SOURCE_DOMINANT_PATTERN = re.compile(
    r"\b(?:"
    r"activated sludge|active sludge|sludge|waste\s*water|wastewater|wwtp|"
    r"treatment plant|processing plant|preprocessing plant|leachate|reactor|digester|influent|effluent|"
    r"production environment|factory|food plant|milk powder plant|"
    r"environmental swab|environmental sponge|swab sponge|sponge powder|"
    r"air sample|drag swab|field|patient room|infant formula|powdered infant formula|"
    r"drinking water|freshwater|saline water|seawater|sea\s*water|marine water|lake water|river water|"
    r"brackish water|deep[-\s]*sea water|sea[-\s]*surface|surface water|marine aquarium|microplastic|"
    r"soil|sediment|rhizosphere|plankton|plant environment|poultry environment|"
    r"environmental donor|environmental sample|environmental waters"
    r")\b",
    re.IGNORECASE,
)

HOST_CONTEXT_MATERIAL_EVIDENCE_PATTERN = re.compile(
    r"\b(?:"
    r"feces|faeces|fecal|faecal|stool|manure|gut|intestinal|rectal|oral|"
    r"urine|blood|milk|tissue|cadaver|carcass|meat"
    r")\b",
    re.IGNORECASE,
)


E_COLI_HOST_CONTEXT_PATTERN = re.compile(
    r"(^|\b)(?:e\s*[\.,-]?\s*coli|ecoli|escherichia\s+coli)(?:\b|$)",
    re.IGNORECASE,
)

LAB_MICROBIAL_HOST_CONTEXT_PATTERN = re.compile(
    r"\b(?:"
    r"dh[\s-]*5[\s-]*alpha|dh5alpha|dh10b|top[\s-]*10|xl[\s-]*1[\s-]*blue(?:[\s-]*mrf)?|"
    r"jm[\s-]*109|bl[\s-]*21|solr|lab(?:oratory)?[\s-]*strain|control[\s-]*strain|"
    r"vaccine[\s-]*strain|atcc[\s-]*strain|microbial[\s-]*community[\s-]*standard[\s-]*strain|"
    r"zymobiomics[\s-]*microbial[\s-]*community[\s-]*standard[\s-]*strain"
    r")\b",
    re.IGNORECASE,
)

SEAFOOD_SOURCE_CONTEXT_PATTERN = re.compile(
    r"\b(?:frozen|raw|retail|market|product|meat|fillet|block|food|sauce|processed|dried|smoked|shucked)\b"
    r".*\b(?:fish|trout|salmon|scallop|shrimp|prawn|oyster|mussel|clam|shellfish|seafood)\b|"
    r"\b(?:fish|trout|salmon|scallop|shrimp|prawn|oyster|mussel|clam|shellfish|seafood)\b"
    r".*\b(?:frozen|raw|retail|market|product|meat|fillet|block|food|sauce|processed|dried|smoked|shucked)\b",
    re.IGNORECASE,
)


def compact_lookup_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_standardization_lookup(value))


def microbial_self_descriptor_context(value: Any) -> tuple[str, str] | None:
    cleaned = clean_host_lookup_text(value)
    compact = compact_lookup_text(value)
    if not cleaned:
        return None
    if E_COLI_HOST_CONTEXT_PATTERN.search(cleaned) or "ecoli" in compact or "escherichiacoli" in compact:
        if re.search(r"\b(?:isolate|strain|jm109|bl21|clone|cloned|pure culture|monoisolate)\b", cleaned):
            return E_COLI_CONTEXT_LABEL, "pure/single culture"
        return E_COLI_CONTEXT_LABEL, "bacterial culture"
    if LAB_MICROBIAL_HOST_CONTEXT_PATTERN.search(cleaned):
        return LAB_MICROBIAL_CONTEXT_LABEL, "pure/single culture"
    if not MICROBIAL_CONTEXT_WORD_PATTERN.search(cleaned):
        return None
    for term in MICROBIAL_SELF_DESCRIPTOR_TERMS:
        normalized_term = normalize_standardization_lookup(term)
        if re.search(rf"(^|\s){re.escape(normalized_term)}(\s|$)", cleaned):
            return "microbial self/lab culture descriptor", "pure/single culture"
    return None


def context_host_recovery_blocked(field_name: str, value: Any) -> bool:
    cleaned = clean_host_lookup_text(value)
    if not cleaned:
        return True
    if field_name in {"Environment Medium", "Environment (Local Scale)", "Environment (Broad Scale)"}:
        return True
    if SEAFOOD_SOURCE_CONTEXT_PATTERN.search(cleaned):
        return True
    if re.search(
        r"\b(?:"
        r"processing plant|preprocessing plant|production environment|factory|food plant|milk powder plant|"
        r"air sample|drag swab|patient room|infant formula|powdered infant formula"
        r")\b",
        cleaned,
    ):
        return True
    if HOST_CONTEXT_SOURCE_DOMINANT_PATTERN.search(cleaned) and not HOST_CONTEXT_MATERIAL_EVIDENCE_PATTERN.search(cleaned):
        return True
    if re.search(r"\b(?:waste\s*water|wastewater|treatment plant|sludge|leachate|reactor|effluent|influent)\b", cleaned):
        return True
    if re.search(r"\b(?:environmental swab|environmental sponge|swab sponge|sponge powder)\b", cleaned):
        return True
    return False

HOST_CONTEXT_SOURCE_SKIP_TERMS = {
    "tomato",
    "avocado",
    "cantaloupe",
    "lettuce",
    "cilantro",
    "spinach",
    "papaya",
    "vegetable",
    "leafy greens",
    "enoki mushroom",
    "kimchi",
    "peanut butter",
    "tree nut",
    "dairy product",
}

STANDARDIZATION_BROAD_CATEGORIES = {
    "blood": "clinical/host-associated material",
    "urine": "clinical/host-associated material",
    "sputum": "clinical/host-associated material",
    "respiratory sample": "clinical/host-associated material",
    "saliva": "clinical/host-associated material",
    "wound": "clinical/host-associated material",
    "skin": "clinical/host-associated material",
    "lung": "clinical/host-associated material",
    "clinical sample": "clinical/host-associated material",
    "clinical material": "clinical/host-associated material",
    "sterile body site": "clinical/host-associated material",
    "cerebrospinal fluid": "clinical/host-associated material",
    "nasopharynx": "clinical/host-associated material",
    "nasal site": "clinical/host-associated material",
    "oral cavity": "clinical/host-associated material",
    "throat": "clinical/host-associated material",
    "liver": "clinical/host-associated material",
    "intestine": "gut content",
    "feces/stool": "feces/stool",
    "fecal/stool swab": "swab",
    "rectal swab": "swab",
    "perianal/anal swab": "swab",
    "nasal swab": "swab",
    "nasopharyngeal swab": "swab",
    "oropharyngeal swab": "swab",
    "throat swab": "swab",
    "wound swab": "swab",
    "wound/skin swab": "swab",
    "wound/pus swab": "swab",
    "oral swab": "swab",
    "tonsil swab": "swab",
    "sinus swab": "swab",
    "intrauterine swab": "swab",
    "air sac swab": "swab",
    "vaginal swab": "swab",
    "urethral swab": "swab",
    "cloacal swab": "swab",
    "skin swab": "swab",
    "ear swab": "swab",
    "eye swab": "swab",
    "environmental swab": "swab",
    "surveillance swab": "swab",
    "body swab": "swab",
    "carcass swab": "swab",
    "drag swab": "swab",
    "hatchery swab": "swab",
    "food-contact surface": "food/processing environment",
    "non-food-contact surface": "food/processing environment",
    "food": "food",
    "food product": "food",
    "food processing environment": "food/processing environment",
    "ready-to-eat food": "food",
    "dairy food": "food/dairy",
    "pet food": "food",
    "fermented food": "food",
    "meat": "food/meat",
    "chicken meat": "food/meat",
    "turkey meat": "food/meat",
    "poultry": "host-associated context",
    "poultry meat": "food/meat",
    "pork": "food/meat",
    "beef": "food/meat",
    "seafood": "aquatic food product",
    "fish product": "aquatic food product",
    "freshwater fish product": "aquatic food product",
    "shrimp product": "aquatic food product",
    "oyster product": "aquatic food product",
    "shellfish product": "aquatic food product",
    "milk": "food/dairy",
    "biological product": "biological/clinical product",
    "water": "water",
    "river water": "water",
    "lake water": "water",
    "pond water": "water",
    "surface water": "water",
    "canal water": "water",
    "creek water": "water",
    "reservoir/dam water": "water",
    "hot spring water": "water",
    "irrigation water": "water",
    "wastewater": "water",
    "hospital wastewater": "water",
    "domestic wastewater": "water",
    "seawater": "water",
    "freshwater": "water",
    "estuarine water": "water",
    "saline water": "water",
    "soil": "soil",
    "sediment": "sediment",
    "marine sediment": "sediment",
    "environmental sample": "environmental material",
    "natural/free-living": "environmental material",
    "subsurface shale": "environmental material",
    "trap sample": "environmental material",
    "culture": "culture",
    "mixed culture": "culture",
    "pure/single culture": "culture",
    "bacterial culture": "culture",
    "cell culture": "culture",
    "microbial culture": "culture",
    "unicyanobacterial culture": "culture",
    "metagenomic assembly": "culture/assembly",
    "enrichment culture": "culture",
    "liquid culture": "culture",
    "plate culture": "culture",
    "laboratory culture": "culture",
    "clone culture": "culture",
    "microbial isolate": "culture/isolate",
    "single cell": "single cell",
    "DNA extract": "molecular extract",
    "FFPE tissue": "tissue",
    "sample": "sample",
    "culture medium": "culture medium",
    "gut": "gut content",
    "gut content": "gut content",
    "cloacal sample": "cloacal sample",
    "manure": "agricultural fecal material",
    "whole organism": "host-associated context",
    "human": "host-associated context",
    "pig": "host-associated context",
    "chicken": "host-associated context",
}

STANDARDIZATION_BROAD_CATEGORIES.update(
    {
        "healthcare facility": "healthcare-associated environment",
        "sewage": "wastewater/sewage",
        "hospital sewage": "wastewater/sewage",
        "activated sludge": "wastewater/sewage",
        "tracheal aspirate/secretion": "respiratory sample",
        "bronchoalveolar lavage fluid": "respiratory sample",
        "bronchial wash/lavage": "respiratory sample",
        "nasopharynx/oropharynx": "upper respiratory tract",
        "nasal cavity/sinus/upper respiratory tract": "upper respiratory site",
        "rectum/perianal region": "gastrointestinal site",
        "skin/body surface": "clinical/host-associated material",
        "lower respiratory tract/bronch/pleural cavity": "respiratory sample",
        "tonsil/oropharyngeal site": "upper respiratory tract",
        "nasal site": "upper respiratory site",
        "oral cavity": "oral cavity",
        "dental plaque": "oral cavity",
        "urogenital swab": "swab",
        "urogenital/reproductive swab": "swab",
        "urogenital reproductive swab": "swab",
        "urethra/penis": "urogenital site",
        "urogenital": "urogenital site",
        "cervix": "urogenital site",
        "ectocervical mucosa": "urogenital site",
        "pus": "clinical fluid/material",
        "abscess": "clinical fluid/material",
        "liver abscess": "clinical fluid/material",
        "bodily fluid": "clinical fluid/material",
        "pleural fluid": "clinical fluid/material",
        "drainage": "clinical fluid/material",
        "aspirate": "clinical fluid/material",
        "gastric biopsy": "gut content",
        "stomach": "gut content",
        "rumen": "gut content",
        "brain": "clinical/host-associated material",
        "kidney": "clinical/host-associated material",
        "spleen": "clinical/host-associated material",
        "bone": "clinical/host-associated material",
        "eye": "clinical/host-associated material",
        "ear": "clinical/host-associated material",
        "placenta": "clinical/host-associated material",
        "lymph node": "clinical/host-associated material",
        "root": "plant-associated material",
        "rhizosphere": "plant-associated material",
        "leaves": "plant-associated material",
        "plant": "plant-associated material",
        "cucumber": "plant-associated material",
        "potato": "plant-associated material",
        "nodule": "plant-associated material",
        "kratom": "plant-associated material",
        "groundwater": "water",
        "river": "water",
        "canal": "water",
        "irrigation canal": "water",
        "stream": "water",
        "pond": "water",
        "estuary": "water",
        "hot spring": "water",
        "surface layer": "water",
        "deep-sea hydrothermal deposit": "environmental/geologic material",
        "produced fluids from hydraulically fractured shales": "environmental/geologic material",
        "ice core section from central arctic ocean": "environmental/geologic material",
        "core": "environmental/geologic material",
        "metadata descriptor/non-source": "metadata descriptor / non-source",
        "urinary tract": "urogenital site",
        "rectum": "gut content",
        "rectovaginal site": "clinical/host-associated material",
        "urogenital/gastrointestinal site": "clinical/host-associated material",
        "host-associated organism": "host-associated context",
        "groin": "clinical/host-associated material",
        "bile": "clinical fluid/material",
        "bloodstream": "clinical fluid/material",
        "biopsy": "clinical/host-associated material",
        "gill": "animal tissue/site",
        "trachea": "respiratory sample",
        "secretion": "clinical fluid/material",
        "sink": "built environment",
        "drain": "built environment",
        "laboratory": "laboratory environment",
        "farm": "agricultural environment",
        "dairy farm": "agricultural environment",
        "deciduous forest": "environmental material",
        "ready-to-eat product": "food",
        "ice cream": "food/dairy",
        "prawn product": "aquatic food product",
        "chicken carcass": "food/meat",
        "ground chicken": "food/meat",
        "spinach": "food/produce",
        "papaya": "food/produce",
        "vegetable": "food/produce",
        "leafy greens": "food/produce",
        "tree nut": "food/produce",
        "peanut butter": "food/plant product",
        "tooth": "oral cavity",
        "catheter": "medical device",
        "sludge": "wastewater/sewage",
        "metagenome": "metadata descriptor / non-source",
        "wildlife": "host-associated context",
        "wood": "plant-associated material",
        "colon contents": "gut content",
        "vagina": "urogenital site",
        "long-term care facility": "healthcare-associated environment",
        "anaerobic bioreactor effluent": "wastewater/sewage",
        "hospital wastewater": "wastewater/sewage",
        "salmon": "host-associated context",
        "surface": "surface sample",
        "heart": "clinical/host-associated material",
        "ascites": "clinical fluid/material",
        "broiler": "host-associated context",
        "environmental sample": "environmental material",
        "endovascular": "clinical/host-associated material",
        "sponge": "surface/sample collection material",
        "intestinal epithelial cells": "gut/host-associated material",
        "mammary gland": "clinical/host-associated material",
        "cubital fossa": "clinical/host-associated material",
        "abdomen": "clinical/host-associated material",
        "genitourinary tract": "urogenital site",
        "cleanroom floor": "built environment",
        "air": "environmental material",
        "cave biofilm": "environmental/geologic material",
        "cold seep": "environmental/geologic material",
        "glacier": "environmental/geologic material",
        "terrestrial environment": "environmental material",
        "kimchi": "fermented food",
        "enoki mushroom": "food/produce",
        "tomato": "food/produce",
        "avocado": "food/produce",
        "cantaloupe": "food/produce",
        "lettuce": "food/produce",
        "cilantro": "food/produce",
        "dairy product": "food/dairy",
        "catfish product": "aquatic food product",
        "anaerobic digester": "wastewater/organic waste",
        "klicava reservoir": "water",
        "kli cava reservoir": "water",
        "kl ava reservoir": "water",
        "řívov reservoir": "water",
        "rimov reservoir": "water",
        "r imov reservoir": "water",
        "římov reservoir": "water",
        "mov reservoir": "water",
        "große fuchskuhle sw lake": "water",
        "grosse fuchskuhle sw lake": "water",
        "gro e fuchskuhle sw lake": "water",
        "maggiore lake": "water",
        "lake washington": "water",
        "hallstatter see lake": "water",
        "hallstätter see lake": "water",
        "hallst tter see lake": "water",
        "volvi lake": "water",
        "lugano lake": "water",
        "landstejn reservoir": "water",
        "landštejn reservoir": "water",
        "land tejn reservoir": "water",
        "taltowisko lake": "water",
        "tałtowisko lake": "water",
        "ta towisko lake": "water",
        "thunersee lake": "water",
        "most lake": "water",
        "garda lake": "water",
        "zurichsee lake": "water",
        "zürichsee lake": "water",
        "z richsee lake": "water",
        "mediterranean sea": "water",
        "saanich inlet": "water",
        "peruvian upwelling": "water",
        "aquatic biome": "water",
        "brine pool": "water",
        "bottom 10cm of land fast sea ice": "environmental/geologic material",
        "hydrothermal vent": "environmental/geologic material",
        "hydrothermal plume": "environmental/geologic material",
        "tui malila hydrothermal plume": "environmental/geologic material",
        "mariner hydrothermal plume": "environmental/geologic material",
        "tahi moana above plume background": "environmental/geologic material",
        "redox gradient": "environmental/geologic material",
        "rock": "environmental/geologic material",
        "bauxite residue": "environmental/geologic material",
        "hydrocarbon": "environmental/geologic material",
        "input used in hydraulically fractured shales": "environmental/geologic material",
        "wall biofilm": "biofilm",
        "bioreactor": "built environment",
        "primary rapid sand filter": "built environment",
        "leachate from a leachate well at an active municipal landfill": "environmental material",
        "neus b leachate from a leachate well at an active municipal landfill in the north eastern united states": "environmental material",
        "neus c leachate from a leachate well at an active municipal landfill in the north eastern united states": "environmental material",
        "cultured embryonic stem cells": "culture",
        "free living fraction": "environmental material",
        "cortex": "clinical/host-associated material",
        "whole polyp cell suspension gfplow rfplow": "clinical/host-associated material",
        "whole polyp cell suspension neuron": "clinical/host-associated material",
    }
)

for _approved_broad_category in (
    "clinical/host-associated material",
    "host-associated context",
    "feces/stool",
    "food",
    "food/meat",
    "food/dairy",
    "food/produce",
    "food/plant product",
    "food/processing environment",
    "water",
    "wastewater/sewage",
    "soil",
    "sediment",
    "environmental material",
    "environmental/geologic material",
    "healthcare-associated environment",
    "agricultural environment",
    "agricultural fecal material",
    "animal-associated environment",
    "plant-associated material",
    "culture",
    "culture/assembly",
    "culture/isolate",
    "culture medium",
    "laboratory environment",
    "built environment",
    "surface sample",
    "biofilm",
    "respiratory sample",
    "upper respiratory tract",
    "upper respiratory site",
    "oral cavity",
    "urogenital site",
    "gastrointestinal site",
    "gut content",
    "tissue",
    "swab",
    "clinical fluid/material",
    "medical device",
    "aquatic food product",
    "biological/clinical product",
    "molecular extract",
    "single cell",
    "sample",
    "cloacal sample",
    "fermented food",
    "surface/sample collection material",
    "gut/host-associated material",
    "wastewater/organic waste",
    "metadata descriptor / non-source",
):
    STANDARDIZATION_BROAD_CATEGORIES.setdefault(_approved_broad_category, _approved_broad_category)

# Keep anatomical specificity in Isolation_Site_SD / Host_Anatomical_Site_SD,
# not in broad source/sample fields.
STANDARDIZATION_BROAD_CATEGORIES.update(
    {
        "oral cavity": "clinical/host-associated material",
        "dental plaque": "clinical/host-associated material",
        "urogenital site": "clinical/host-associated material",
        "urogenital tract": "clinical/host-associated material",
        "gastrointestinal site": "clinical/host-associated material",
        "rectum/perianal region": "clinical/host-associated material",
        "nasal cavity/sinus/upper respiratory tract": "clinical/host-associated material",
        "skin/body surface": "clinical/host-associated material",
        "breast": "clinical/host-associated material",
        "organ/tissue site": "clinical/host-associated material",
        "cloaca": "clinical/host-associated material",
    }
)

HOST_ONLY_SAMPLE_TYPE_TERMS = {
    "human",
    "patient",
    "people",
    "animal",
    "mammal",
    "bird",
    "poultry",
    "cattle",
    "cow",
    "pig",
    "swine",
    "chicken",
    "fish",
    "plant",
    "bacteria",
    "organism",
    "host",
    "whole organism",
}

NON_COUNTRY_OUTPUT_TERMS = {
    "central arctic ocean/eurasian basin",
    "suburb of beijing",
}

SAMPLE_TYPE_MATERIAL_PATTERN = re.compile(
    r"\b("
    r"blood|feces|faeces|fecal|faecal|stool|urine|sputum|swab|tissue|milk|meat|gut|saliva|"
    r"biopsy|lavage|fluid|pus|abscess|wound|skin|nasal|rectal|oral|vaginal|manure|carcass|"
    r"cecal|caecal|intestine|intestinal|lung|liver|kidney|spleen|brain|placenta"
    r")\b",
    re.IGNORECASE,
)

ANATOMICAL_SITE_SYNONYMS = {
    "rectum": "rectum/perianal region",
    "rectal": "rectum/perianal region",
    "recto anal junction": "rectum/perianal region",
    "rectoanal junction": "rectum/perianal region",
    "recto-anal junction": "rectum/perianal region",
    "perirectal": "rectum/perianal region",
    "perianal": "rectum/perianal region",
    "anus": "rectum/perianal region",
    "nasal site": "nasal cavity/sinus/upper respiratory tract",
    "nasal cavity": "nasal cavity/sinus/upper respiratory tract",
    "nasal": "nasal cavity/sinus/upper respiratory tract",
    "nose": "nasal cavity/sinus/upper respiratory tract",
    "nares": "nasal cavity/sinus/upper respiratory tract",
    "sinus": "nasal cavity/sinus/upper respiratory tract",
    "paranasal sinus": "nasal cavity/sinus/upper respiratory tract",
    "ent sinus": "nasal cavity/sinus/upper respiratory tract",
    "nasopharynx": "nasopharynx/oropharynx",
    "nasopharyngeal": "nasopharynx/oropharynx",
    "oropharynx": "nasopharynx/oropharynx",
    "pharynx": "nasopharynx/oropharynx",
    "throat": "nasopharynx/oropharynx",
    "oral": "oral cavity",
    "oral cavity": "oral cavity",
    "mouth": "oral cavity",
    "dental plaque": "oral cavity",
    "tooth": "oral cavity",
    "skin": "skin/body surface",
    "skin body surface": "skin/body surface",
    "forehead": "skin/body surface",
    "foot": "skin/body surface",
    "leg": "skin/body surface",
    "chin": "skin/body surface",
    "palm": "skin/body surface",
    "axilla": "skin/body surface",
    "umbilicus": "skin/body surface",
    "sacrum": "skin/body surface",
    "popliteal fossa": "skin/body surface",
    "right popliteal fossa": "skin/body surface",
    "left popliteal fossa": "skin/body surface",
    "l index": "skin/body surface",
    "r index": "skin/body surface",
    "l palm": "skin/body surface",
    "r palm": "skin/body surface",
    "l cheek": "skin/body surface",
    "r cheek": "skin/body surface",
    "urogenital": "urogenital tract",
    "urogenital site": "urogenital tract",
    "genitourinary tract": "urogenital tract",
    "urinary tract": "urogenital tract",
    "urethra": "urogenital tract",
    "urethral": "urogenital tract",
    "urethra penis": "urogenital tract",
    "penis": "urogenital tract",
    "vagina": "vagina",
    "vaginal": "vagina",
    "vaginal site": "vagina",
    "cervix": "cervix",
    "ectocervical mucosa": "cervix",
    "uterus": "uterus",
    "colon": "gastrointestinal tract",
    "colon contents": "gastrointestinal tract",
    "ileum": "gastrointestinal tract",
    "caecum": "gastrointestinal tract",
    "cecum": "gastrointestinal tract",
    "caecal": "gastrointestinal tract",
    "cecal": "gastrointestinal tract",
    "intestine": "gastrointestinal tract",
    "intestinal tract": "gastrointestinal tract",
    "gastrointestinal tract": "gastrointestinal tract",
    "gut": "gastrointestinal tract",
    "gut content": "gastrointestinal tract",
    "stomach": "gastrointestinal tract",
    "rumen": "gastrointestinal tract",
    "cloaca": "cloaca",
    "cloacae": "cloaca",
    "bronch": "lower respiratory tract/bronch/pleural cavity",
    "bronchial": "lower respiratory tract/bronch/pleural cavity",
    "bronchial wash": "lower respiratory tract/bronch/pleural cavity",
    "bronchial lavage": "lower respiratory tract/bronch/pleural cavity",
    "bronchoalveolar lavage": "lower respiratory tract/bronch/pleural cavity",
    "bronchoalveolar lavage fluid": "lower respiratory tract/bronch/pleural cavity",
    "lung": "lower respiratory tract/bronch/pleural cavity",
    "trachea": "lower respiratory tract/bronch/pleural cavity",
    "pleural fluid": "lower respiratory tract/bronch/pleural cavity",
    "pleural effusion": "lower respiratory tract/bronch/pleural cavity",
    "pleural cavity": "lower respiratory tract/bronch/pleural cavity",
    "tonsil": "tonsil/oropharyngeal site",
    "breast": "breast",
    "mammary gland": "breast",
    "pancreas": "organ/tissue site",
    "liver": "organ/tissue site",
    "brain": "organ/tissue site",
    "kidney": "organ/tissue site",
    "spleen": "organ/tissue site",
    "bone": "organ/tissue site",
    "eye": "organ/tissue site",
    "ear": "organ/tissue site",
    "placenta": "organ/tissue site",
    "lymph node": "organ/tissue site",
    "heart": "organ/tissue site",
    "abdomen": "organ/tissue site",
}

SAMPLE_MATERIAL_EVIDENCE_PATTERN = re.compile(
    r"\b(?:"
    r"swab|fluid|effusion|lavage|wash|aspirate|tissue|biopsy|content|contents|feces|faeces|"
    r"fecal|faecal|stool|saliva|plaque|pus|urine|blood|sputum|milk|meat|carcass|culture|"
    r"cell line|cells?|clone|isolate"
    r")\b",
    re.IGNORECASE,
)

FOOD_CUT_CONTEXT_PATTERN = re.compile(
    r"\b(?:retail|abattoir|ground|minced|meat|tenderloin|fillet|fillets|strip|strips|sandwich|frozen|raw|"
    r"product|poultry|chicken|turkey|lamb|frog|drumstick|leg quarters?)\b",
    re.IGNORECASE,
)


def food_cut_sample_type(value: Any) -> str:
    cleaned = normalize_standardization_lookup(value)
    raw_text = "" if value is None else str(value).strip().lower()
    searchable = f"{cleaned} {raw_text}"
    if not cleaned or not FOOD_CUT_CONTEXT_PATTERN.search(searchable):
        return ""
    if re.search(r"\b(?:breast|leg|drumstick|tenderloin|fillet|fillets|strip|strips)\b", searchable):
        return "poultry meat" if re.search(r"\b(?:chicken|turkey|poultry|breast|drumstick)\b", searchable) else "meat"
    return ""


def canonical_anatomical_site(value: Any) -> str:
    if food_cut_sample_type(value):
        return ""
    for candidate in standardization_lookup_variants(value):
        if candidate in ANATOMICAL_SITE_SYNONYMS:
            return ANATOMICAL_SITE_SYNONYMS[candidate]
    cleaned = normalize_standardization_lookup(value)
    if not cleaned:
        return ""
    raw_text = "" if value is None else str(value).strip().lower()
    searchable = f"{cleaned} {raw_text}"
    patterns = (
        (r"\b(rectal|rectum|recto\s*anal|perirectal|perianal|anus)\b", "rectum/perianal region"),
        (r"\b(nasal|nose|nares|sinus|paranasal)\b", "nasal cavity/sinus/upper respiratory tract"),
        (r"\b(oral|mouth|dental|tooth|saliva)\b", "oral cavity"),
        (r"\b(skin|forehead|foot|leg|chin|palm|axilla|umbilicus|sacrum|popliteal|cheek|index|perineum|perineal|tarsal)\b", "skin/body surface"),
        (r"\b(urogenital|genitourinary|urinary|urethra|urethral|penis|vagina|vaginal|cervix|uterus)\b", "urogenital tract"),
        (r"\b(colon|ileum|caecum|cecum|intestinal|intestine|gastrointestinal|gut|stomach|rumen|cloaca)\b", "gastrointestinal tract"),
        (r"\b(bronch|bronchial|bronchoalveolar|pleural|lung|trachea)\b", "lower respiratory tract/bronch/pleural cavity"),
        (r"\b(tonsil)\b", "tonsil/oropharyngeal site"),
        (r"\b(breast|mammary)\b", "breast"),
        (r"\b(pancreas|liver|brain|kidney|spleen|bone|eye|ocular|conjunctiva|ear|placenta|lymph node|heart|abdomen)\b", "organ/tissue site"),
    )
    for pattern, label in patterns:
        if re.search(pattern, searchable):
            return label
    return ""


def sample_type_from_body_site_context(value: Any) -> str:
    cleaned = normalize_standardization_lookup(value)
    if not cleaned:
        return ""
    food_cut = food_cut_sample_type(cleaned)
    if food_cut:
        return food_cut
    if re.search(r"\b(rectal|rectum|perirectal|perianal)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "rectal swab"
    if re.search(r"\b(nasal|nose|nares)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "nasal swab"
    if re.search(r"\b(oral|mouth)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "oral swab"
    if re.search(r"\b(skin|body surface)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "skin swab"
    if re.search(r"\b(vaginal|vagina)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "vaginal swab"
    if re.search(r"\b(urogenital|urethral|cervix|cervical)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "urogenital swab"
    if re.search(r"\b(cloacal|cloaca)\b", cleaned) and re.search(r"\bswab\b", cleaned):
        return "cloacal swab"
    if re.search(r"\b(bronchoalveolar|bal|balf)\b", cleaned):
        return "bronchoalveolar lavage fluid"
    if re.search(r"\bbronchial\b", cleaned) and re.search(r"\b(wash|lavage)\b", cleaned):
        return "bronchial wash/lavage"
    if re.search(r"\bpleural\b", cleaned) and re.search(r"\b(fluid|effusion)\b", cleaned):
        return "pleural fluid"
    if re.search(r"\b(colon|intestinal|intestine|gut|cecal|caecal|cecum|caecum)\b", cleaned) and re.search(r"\b(content|contents)\b", cleaned):
        return "gut content"
    return ""


def sample_type_is_site_only(value: Any) -> bool:
    text = "" if value is None else str(value).strip()
    if not text:
        return False
    if not canonical_anatomical_site(text):
        return False
    return not SAMPLE_MATERIAL_EVIDENCE_PATTERN.search(text)


def source_context_for_anatomical_site(value: Any) -> str:
    cleaned = normalize_standardization_lookup(value)
    if not canonical_anatomical_site(value):
        return ""
    if re.search(r"\b(?:pleural|pus|abscess|fluid|effusion|aspirate|drainage)\b", cleaned):
        return "clinical fluid/material"
    if re.search(r"\b(?:bronch|bronchoalveolar|tracheal|respiratory|sputum)\b", cleaned):
        return "respiratory sample"
    return "clinical/host-associated material"


def source_context_for_disease_or_health(value: Any) -> str:
    standardized, method, _ = standardize_metadata_concept(value, HOST_DISEASE_SYNONYMS)
    if method != "original" and standardized:
        return "clinical/host-associated material"
    standardized, method, _ = standardize_metadata_concept(value, HOST_HEALTH_STATE_SYNONYMS)
    if method != "original" and standardized:
        return "clinical/host-associated material"
    cleaned = normalize_standardization_lookup(value)
    if re.search(r"\b(?:aborted?|abortion|fetus|fetal|septicemia|leukemia|myeloid)\b", cleaned):
        return "clinical/host-associated material"
    return ""


def source_context_for_lab_or_metadata_artifact(value: Any) -> tuple[str, str] | None:
    cleaned = normalize_standardization_lookup(value)
    if not cleaned:
        return None
    if cleaned in {"ref", "#ref"} or "#ref" in str(value).lower():
        return "", "metadata_error"
    if re.fullmatch(r"facility(?: [a-z0-9]+)?", cleaned) or cleaned in {"ot", "hcw hand"}:
        return "healthcare-associated environment", "facility_code_router"
    if cleaned in {"cxwnd", "roar", "cipa", "esba", "bk", "o"}:
        return "metadata descriptor / non-source", "metadata_code_router"
    if re.search(r"\b(?:derived from|parent strain|resistant derivatives|exposed to|ciprofloxacin|atcc)\b", cleaned):
        return "culture", "lab_artifact_router"
    if cleaned in {"isolated clone", "isolated organism"}:
        return "culture/isolate", "lab_artifact_router"
    if re.search(r"\b(?:dh5a|xl10 gold|xl1 mfr blue|mc1061|electroten blue|synthetic construct)\b", cleaned):
        return "culture", "lab_artifact_router"
    return None


def sample_type_rule_is_host_only(synonym: Any, proposed_value: Any) -> bool:
    proposed = normalize_standardization_lookup(proposed_value)
    source = normalize_standardization_lookup(synonym)
    if proposed not in HOST_ONLY_SAMPLE_TYPE_TERMS:
        return False
    return not SAMPLE_TYPE_MATERIAL_PATTERN.search(source)


def sanitize_sample_type_standardization(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if sample_type_rule_is_host_only(text, text) or sample_type_is_site_only(text) else text


def enforce_clean_sample_type_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if "Sample_Type_SD" not in frame.columns:
        return frame
    cleaned = frame["Sample_Type_SD"].apply(sanitize_sample_type_standardization)
    invalid_mask = frame["Sample_Type_SD"].fillna("").astype(str).str.strip().ne("") & cleaned.eq("")
    frame["Sample_Type_SD"] = cleaned
    for column in ["Sample_Type_SD_Broad", "Sample_Type_SD_Detail", "Sample_Type_Ontology_ID"]:
        if column in frame.columns:
            frame.loc[invalid_mask, column] = ""
    if "Sample_Type_SD_Method" in frame.columns:
        frame.loc[invalid_mask, "Sample_Type_SD_Method"] = "missing"
    return frame


def normalize_standardization_lookup(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[_;/,|:+-]+", " ", text)
    text = re.sub(r"[^a-z0-9. ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return STANDARDIZATION_SPELLING_CORRECTIONS.get(text, text)


def standardization_lookup_variants(value: Any) -> list[str]:
    cleaned = normalize_standardization_lookup(value)
    variants: list[str] = []
    for candidate in [cleaned, cleaned.replace(".", ""), cleaned.replace(" ", "")]:
        candidate = STANDARDIZATION_SPELLING_CORRECTIONS.get(candidate, candidate)
        if candidate and candidate not in variants:
            variants.append(candidate)
        if candidate.endswith("s") and len(candidate) > 4:
            singular = candidate[:-1]
            if singular not in variants:
                variants.append(singular)
    return variants


def load_standardization_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {str(key): str(value).strip() for key, value in row.items() if key is not None}
            for row in csv.DictReader(handle)
        ]


def apply_core_standardization_overrides() -> None:
    SAMPLE_TYPE_SYNONYMS.update(
        {
            "stool swab": "fecal/stool swab",
            "fecal swab": "fecal/stool swab",
            "faecal swab": "fecal/stool swab",
            "rectal swab": "rectal swab",
            "rectum swab": "rectal swab",
            "swab rectum": "rectal swab",
            "stool rectal swab": "rectal swab",
            "rectal swab stool": "rectal swab",
            "anal swab": "perianal/anal swab",
            "perianal swab": "perianal/anal swab",
            "perirectal swab": "perianal/anal swab",
            "urogenital swab": "urogenital swab",
            "bronchial wash": "bronchial wash/lavage",
            "bronchial lavage": "bronchial wash/lavage",
            "bronchoalveolar lavage": "bronchoalveolar lavage fluid",
            "bronchoalveolar lavage fluid": "bronchoalveolar lavage fluid",
            "balf": "bronchoalveolar lavage fluid",
            "pleural fluid": "pleural fluid",
            "pleural effusion": "pleural fluid",
            "intestinal content": "gut content",
            "intestinal contents": "gut content",
            "gut content": "gut content",
            "gut contents": "gut content",
            "colon contents": "gut content",
            "metagenomic assembly": "metagenomic assembly",
            "metagenome assembly": "metagenomic assembly",
            "food processing environment": "food processing environment",
            "food production environment": "food processing environment",
            "food contact surface": "food-contact surface",
            "non food contact surface": "non-food-contact surface",
            "raw intact chicken": "chicken meat",
            "comminuted chicken": "chicken meat",
            "nonintact chicken": "chicken meat",
            "chicken breast": "chicken meat",
            "retail breast": "poultry meat",
            "abattoir breast": "poultry meat",
            "breast tenderloins": "poultry meat",
            "breast fillets": "poultry meat",
            "breast strips": "poultry meat",
            "skinless breast": "poultry meat",
            "turkey breast": "poultry meat",
            "turkey breast sandwich": "poultry meat",
            "poultry breast": "poultry meat",
            "chicken breast": "poultry meat",
            "lamb leg": "meat",
            "leg quarters": "poultry meat",
            "frog leg": "meat",
            "frozen frog leg": "meat",
            "drumstick": "poultry meat",
            "young chicken carcass rinse": "chicken meat",
            "ground turkey": "turkey meat",
            "comminuted turkey": "turkey meat",
            "raw ground pork": "pork",
            "raw ground beef": "beef",
            "comminuted beef": "beef",
            "chicken meat": "chicken meat",
            "turkey meat": "turkey meat",
            "poultry meat": "poultry meat",
            "poultry": "host-associated context",
            "pork": "pork",
            "beef": "beef",
            "fish product": "fish product",
            "raw fish product": "fish product",
            "freshwater fish product": "freshwater fish product",
            "shrimp product": "shrimp product",
            "prawn product": "shrimp product",
            "oyster product": "oyster product",
            "shucked oyster": "oyster product",
            "shellfish product": "shellfish product",
            "seafood": "seafood",
            "cheese": "dairy food",
            "cheese rind": "dairy food",
            "yogurt": "dairy food",
            "yoghurt": "dairy food",
            "biological product": "biological product",
            "isolate": "microbial isolate",
            "bacterial isolate": "microbial isolate",
            "microbial isolate": "microbial isolate",
            "single cell": "single cell",
            "cultured colonies": "culture",
            "cultured colony": "culture",
            "cultured microbe": "microbial culture",
            "cultured bacterium": "bacterial culture",
            "microorganism": "microbial isolate",
            "microbe": "microbial isolate",
            "bacterial": "microbial isolate",
            "dna": "DNA extract",
            "genomic dna": "DNA extract",
            "genomic assembly": "metagenomic assembly",
            "sample": "sample",
            "ffpe": "FFPE tissue",
            "sterile site": "sterile body site",
            "normally sterile site": "sterile body site",
            "clinical": "clinical sample",
            "clinical sample": "clinical sample",
            "clinical samples": "clinical sample",
            "clinical specimen": "clinical sample",
            "clinical material": "clinical material",
            "homo sapiens clinical": "clinical sample",
            "hospital": "healthcare facility",
            "intensive care unit": "healthcare facility",
            "nursing home": "long-term care facility",
            "icu": "healthcare facility",
            "hospital icu": "healthcare facility",
            "sewage": "sewage",
            "hospital sewage": "hospital sewage",
            "activated sludge": "sewage",
            "respiratory": "respiratory sample",
            "respiratory sample": "respiratory sample",
            "respiratory samples": "respiratory sample",
            "respiratory tract": "respiratory sample",
            "lower respiratory tract": "respiratory sample",
            "upper respiratory tract": "respiratory sample",
            "tracheal aspirate": "tracheal aspirate/secretion",
            "tracheal secretion": "tracheal aspirate/secretion",
            "bal": "bronchoalveolar lavage fluid",
            "bronchoalveolar lavage": "bronchoalveolar lavage fluid",
            "bronchoalveolar lavage fluid": "bronchoalveolar lavage fluid",
            "sputum": "sputum",
            "csf": "cerebrospinal fluid",
            "cerebrospinal fluid": "cerebrospinal fluid",
            "np": "nasopharynx/oropharynx",
            "np swab": "nasopharyngeal swab",
            "nasopharynx": "nasopharynx/oropharynx",
            "nasopharyngeal": "nasopharynx/oropharynx",
            "nasophaynx": "nasopharynx/oropharynx",
            "pharynx": "nasopharynx/oropharynx",
            "throat": "nasopharynx/oropharynx",
            "pharyngeal exudate": "nasopharynx/oropharynx",
            "nasopharynx oropharynx": "nasopharynx/oropharynx",
            "nasopharynx/oropharynx": "nasopharynx/oropharynx",
            "nose": "nasal site",
            "nasal": "nasal site",
            "nares": "nasal site",
            "nare": "nasal site",
            "nasal cavity": "nasal site",
            "oral": "oral cavity",
            "oral cavity": "oral cavity",
            "oral metagenome": "oral cavity",
            "dental plaque": "dental plaque",
            "mouth": "oral cavity",
            "urethra": "urethra/penis",
            "urethral": "urethra/penis",
            "penis urethra": "urethra/penis",
            "urogenital": "urogenital",
            "urogenital site": "urogenital",
            "cervix": "cervix",
            "ectocervical mucosa": "ectocervical mucosa",
            "uterus": "uterus",
            "pus": "pus",
            "abscess": "abscess",
            "liver abscess": "liver abscess",
            "bodily fluid": "bodily fluid",
            "fluid": "bodily fluid",
            "pleural fluid": "pleural fluid",
            "pleural effusion": "pleural fluid",
            "drainage": "drainage",
            "aspirate": "aspirate",
            "liver": "liver",
            "brain": "brain",
            "kidney": "kidney",
            "spleen": "spleen",
            "bone": "bone",
            "eye": "eye",
            "ear": "ear",
            "placenta": "placenta",
            "lymph node": "lymph node",
            "skin": "skin",
            "forehead": "skin",
            "foot": "skin",
            "leg": "skin",
            "chin": "skin",
            "palm": "skin",
            "axilla": "skin",
            "umbilicus": "skin",
            "sacrum": "skin",
            "right popliteal fossa": "skin",
            "left popliteal fossa": "skin",
            "l index": "skin",
            "r index": "skin",
            "l palm": "skin",
            "r palm": "skin",
            "l cheek": "skin",
            "r cheek": "skin",
            "wound": "wound",
            "cecal": "gut content",
            "caecal": "gut content",
            "cecum": "gut content",
            "caecum": "gut content",
            "cecal content": "gut content",
            "caecal content": "gut content",
            "intestine": "gut content",
            "colon": "gut content",
            "ileum": "gut content",
            "cloaca": "cloacal sample",
            "cloacae": "cloacal sample",
            "intestinal tract": "gut content",
            "gastrointestinal tract": "gut content",
            "stomach": "gut content",
            "gastric biopsy": "gastric biopsy",
            "rumen": "gut content",
            "root": "root",
            "rhizosphere": "rhizosphere",
            "leaves": "leaves",
            "plant": "plant",
            "cucumber": "cucumber",
            "potato": "potato",
            "nodule": "nodule",
            "kratom": "kratom",
            "groundwater": "groundwater",
            "river": "river",
            "canal": "canal",
            "irrigation canal": "irrigation canal",
            "stream": "stream",
            "pond": "pond",
            "estuary": "estuary",
            "hot spring": "hot spring",
            "surface layer": "surface layer",
            "deep sea hydrothermal deposit": "deep-sea hydrothermal deposit",
            "produced fluids from hydraulically fractured shales": "produced fluids from hydraulically fractured shales",
            "ice core section from central arctic ocean": "ice core section from central arctic ocean",
            "core": "core",
            "bacteria": "metadata descriptor/non-source",
            "assembly": "metadata descriptor/non-source",
            "microbial community": "metadata descriptor/non-source",
            "host associated strain": "metadata descriptor/non-source",
            "invasive": "metadata descriptor/non-source",
            "screening": "metadata descriptor/non-source",
            "surveillance": "metadata descriptor/non-source",
            "pathogen.cl": "metadata descriptor/non-source",
            "other": "metadata descriptor/non-source",
            "uti": "urinary tract",
            "urinary": "urinary tract",
            "urinary tract": "urinary tract",
            "rectal": "rectum",
            "rectum": "rectum",
            "recto vaginal": "rectovaginal site",
            "recto-vaginal": "rectovaginal site",
            "groin": "groin",
            "bile": "bile",
            "biopsy": "biopsy",
            "bloodstream": "bloodstream",
            "bloodstream isolates": "bloodstream",
            "gill": "gill",
            "trachea": "trachea",
            "secretion": "secretion",
            "sink": "sink",
            "drain": "drain",
            "laboratory": "laboratory",
            "farm": "farm",
            "dairy farm": "dairy farm",
            "deciduous forest": "deciduous forest",
            "rte product": "ready-to-eat product",
            "ice cream": "ice cream",
            "prawns": "prawn product",
            "prawn": "prawn product",
            "ground component chicken": "ground chicken",
            "ground chicken": "ground chicken",
            "carcass": "chicken carcass",
            "spinach": "spinach",
            "papaya": "papaya",
            "vegetable": "vegetable",
            "leafy greens": "leafy greens",
            "tree nut": "tree nut",
            "peanut butter": "peanut butter",
            "tooth": "tooth",
            "catheter": "catheter",
            "sludge": "sludge",
            "metagenome": "metagenome",
            "wildlife": "wildlife",
            "wood": "wood",
            "colon contents": "colon contents",
            "vagina": "vagina",
            "long term care facility": "long-term care facility",
            "anaerobic bioreactor effluent": "anaerobic bioreactor effluent",
            "hospital wastewater": "hospital wastewater",
            "salmon": "salmon",
            "surface": "surface",
            "heart": "heart",
            "ascites": "ascites",
            "broiler": "broiler",
            "enviromental": "environmental sample",
            "environmental": "environmental sample",
            "endovascular": "endovascular",
            "sponge": "sponge",
            "rectovaginal site": "rectovaginal site",
            "ready-to-eat product": "ready-to-eat product",
            "urinary tract": "urinary tract",
            "bloodstream": "bloodstream",
            "environmental sample": "environmental sample",
            "long-term care facility": "long-term care facility",
            "prawn product": "prawn product",
            "ground chicken": "ground chicken",
            "chicken carcass": "chicken carcass",
            "healthy people": "",
            "non hospitalized person": "",
            "hospital patients": "",
            "infants": "",
            "homo": "",
            "homo sapiens": "",
            "companion animal": "",
            "chickens": "",
            "sheep": "",
            "whole stomoxys flies": "",
            "blood from patients of rural regional hospital": "blood",
            "wt mouse intestinal epithelial cells": "intestinal epithelial cells",
            "intestinal epithelial cells": "intestinal epithelial cells",
            "mammary gland": "mammary gland",
            "right cubital fossa": "cubital fossa",
            "cubital fossa": "cubital fossa",
            "abdomen": "abdomen",
            "genitourinary tract": "genitourinary tract",
            "balf": "bronchoalveolar lavage fluid",
            "esputum": "sputum",
            "rectal screening": "rectum",
            "nasal surveillance": "nasal site",
            "cleanroom floor": "cleanroom floor",
            "air": "air",
            "biofilm from cave": "cave biofilm",
            "biofilm from sulfidic cave": "cave biofilm",
            "cold seep": "cold seep",
            "glacier": "glacier",
            "terrestrial": "terrestrial environment",
            "anthropogenic terrestrial biome": "terrestrial environment",
            "kimchi": "kimchi",
            "enoki mushroom": "enoki mushroom",
            "tomato": "tomato",
            "avocado": "avocado",
            "cantaloupe": "cantaloupe",
            "lettuce": "lettuce",
            "dairy product": "dairy product",
            "catfish product": "catfish product",
            "product raw intact siluriformes ictaluridae": "catfish product",
            "product raw intact siluriformes ictaluridae catfish": "catfish product",
            "anaerobic digestion of organic wastes under variable temperature conditions and feedstocks": "anaerobic digester",
            "anaerobic digester": "anaerobic digester",
            "cilantro": "cilantro",
            "cave biofilm": "cave biofilm",
        }
    )
    SAMPLE_TYPE_SYNONYMS.pop("fish", None)
    for facility_key in (
        "hospital",
        "intensive care unit",
        "nursing home",
        "icu",
        "hospital icu",
        "healthcare facility",
    ):
        SAMPLE_TYPE_SYNONYMS.pop(facility_key, None)
    for host_only_key in HOST_ONLY_SAMPLE_TYPE_TERMS:
        SAMPLE_TYPE_SYNONYMS.pop(host_only_key, None)
    SAMPLE_TYPE_SYNONYMS.update(
        {
            "wound swab": "wound swab",
            "swab wound": "wound swab",
            "swab_wound": "wound swab",
        }
    )
    ENVIRONMENT_MEDIUM_SYNONYMS.update(
        {
            "river water": "river water",
            "lake water": "lake water",
            "pond water": "pond water",
            "hospital wastewater": "hospital wastewater",
            "hospital waste water": "hospital wastewater",
            "domestic wastewater": "domestic wastewater",
            "domestic waste water": "domestic wastewater",
            "estuarine water": "estuarine water",
            "marine water": "seawater",
            "hot spring water": "hot spring water",
            "irrigation water": "irrigation water",
            "cheese": "dairy food",
            "cheese rind": "dairy food",
            "yogurt": "dairy food",
            "yoghurt": "dairy food",
            "biological product": "biological product",
            "ant built patch material": "soil",
            "buffered agar": "culture medium",
            "agar": "culture medium",
            "leaf": "plant tissue",
        }
    )
    # Keep generic food labels generic. Specific meat/product classes are handled by
    # more specific keys such as "pork", "chicken meat", and "raw ground beef".
    ISOLATION_SOURCE_SYNONYMS.update(
        {
            "food": "food/food product",
            "foods": "food/food product",
            "food product": "food/food product",
        }
    )
    HOST_BROAD_SYNONYMS.update(
        {
            "bird": ("Aves", "8782"),
            "birds": ("Aves", "8782"),
            "wild bird": ("Aves", "8782"),
            "wild birds": ("Aves", "8782"),
            "avian": ("Aves", "8782"),
            "mammal": ("Mammalia", "40674"),
            "mammals": ("Mammalia", "40674"),
            "rodent": ("Rodentia", "9989"),
            "rodents": ("Rodentia", "9989"),
        }
    )


def load_external_standardization_rules() -> None:
    for row in load_standardization_csv(STANDARDIZATION_DIR / "host_synonyms.csv"):
        synonym = normalize_standardization_lookup(row.get("synonym"))
        canonical = (row.get("canonical") or "").strip()
        taxid = (row.get("taxid") or "").strip()
        confidence = (row.get("confidence") or "high").strip().lower()
        if not synonym or not canonical or not taxid:
            continue
        if confidence == "medium":
            HOST_BROAD_SYNONYMS[synonym] = (canonical, taxid)
        else:
            HOST_SYNONYMS[synonym] = (canonical, taxid)

    for row in load_standardization_csv(STANDARDIZATION_DIR / "host_negative_rules.csv"):
        synonym = normalize_standardization_lookup(row.get("synonym"))
        decision = normalize_standardization_lookup(row.get("decision"))
        if not synonym:
            continue
        if decision in {"missing", "absent"}:
            STANDARDIZATION_MISSING_TOKENS.add(synonym)
        elif decision in {"not_identifiable", "not identifiable"}:
            HOST_NOT_IDENTIFIABLE_TOKENS.add(synonym)
        elif decision in {"non_host_source", "non host source", "source"}:
            NON_HOST_SOURCE_HINTS.add(synonym)

    for row in load_standardization_csv(STANDARDIZATION_DIR / "controlled_categories.csv"):
        synonym = normalize_standardization_lookup(row.get("synonym") or row.get("original_value") or row.get("normalized_value"))
        category = (row.get("category") or row.get("proposed_value") or "").strip()
        destination = (row.get("destination") or "").strip()
        ontology_id = (row.get("ontology_id") or "").strip()
        status = normalize_standardization_lookup(row.get("status") or "approved")
        if status and status not in {"approved", "active"}:
            continue
        if not synonym or not category:
            continue
        if ontology_id:
            CONTROLLED_CATEGORY_ONTOLOGY_IDS[category] = ontology_id
        if destination == "Environment_Medium_SD":
            ENVIRONMENT_MEDIUM_SYNONYMS[synonym] = category
        elif destination == "Environment_Broad_Scale_SD":
            ENVIRONMENT_BROAD_SYNONYMS[synonym] = category
        elif destination == "Environment_Local_Scale_SD":
            ENVIRONMENT_LOCAL_SYNONYMS[synonym] = category
        elif destination == "Isolation_Site_SD":
            ISOLATION_SITE_SYNONYMS[synonym] = category
        elif destination == "Host_Disease_SD":
            HOST_DISEASE_SYNONYMS[synonym] = category
        elif destination == "Host_Health_State_SD":
            HOST_HEALTH_STATE_SYNONYMS[synonym] = category
        elif destination == "Sample_Type_SD":
            if sample_type_rule_is_host_only(synonym, category):
                continue
            SAMPLE_TYPE_SYNONYMS[synonym] = category
        elif destination == "Isolation_Source_SD":
            ISOLATION_SOURCE_SYNONYMS[synonym] = category
    apply_core_standardization_overrides()


load_external_standardization_rules()


def apply_approved_standardization_rule_to_memory(rule: Mapping[str, Any]) -> None:
    destination = str(rule.get("destination") or "").strip()
    proposed_value = str(rule.get("proposed_value") or "").strip()
    ontology_id = str(rule.get("ontology_id") or "").strip()
    method = str(rule.get("method") or "").strip().lower()
    confidence = str(rule.get("confidence") or "").strip().lower()
    normalized_value = normalize_standardization_lookup(rule.get("normalized_value") or rule.get("original_value"))
    if not normalized_value or not destination:
        return

    if destination == "Host_SD":
        if not proposed_value:
            if method == "missing":
                STANDARDIZATION_MISSING_TOKENS.add(normalized_value)
            elif method == "non_host_source":
                NON_HOST_SOURCE_HINTS.add(normalized_value)
            elif method == "not_identifiable":
                HOST_NOT_IDENTIFIABLE_TOKENS.add(normalized_value)
            clear_standardization_runtime_caches()
            return
        if not ontology_id:
            return
        target = HOST_SYNONYMS if confidence == "high" else HOST_BROAD_SYNONYMS
        target[normalized_value] = (proposed_value, ontology_id)
        clear_standardization_runtime_caches()
        return

    if ontology_id:
        CONTROLLED_CATEGORY_ONTOLOGY_IDS[proposed_value] = ontology_id
    if destination == "Environment_Medium_SD":
        ENVIRONMENT_MEDIUM_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Environment_Broad_Scale_SD":
        ENVIRONMENT_BROAD_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Environment_Local_Scale_SD":
        ENVIRONMENT_LOCAL_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Isolation_Site_SD":
        ISOLATION_SITE_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Host_Disease_SD":
        HOST_DISEASE_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Host_Health_State_SD":
        HOST_HEALTH_STATE_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Sample_Type_SD":
        if sample_type_rule_is_host_only(normalized_value, proposed_value):
            clear_standardization_runtime_caches()
            return
        SAMPLE_TYPE_SYNONYMS[normalized_value] = proposed_value
    elif destination == "Isolation_Source_SD":
        ISOLATION_SOURCE_SYNONYMS[normalized_value] = proposed_value
    clear_standardization_runtime_caches()


def load_approved_standardization_rules_into_memory(db: sqlite3.Connection) -> None:
    try:
        rows = db.execute(
            """
            SELECT *
            FROM standardization_rules
            WHERE status = 'approved'
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for row in rows:
        apply_approved_standardization_rule_to_memory(dict(row))
    apply_core_standardization_overrides()


def clean_host_lookup_text(value: Any) -> str:
    return normalize_standardization_lookup(value)


HOST_COMMON_NAME_BY_TAXID = {
    "9606": "human",
    "10090": "mouse",
    "10116": "rat",
    "9913": "cattle",
    "9823": "pig",
    "9825": "domestic pig",
    "9031": "chicken",
    "9103": "turkey",
    "8843": "duck",
    "9615": "dog",
    "9685": "cat",
    "9796": "horse",
    "9925": "goat",
    "9940": "sheep",
    "8782": "bird",
    "40674": "mammal",
    "9989": "rodent",
}

HOST_LINEAGE_CACHE: dict[str, dict[str, str]] | None = None


HOST_CONTEXT_PATTERNS = [
    (re.compile(r"\b(pet|pets|companion pet|companion animal)\b", re.IGNORECASE), "Host_Context_SD", "pet/companion animal"),
    (re.compile(r"\b(infant|neonate|newborn)\b", re.IGNORECASE), "Host_Age_Group_SD", "infant"),
    (re.compile(r"\b(child|children|pediatric|paediatric)\b", re.IGNORECASE), "Host_Age_Group_SD", "child"),
    (re.compile(r"\b(adult)\b", re.IGNORECASE), "Host_Age_Group_SD", "adult"),
    (re.compile(r"\b(young)\b", re.IGNORECASE), "Host_Age_Group_SD", "young"),
    (re.compile(r"\b(market swine)\b", re.IGNORECASE), "Host_Production_Context_SD", "market swine"),
    (re.compile(r"\b(steer)\b", re.IGNORECASE), "Host_Production_Context_SD", "steer"),
    (re.compile(r"\b(heifer)\b", re.IGNORECASE), "Host_Production_Context_SD", "heifer"),
    (re.compile(r"\b(sow)\b", re.IGNORECASE), "Host_Production_Context_SD", "sow"),
    (re.compile(r"\b(cecal|caecal|cecum|caecum)\b", re.IGNORECASE), "Host_Anatomical_Site_SD", "cecal/gut content"),
    (re.compile(r"\b(feces|faeces|fecal|faecal|stool)\b", re.IGNORECASE), "Host_Anatomical_Site_SD", "feces/stool"),
]


def extract_host_context_fields(value: Any) -> dict[str, str]:
    text = "" if value is None else str(value)
    context = {
        "Host_Context_SD": "",
        "Host_Age_Group_SD": "",
        "Host_Production_Context_SD": "",
        "Host_Anatomical_Site_SD": "",
    }
    microbial_context = microbial_self_descriptor_context(text)
    if microbial_context is not None:
        context["Host_Context_SD"] = microbial_context[0]
    for pattern, field, label in HOST_CONTEXT_PATTERNS:
        if pattern.search(text) and not context[field]:
            context[field] = label
    return context


def host_context_sample_type(host_standardization: Mapping[str, str], host_value: Any) -> tuple[str, str, str]:
    context_label = str(host_standardization.get("Host_Context_SD") or "")
    microbial_context = microbial_self_descriptor_context(host_value)
    if microbial_context is not None:
        return microbial_context[1], "host_context", ""
    if context_label in {E_COLI_CONTEXT_LABEL, LAB_MICROBIAL_CONTEXT_LABEL, "microbial self/lab culture descriptor"}:
        return "pure/single culture", "host_context", ""
    return "", "missing", ""


def empty_host_lineage() -> dict[str, str]:
    return {
        "Host_Rank": "",
        "Host_Superkingdom": "",
        "Host_Phylum": "",
        "Host_Class": "",
        "Host_Order": "",
        "Host_Family": "",
        "Host_Genus": "",
        "Host_Species": "",
        "Host_Common_Name": "",
    }


def parse_taxonkit_lineage_output(lineage_output: str, reformat_output: str) -> dict[str, dict[str, str]]:
    ranks: dict[str, tuple[str, str]] = {}
    for line in lineage_output.splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            ranks[parts[0].strip()] = (parts[1].strip(), parts[2].strip())

    parsed: dict[str, dict[str, str]] = {}
    for line in reformat_output.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        taxid = parts[0].strip()
        if not taxid:
            continue
        lineage_text, rank = ranks.get(taxid, ("", ""))
        superkingdom = ""
        lineage_set = set(lineage_text.split(";"))
        for candidate in ("Eukaryota", "Bacteria", "Archaea", "Viruses"):
            if candidate in lineage_set:
                superkingdom = candidate
                break
        lineage = empty_host_lineage()
        lineage.update(
            {
                "Host_Rank": rank,
                "Host_Superkingdom": superkingdom,
                "Host_Phylum": parts[4].strip() if len(parts) > 4 else "",
                "Host_Class": parts[5].strip() if len(parts) > 5 else "",
                "Host_Order": parts[6].strip() if len(parts) > 6 else "",
                "Host_Family": parts[7].strip() if len(parts) > 7 else "",
                "Host_Genus": parts[8].strip() if len(parts) > 8 else "",
                "Host_Species": parts[9].strip() if len(parts) > 9 else "",
                "Host_Common_Name": HOST_COMMON_NAME_BY_TAXID.get(taxid, ""),
            }
        )
        parsed[taxid] = lineage
    return parsed


def run_taxonkit_lineage_batch(taxids: list[str]) -> dict[str, dict[str, str]]:
    unique_taxids = sorted({str(taxid).strip() for taxid in taxids if str(taxid).strip().isdigit()})
    if not unique_taxids:
        return {}
    try:
        lineage_result = subprocess.run(
            ["taxonkit", "lineage", "-r"],
            input="\n".join(unique_taxids) + "\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
        reformat_result = subprocess.run(
            ["taxonkit", "reformat", "-f", "{k}\t{p}\t{c}\t{o}\t{f}\t{g}\t{s}"],
            input=lineage_result.stdout,
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if lineage_result.returncode != 0 or reformat_result.returncode != 0:
        return {}
    return parse_taxonkit_lineage_output(lineage_result.stdout, reformat_result.stdout)


def host_lineage_cache() -> dict[str, dict[str, str]]:
    global HOST_LINEAGE_CACHE
    if HOST_LINEAGE_CACHE is None:
        HOST_LINEAGE_CACHE = run_taxonkit_lineage_batch(
            [taxid for _, taxid in list(HOST_SYNONYMS.values()) + list(HOST_BROAD_SYNONYMS.values())]
        )
    return HOST_LINEAGE_CACHE


@lru_cache(maxsize=20_000)
def taxonkit_host_lineage(taxid: str) -> dict[str, str]:
    taxid = str(taxid or "").strip()
    lineage = empty_host_lineage()
    if not taxid.isdigit():
        return lineage
    cached = host_lineage_cache().get(taxid)
    if cached is not None:
        return dict(cached)
    return run_taxonkit_lineage_batch([taxid]).get(taxid, lineage)


def enrich_host_standardization(value: Any, host: Mapping[str, str]) -> dict[str, str]:
    enriched = {column: "" for column in HOST_STANDARDIZATION_COLUMNS}
    original = "" if value is None else str(value).strip()
    cleaned = clean_host_lookup_text(original)
    enriched.update(
        {
            "Host_Original": original,
            "Host_Cleaned": cleaned,
            "Host_SD": str(host.get("Host_SD") or ""),
            "Host_TaxID": str(host.get("Host_TaxID") or ""),
            "Host_SD_Method": str(host.get("Host_SD_Method") or ""),
            "Host_SD_Confidence": str(host.get("Host_SD_Confidence") or ""),
            "Host_Match_Method": str(host.get("Host_SD_Method") or ""),
            "Host_Confidence": str(host.get("Host_SD_Confidence") or ""),
            "Host_Review_Status": (
                "accepted"
                if host.get("Host_TaxID")
                else (
                    "missing"
                    if str(host.get("Host_SD_Method") or "") == "missing"
                    else (
                        str(host.get("Host_SD_Method") or "")
                        if str(host.get("Host_SD_Method") or "") in {"non_host_source", "not_identifiable"}
                        else "review_needed"
                    )
                )
            ),
        }
    )
    enriched.update(extract_host_context_fields(original))
    if enriched["Host_TaxID"]:
        enriched.update(taxonkit_host_lineage(enriched["Host_TaxID"]))
    if not enriched["Host_Common_Name"] and cleaned and enriched["Host_SD"] and cleaned != clean_host_lookup_text(enriched["Host_SD"]):
        enriched["Host_Common_Name"] = cleaned
    return enriched


def standardize_host_metadata(value: Any) -> dict[str, str]:
    original = "" if value is None else str(value).strip()
    if metadata_value_is_missing(original):
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "missing",
            "Host_SD_Confidence": "none",
        }
    cleaned = clean_host_lookup_text(original)
    if microbial_self_descriptor_context(cleaned) is not None:
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    if SEAFOOD_SOURCE_CONTEXT_PATTERN.search(cleaned):
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    if cleaned in HOST_NOT_IDENTIFIABLE_TOKENS:
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "not_identifiable",
            "Host_SD_Confidence": "none",
        }
    if cleaned in HOST_TAXONOMY_PRIORITY_TOKENS:
        for candidate in standardization_lookup_variants(original):
            if candidate in HOST_SYNONYMS:
                name, taxid = HOST_SYNONYMS[candidate]
                return {
                    "Host_SD": name,
                    "Host_TaxID": taxid,
                    "Host_SD_Method": "dictionary",
                    "Host_SD_Confidence": "high",
                }
            if candidate in HOST_BROAD_SYNONYMS:
                name, taxid = HOST_BROAD_SYNONYMS[candidate]
                return {
                    "Host_SD": name,
                    "Host_TaxID": taxid,
                    "Host_SD_Method": "broad_dictionary",
                    "Host_SD_Confidence": "medium",
                }
    if cleaned in NON_HOST_SOURCE_HINTS:
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    if context_host_recovery_blocked("Host", original):
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    compact = cleaned.replace(".", "")
    for candidate in standardization_lookup_variants(original):
        if candidate in HOST_SYNONYMS:
            name, taxid = HOST_SYNONYMS[candidate]
            return {
                "Host_SD": name,
                "Host_TaxID": taxid,
                "Host_SD_Method": "dictionary",
                "Host_SD_Confidence": "high",
            }
    for key, (name, taxid) in HOST_SUBSTRING_SYNONYMS.items():
        pattern = rf"(^|\s){re.escape(key.replace('.', ''))}(\s|$)"
        if re.search(pattern, compact):
            return {
                "Host_SD": name,
                "Host_TaxID": taxid,
                "Host_SD_Method": "cleaned_match",
                "Host_SD_Confidence": "high",
            }
    for key, (name, taxid) in HOST_CONTEXT_SYNONYMS.items():
        if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
            return {
                "Host_SD": name,
                "Host_TaxID": taxid,
                "Host_SD_Method": "context_dictionary",
                "Host_SD_Confidence": "medium",
            }
    for key, (name, taxid) in HOST_BROAD_SYNONYMS.items():
        if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
            return {
                "Host_SD": name,
                "Host_TaxID": taxid,
                "Host_SD_Method": "broad_dictionary",
                "Host_SD_Confidence": "medium",
            }
    source_category = source_standardization_synonyms().get(cleaned)
    if source_category:
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    if NON_HOST_SOURCE_PATTERN.search(cleaned):
        return {
            "Host_SD": "",
            "Host_TaxID": "",
            "Host_SD_Method": "non_host_source",
            "Host_SD_Confidence": "none",
        }
    return {
        "Host_SD": original,
        "Host_TaxID": "",
        "Host_SD_Method": "unmapped",
        "Host_SD_Confidence": "none",
    }


def standardize_host_from_metadata_context(row: Mapping[str, Any]) -> dict[str, str] | None:
    values = [
        ("Isolation Source", row.get("Isolation Source")),
        ("Isolation Site", row.get("Isolation Site")),
        ("Sample Type", row.get("Sample Type")),
        ("Environment Medium", row.get("Environment Medium")),
    ]
    for field_name, value in values:
        if metadata_value_is_missing(value):
            continue
        cleaned = clean_host_lookup_text(value)
        if context_host_recovery_blocked(field_name, value):
            continue
        if field_name == "Isolation Source" and cleaned in HOST_CONTEXT_SOURCE_SKIP_TERMS:
            continue
        if cleaned == "poultry":
            continue
        if re.search(r"\b(product|meat|carcass|food|seafood|shellfish|shrimp|prawn|oyster)\b", cleaned):
            continue
        host = standardize_host_metadata(cleaned)
        if host.get("Host_TaxID"):
            method = str(host.get("Host_SD_Method") or "")
            if not method.startswith("context_"):
                host["Host_SD_Method"] = f"context_{method}"
            host["_Host_Source_Value"] = str(value).strip()
            return host
        for key, (name, taxid) in {**HOST_CONTEXT_SYNONYMS, **HOST_BROAD_SYNONYMS}.items():
            if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
                confidence = "medium" if key in HOST_CONTEXT_SYNONYMS else "low"
                return {
                    "Host_SD": name,
                    "Host_TaxID": taxid,
                    "Host_SD_Method": "context_dictionary",
                    "Host_SD_Confidence": confidence,
                    "_Host_Source_Value": str(value).strip(),
                }
    return None


def metadata_value_is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return True
    lowered = text.lower()
    normalized = normalize_standardization_lookup(lowered)
    if lowered in MISSING_VALUE_TOKENS or normalized in STANDARDIZATION_MISSING_TOKENS:
        return True
    missing_prefixes = (
        "missing",
        "no collected",
        "not collect",
        "not applicable",
        "not available",
        "not collected",
        "not provided",
        "not recorded",
        "not determined",
        "unidentified",
        "unknown",
    )
    if re.match(r"^\d+\s*(not applicable|not available|not collected|not provided|unknown)\b", normalized):
        return True
    return normalized.startswith(missing_prefixes)


def clean_metadata_concept_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\[[A-Za-z]+:\d+\]", " ", text)
    return normalize_standardization_lookup(text)


_METADATA_SYNONYM_CONTEXT_CACHE: dict[int, tuple[int, tuple[tuple[re.Pattern[str], str, str], ...]]] = {}
_SOURCE_STANDARDIZATION_SYNONYMS_CACHE: tuple[int, int, dict[str, str]] | None = None
_BROAD_STANDARDIZATION_CONTEXT_CACHE: tuple[int, tuple[tuple[re.Pattern[str], str], ...]] | None = None


def clear_standardization_runtime_caches() -> None:
    global _SOURCE_STANDARDIZATION_SYNONYMS_CACHE, _BROAD_STANDARDIZATION_CONTEXT_CACHE
    _METADATA_SYNONYM_CONTEXT_CACHE.clear()
    _SOURCE_STANDARDIZATION_SYNONYMS_CACHE = None
    _BROAD_STANDARDIZATION_CONTEXT_CACHE = None


def metadata_synonym_context_items(synonyms: dict[str, str]) -> tuple[tuple[re.Pattern[str], str, str], ...]:
    cache_key = id(synonyms)
    cached = _METADATA_SYNONYM_CONTEXT_CACHE.get(cache_key)
    if cached is not None and cached[0] == len(synonyms):
        return cached[1]
    items = tuple(
        sorted(
            (
                (
                    re.compile(rf"(^|\s){re.escape(clean_metadata_concept_text(key))}(\s|$)"),
                    standardized,
                    CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(standardized, ""),
                )
                for key, standardized in synonyms.items()
                if clean_metadata_concept_text(key)
            ),
            key=lambda item: len(item[0].pattern),
            reverse=True,
        )
    )
    _METADATA_SYNONYM_CONTEXT_CACHE[cache_key] = (len(synonyms), items)
    return items


def source_standardization_synonyms() -> dict[str, str]:
    global _SOURCE_STANDARDIZATION_SYNONYMS_CACHE
    stamp = (
        len(ISOLATION_SOURCE_SYNONYMS),
        len(SAMPLE_TYPE_SYNONYMS)
        + len(ENVIRONMENT_MEDIUM_SYNONYMS)
        + len(ISOLATION_SITE_SYNONYMS),
    )
    if _SOURCE_STANDARDIZATION_SYNONYMS_CACHE is not None and _SOURCE_STANDARDIZATION_SYNONYMS_CACHE[:2] == stamp:
        return _SOURCE_STANDARDIZATION_SYNONYMS_CACHE[2]
    combined = {
        **SAMPLE_TYPE_SYNONYMS,
        **ENVIRONMENT_MEDIUM_SYNONYMS,
        **ISOLATION_SITE_SYNONYMS,
        **ISOLATION_SOURCE_SYNONYMS,
    }
    _SOURCE_STANDARDIZATION_SYNONYMS_CACHE = (stamp[0], stamp[1], combined)
    return combined


def standardize_metadata_concept(value: Any, synonyms: dict[str, str]) -> tuple[str, str, str]:
    original = "" if value is None else str(value).strip()
    if metadata_value_is_missing(original):
        return "", "missing", ""
    cleaned = clean_metadata_concept_text(original)
    for candidate in standardization_lookup_variants(original):
        if candidate in synonyms:
            standardized = synonyms[candidate]
            return standardized, "dictionary", CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(standardized, "")
    for pattern, standardized, ontology_id in metadata_synonym_context_items(synonyms):
        if pattern.search(cleaned):
            return standardized, "context_dictionary", ontology_id
    return original, "original", ""


def first_standardized_concept(
    values: list[Any],
    synonyms: dict[str, str],
    fallback_standardizer: Callable[[str], str] | None = None,
    *,
    allow_original: bool = False,
) -> tuple[str, str, str]:
    for value in values:
        if metadata_value_is_missing(value):
            continue
        standardized, method, ontology_id = standardize_metadata_concept(value, synonyms)
        if method != "original":
            return standardized, method, ontology_id
        if fallback_standardizer is not None:
            fallback = fallback_standardizer(str(value).strip())
            if not metadata_value_is_missing(fallback):
                return fallback, "standardizer", CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(fallback, "")
        if allow_original:
            return standardized, "original", ""
    return "", "missing", ""


HOST_ONLY_ISOLATION_SOURCE_VALUES = {
    "human",
    "patient",
    "infant",
    "adult",
    "child",
    "male",
    "female",
    "pig",
    "swine",
    "cattle",
    "cow",
    "bovine",
    "dog",
    "canine",
    "equine",
    "goat",
    "duck",
    "avian",
    "fly",
    "mosquito",
    "fish",
    "animal",
}


ISOLATION_SOURCE_CONTEXT_OVERRIDES = {
    "clinical": "clinical sample",
    "clinical sample": "clinical sample",
    "clinical samples": "clinical sample",
    "clinical material": "clinical material",
    "clinical specimen": "clinical sample",
    "homo sapiens clinical": "clinical sample",
    "sterile site": "sterile body site",
    "normally sterile site": "sterile body site",
    "hospital": "healthcare facility",
    "intensive care unit": "healthcare facility",
    "nursing home": "long-term care facility",
    "icu": "healthcare facility",
    "hospital icu": "healthcare facility",
    "sewage": "sewage",
    "hospital sewage": "hospital sewage",
    "activated sludge": "sewage",
    "respiratory": "respiratory sample",
    "tracheal aspirate": "tracheal aspirate/secretion",
    "tracheal secretion": "tracheal aspirate/secretion",
    "bal": "bronchoalveolar lavage fluid",
    "bronchoalveolar lavage": "bronchoalveolar lavage fluid",
    "bronchoalveolar lavage fluid": "bronchoalveolar lavage fluid",
    "np": "nasopharynx/oropharynx",
    "nasopharynx": "nasopharynx/oropharynx",
    "nasopharyngeal": "nasopharynx/oropharynx",
    "nasophaynx": "nasopharynx/oropharynx",
    "pharynx": "nasopharynx/oropharynx",
    "throat": "nasopharynx/oropharynx",
    "pharyngeal exudate": "nasopharynx/oropharynx",
    "nasopharynx oropharynx": "nasopharynx/oropharynx",
    "nasopharynx/oropharynx": "nasopharynx/oropharynx",
    "nose": "nasal site",
    "nasal": "nasal site",
    "nares": "nasal site",
    "nare": "nasal site",
    "nasal cavity": "nasal site",
    "oral": "oral cavity",
    "oral cavity": "oral cavity",
    "oral metagenome": "oral cavity",
    "dental plaque": "dental plaque",
    "urethra": "urethra/penis",
    "urethral": "urethra/penis",
    "penis urethra": "urethra/penis",
    "urogenital": "urogenital",
    "cervix": "cervix",
    "ectocervical mucosa": "ectocervical mucosa",
    "pus": "pus",
    "abscess": "abscess",
    "liver abscess": "liver abscess",
    "bodily fluid": "bodily fluid",
    "fluid": "bodily fluid",
    "pleural fluid": "pleural fluid",
    "drainage": "drainage",
    "aspirate": "aspirate",
    "liver": "liver",
    "brain": "brain",
    "kidney": "kidney",
    "spleen": "spleen",
    "bone": "bone",
    "eye": "eye",
    "ear": "ear",
    "placenta": "placenta",
    "lymph node": "lymph node",
    "skin": "skin",
    "wound": "wound",
    "stomach": "gut content",
    "gastric biopsy": "gastric biopsy",
    "rumen": "gut content",
    "root": "root",
    "rhizosphere": "rhizosphere",
    "leaves": "leaves",
    "plant": "plant",
    "cucumber": "cucumber",
    "potato": "potato",
    "nodule": "nodule",
    "kratom": "kratom",
    "groundwater": "groundwater",
    "river": "river",
    "canal": "canal",
    "irrigation canal": "irrigation canal",
    "stream": "stream",
    "pond": "pond",
    "estuary": "estuary",
    "hot spring": "hot spring",
    "surface layer": "surface layer",
    "deep sea hydrothermal deposit": "deep-sea hydrothermal deposit",
    "produced fluids from hydraulically fractured shales": "produced fluids from hydraulically fractured shales",
    "ice core section from central arctic ocean": "ice core section from central arctic ocean",
    "core": "core",
    "bacteria": "metadata descriptor/non-source",
    "assembly": "metadata descriptor/non-source",
    "microbial community": "metadata descriptor/non-source",
    "host associated strain": "metadata descriptor/non-source",
    "invasive": "metadata descriptor/non-source",
    "screening": "metadata descriptor/non-source",
    "surveillance": "metadata descriptor/non-source",
    "pathogen.cl": "metadata descriptor/non-source",
    "other": "metadata descriptor/non-source",
    "uti": "urinary tract",
    "urinary": "urinary tract",
    "urinary tract": "urinary tract",
    "rectal": "rectum",
    "rectum": "rectum",
    "recto vaginal": "rectovaginal site",
    "recto-vaginal": "rectovaginal site",
    "groin": "groin",
    "bile": "bile",
    "biopsy": "biopsy",
    "bloodstream": "bloodstream",
    "bloodstream isolates": "bloodstream",
    "gill": "gill",
    "trachea": "trachea",
    "secretion": "secretion",
    "sink": "sink",
    "drain": "drain",
    "laboratory": "laboratory",
    "farm": "farm",
    "dairy farm": "dairy farm",
    "deciduous forest": "deciduous forest",
    "rte product": "ready-to-eat product",
    "ice cream": "ice cream",
    "prawns": "prawn product",
    "prawn": "prawn product",
    "ground component chicken": "ground chicken",
    "ground chicken": "ground chicken",
    "carcass": "chicken carcass",
    "spinach": "spinach",
    "papaya": "papaya",
    "vegetable": "vegetable",
    "leafy greens": "leafy greens",
    "tree nut": "tree nut",
    "peanut butter": "peanut butter",
    "tooth": "tooth",
    "catheter": "catheter",
    "sludge": "sludge",
    "metagenome": "metagenome",
    "wildlife": "wildlife",
    "wood": "wood",
    "colon contents": "colon contents",
    "vagina": "vagina",
    "long term care facility": "long-term care facility",
    "anaerobic bioreactor effluent": "anaerobic bioreactor effluent",
    "hospital wastewater": "hospital wastewater",
    "salmon": "salmon",
    "surface": "surface",
    "heart": "heart",
    "ascites": "ascites",
    "broiler": "broiler",
    "enviromental": "environmental sample",
    "environmental": "environmental sample",
    "endovascular": "endovascular",
    "sponge": "sponge",
    "healthy people": "",
    "non hospitalized person": "",
    "hospital patients": "",
    "infants": "",
    "homo": "",
    "homo sapiens": "",
    "companion animal": "",
    "chickens": "",
    "sheep": "",
    "whole stomoxys flies": "",
    "blood from patients of rural regional hospital": "blood",
    "wt mouse intestinal epithelial cells": "intestinal epithelial cells",
    "intestinal epithelial cells": "intestinal epithelial cells",
    "mammary gland": "mammary gland",
    "right cubital fossa": "cubital fossa",
    "cubital fossa": "cubital fossa",
    "abdomen": "abdomen",
    "genitourinary tract": "genitourinary tract",
    "balf": "bronchoalveolar lavage fluid",
    "esputum": "sputum",
    "rectal screening": "rectum",
    "nasal surveillance": "nasal site",
    "cleanroom floor": "cleanroom floor",
    "air": "air",
    "biofilm from cave": "cave biofilm",
    "biofilm from sulfidic cave": "cave biofilm",
    "cold seep": "cold seep",
    "glacier": "glacier",
    "terrestrial": "terrestrial environment",
    "anthropogenic terrestrial biome": "terrestrial environment",
    "kimchi": "kimchi",
    "enoki mushroom": "enoki mushroom",
    "tomato": "tomato",
    "avocado": "avocado",
    "cantaloupe": "cantaloupe",
    "lettuce": "lettuce",
    "dairy product": "dairy product",
    "catfish product": "catfish product",
    "product raw intact siluriformes ictaluridae": "catfish product",
    "product raw intact siluriformes ictaluridae catfish": "catfish product",
    "anaerobic digestion of organic wastes under variable temperature conditions and feedstocks": "anaerobic digester",
    "anaerobic digester": "anaerobic digester",
    "cilantro": "cilantro",
    "cave biofilm": "cave biofilm",
}


def isolation_source_material_context(value: Any) -> str:
    if metadata_value_is_missing(value):
        return ""
    raw_lower = str(value).strip().lower()
    cleaned = clean_metadata_concept_text(value)
    if not cleaned:
        return ""

    food_cut = food_cut_sample_type(value)
    if food_cut:
        return food_cut
    if cleaned in HOST_ONLY_ISOLATION_SOURCE_VALUES:
        return ""
    if cleaned in ISOLATION_SOURCE_CONTEXT_OVERRIDES:
        return ISOLATION_SOURCE_CONTEXT_OVERRIDES[cleaned]
    for key, standardized in sorted(ISOLATION_SOURCE_CONTEXT_OVERRIDES.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
            return standardized

    if cleaned == "poultry":
        return ""
    if re.search(r"\b(caecal|cecal|caecum|cecum|intestin|gastrointestinal|gut)\b", raw_lower) or re.search(r"\b(caecal|cecal|caecum|cecum|intestin|gastrointestinal|gut)\b", cleaned):
        return "gut content"
    if re.search(r"\b(csf|cerebrospinal fluid)\b", cleaned):
        return "cerebrospinal fluid"
    if re.search(r"\b(np swab|nasopharyngeal swab)\b", cleaned):
        return "nasopharyngeal swab"
    if re.search(r"\b(nasal swab|nose swab)\b", cleaned):
        return "nasal swab"
    if re.search(r"\b(chicken|poultry)\b", cleaned) and re.search(r"\b(carcass|meat|breast|comminuted|product|raw|nonintact|intact|rinse|post chill|pre evisceration)\b", cleaned):
        return "chicken meat"
    if re.search(r"\bturkey\b", cleaned) and re.search(r"\b(meat|ground|comminuted|product|raw|nonintact|intact)\b", cleaned):
        return "turkey meat"
    if re.search(r"\bpork\b", cleaned) and re.search(r"\b(meat|ground|product|raw|nonintact|intact)\b", cleaned):
        return "pork"
    if re.search(r"\bbeef\b", cleaned) and re.search(r"\b(meat|ground|comminuted|product|raw|nonintact|intact)\b", cleaned):
        return "beef"
    if re.search(r"\bfreshwater fish\b", cleaned) and re.search(r"\b(product|raw|processed|fillet|market)\b", cleaned):
        return "freshwater fish product"
    if re.search(r"\bfish\b", cleaned) and re.search(r"\b(product|raw|processed|fillet|market)\b", cleaned):
        return "fish product"
    if re.search(r"\b(shrimp|prawn)\b", cleaned) and re.search(r"\b(product|raw|processed|market)\b", cleaned):
        return "shrimp product"
    if re.search(r"\boyster\b", cleaned) and re.search(r"\b(product|raw|processed|shucked|market)\b", cleaned):
        return "oyster product"
    if re.search(r"\b(shellfish|mussel|clam)\b", cleaned) and re.search(r"\b(product|raw|processed|market)\b", cleaned):
        return "shellfish product"
    if cleaned == "seafood":
        return "seafood"
    if cleaned in HOST_ONLY_ISOLATION_SOURCE_VALUES | {"chicken", "turkey"}:
        return ""
    if re.search(r"\banimal (swine|cattle|chicken|turkey)\b", cleaned):
        return ""
    if re.search(r"\b(human|patient|listeriosis|pig|swine|cattle|cow|steer|heifer|chicken|turkey|poultry)\b", cleaned):
        food_like = re.search(r"\b(carcass|meat|breast|ground|comminuted|product|raw|nonintact|pork|beef)\b", cleaned)
        if not food_like:
            return ""
    return str(value).strip()


def broad_standardization_category(value: str) -> str:
    if not value:
        return ""
    cleaned = normalize_standardization_lookup(value)
    if cleaned in STANDARDIZATION_BROAD_CATEGORIES:
        return STANDARDIZATION_BROAD_CATEGORIES[cleaned]
    global _BROAD_STANDARDIZATION_CONTEXT_CACHE
    if (
        _BROAD_STANDARDIZATION_CONTEXT_CACHE is None
        or _BROAD_STANDARDIZATION_CONTEXT_CACHE[0] != len(STANDARDIZATION_BROAD_CATEGORIES)
    ):
        _BROAD_STANDARDIZATION_CONTEXT_CACHE = (
            len(STANDARDIZATION_BROAD_CATEGORIES),
            tuple(
                sorted(
                    (
                        (re.compile(rf"(^|\s){re.escape(normalize_standardization_lookup(key))}(\s|$)"), broad)
                        for key, broad in STANDARDIZATION_BROAD_CATEGORIES.items()
                    ),
                    key=lambda item: len(item[0].pattern),
                    reverse=True,
                )
            ),
        )
    for pattern, broad in _BROAD_STANDARDIZATION_CONTEXT_CACHE[1]:
        if pattern.search(cleaned):
            return broad
    # Broad fields must remain controlled vocabularies. Preserve the specific
    # standardized value in the *_SD column, but do not leak raw/noisy values
    # into *_SD_Broad when no controlled broad category is known.
    return ""


def standardize_secondary_metadata(row: dict[str, Any], host_standardization: dict[str, str]) -> dict[str, str]:
    host_value = row.get("Host")
    host_method = str(host_standardization.get("Host_SD_Method") or "")
    host_as_context = host_value if host_method == "non_host_source" else ""
    host_disease_context = [
        row.get("Host Disease"),
        row.get("BioSample Host Disease"),
        row.get("BioSample Disease"),
        row.get("BioSample Study Disease"),
        row.get("BioSample Disease State"),
    ]
    disease_candidate_values = [*host_disease_context, row.get("Isolation Source")]
    anatomy_context_values = [
        row.get("Isolation Site"),
        row.get("BioSample Isolation Site"),
        row.get("BioSample Body Site"),
        row.get("BioSample Organism Part"),
        row.get("BioSample Tissue"),
        row.get("BioSample Tissue Type"),
        row.get("BioSample Host Tissue Sampled"),
        row.get("Sample Type"),
        row.get("Isolation Source"),
    ]
    anatomy_site = next((site for site in (canonical_anatomical_site(value) for value in anatomy_context_values) if site), "")
    body_site_sample_type = next(
        (sample for sample in (sample_type_from_body_site_context(value) for value in anatomy_context_values) if sample),
        "",
    )
    isolation_source_material = isolation_source_material_context(row.get("Isolation Source"))
    isolation_source, isolation_method, isolation_ontology_id = first_standardized_concept(
        [isolation_source_material, row.get("Isolation Site"), host_as_context],
        source_standardization_synonyms(),
        standardize_isolation_source,
    )
    host_disease_source, host_disease_source_method, host_disease_source_ontology_id = first_standardized_concept(
        host_disease_context,
        source_standardization_synonyms(),
    )
    if host_disease_source and (not isolation_source or isolation_method == "missing"):
        isolation_source, isolation_method, isolation_ontology_id = (
            host_disease_source,
            host_disease_source_method,
            host_disease_source_ontology_id,
        )
    isolation_site, isolation_site_method, isolation_site_ontology_id = first_standardized_concept(
        [
            row.get("Isolation Site"),
            row.get("BioSample Isolation Site"),
            row.get("BioSample Body Site"),
            row.get("BioSample Organism Part"),
            row.get("BioSample Tissue"),
            row.get("BioSample Tissue Type"),
            row.get("BioSample Host Tissue Sampled"),
            row.get("Sample Type"),
            row.get("Isolation Source"),
        ],
        ISOLATION_SITE_SYNONYMS,
    )
    if anatomy_site and (not isolation_site or canonical_anatomical_site(isolation_site)):
        isolation_site = anatomy_site
        isolation_site_method = "anatomy_router"
        isolation_site_ontology_id = ""
    environment_broad, environment_broad_method, environment_broad_ontology_id = first_standardized_concept(
        [
            row.get("Environment (Broad Scale)"),
            row.get("BioSample ENV Broad Scale"),
            row.get("BioSample ENV Biome"),
            row.get("BioSample Environment Biome"),
            row.get("BioSample Biome"),
            row.get("BioSample Metagenome Source"),
            row.get("Environment Medium"),
            row.get("Isolation Source"),
            row.get("Sample Type"),
            host_as_context,
            *host_disease_context,
        ],
        ENVIRONMENT_BROAD_SYNONYMS,
    )
    environment_local, environment_local_method, environment_local_ontology_id = first_standardized_concept(
        [
            row.get("Environment (Local Scale)"),
            row.get("BioSample ENV Local Scale"),
            row.get("BioSample ENV Feature"),
            row.get("BioSample Environment Feature"),
            row.get("BioSample Feature"),
            row.get("BioSample Collection Site"),
            row.get("BioSample Sample Site"),
            row.get("BioSample Location Type"),
            row.get("Isolation Source"),
            row.get("Sample Type"),
            host_as_context,
            *host_disease_context,
        ],
        ENVIRONMENT_LOCAL_SYNONYMS,
    )
    environment_medium, environment_method, environment_ontology_id = first_standardized_concept(
        [
            row.get("Environment Medium"),
            row.get("Environment (Local Scale)"),
            row.get("Environment (Broad Scale)"),
            row.get("Isolation Source"),
            row.get("Sample Type"),
            host_as_context,
            *host_disease_context,
        ],
        ENVIRONMENT_MEDIUM_SYNONYMS,
    )
    sample_type, sample_type_method, sample_type_ontology_id = first_standardized_concept(
        [row.get("Sample Type"), isolation_source_material, row.get("Isolation Source"), host_as_context, *host_disease_context],
        SAMPLE_TYPE_SYNONYMS,
    )
    host_sample_type, host_sample_method, host_sample_ontology_id = host_context_sample_type(host_standardization, host_value)
    if host_sample_type and (not sample_type or sample_type in {"culture", "microbial culture", "microbial isolate"}):
        sample_type, sample_type_method, sample_type_ontology_id = (
            host_sample_type,
            host_sample_method,
            host_sample_ontology_id,
        )
    if body_site_sample_type and (not sample_type or sample_type_is_site_only(sample_type) or sample_type in {"sample"}):
        sample_type, sample_type_method, sample_type_ontology_id = (
            body_site_sample_type,
            "body_site_material_router",
            CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(body_site_sample_type, ""),
        )
    host_disease, host_disease_method, host_disease_ontology_id = first_standardized_concept(
        disease_candidate_values,
        HOST_DISEASE_SYNONYMS,
    )
    host_health_state, host_health_state_method, host_health_state_ontology_id = first_standardized_concept(
        [
            row.get("Host Health State"),
            row.get("BioSample Host Health State"),
            row.get("BioSample Health State"),
            row.get("BioSample Health Status"),
            row.get("BioSample Host Health"),
            *disease_candidate_values,
        ],
        HOST_HEALTH_STATE_SYNONYMS,
    )
    sample_type = sanitize_sample_type_standardization(sample_type)
    if not sample_type:
        sample_type_method = "missing"
        sample_type_ontology_id = ""
    source_context = source_context_for_anatomical_site(isolation_source)
    if source_context:
        isolation_source = source_context
        isolation_method = "anatomy_source_router"
        isolation_ontology_id = CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(source_context, "")
    disease_source_context = source_context_for_disease_or_health(isolation_source)
    if disease_source_context:
        isolation_source = disease_source_context
        isolation_method = "disease_source_router"
        isolation_ontology_id = CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(disease_source_context, "")
    lab_context = source_context_for_lab_or_metadata_artifact(isolation_source)
    if lab_context is not None:
        isolation_source, isolation_method = lab_context
        isolation_ontology_id = CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(isolation_source, "")
    if host_disease and not host_health_state and host_disease != "healthy/no disease reported":
        host_health_state = "diseased"
        host_health_state_method = "disease_inference"
        host_health_state_ontology_id = ""
    return {
        "Isolation_Source_SD": isolation_source,
        "Isolation_Source_SD_Broad": broad_standardization_category(isolation_source),
        "Isolation_Source_SD_Detail": isolation_source,
        "Isolation_Source_SD_Method": isolation_method,
        "Isolation_Source_Ontology_ID": isolation_ontology_id,
        "Isolation_Site_SD": isolation_site,
        "Isolation_Site_SD_Broad": broad_standardization_category(isolation_site),
        "Isolation_Site_SD_Detail": isolation_site,
        "Isolation_Site_SD_Method": isolation_site_method,
        "Isolation_Site_Ontology_ID": isolation_site_ontology_id,
        "Environment_Broad_Scale_SD": environment_broad,
        "Environment_Broad_Scale_SD_Broad": broad_standardization_category(environment_broad),
        "Environment_Broad_Scale_SD_Detail": environment_broad,
        "Environment_Broad_Scale_SD_Method": environment_broad_method,
        "Environment_Broad_Scale_Ontology_ID": environment_broad_ontology_id,
        "Environment_Local_Scale_SD": environment_local,
        "Environment_Local_Scale_SD_Broad": broad_standardization_category(environment_local),
        "Environment_Local_Scale_SD_Detail": environment_local,
        "Environment_Local_Scale_SD_Method": environment_local_method,
        "Environment_Local_Scale_Ontology_ID": environment_local_ontology_id,
        "Environment_Medium_SD": environment_medium,
        "Environment_Medium_SD_Broad": broad_standardization_category(environment_medium),
        "Environment_Medium_SD_Detail": environment_medium,
        "Environment_Medium_SD_Method": environment_method,
        "Environment_Medium_Ontology_ID": environment_ontology_id,
        "Sample_Type_SD": sample_type,
        "Sample_Type_SD_Broad": broad_standardization_category(sample_type),
        "Sample_Type_SD_Detail": sample_type,
        "Sample_Type_SD_Method": sample_type_method,
        "Sample_Type_Ontology_ID": sample_type_ontology_id,
        "Host_Disease_SD": host_disease,
        "Host_Disease_SD_Broad": broad_standardization_category(host_disease),
        "Host_Disease_SD_Detail": host_disease,
        "Host_Disease_SD_Method": host_disease_method,
        "Host_Disease_Ontology_ID": host_disease_ontology_id,
        "Host_Health_State_SD": host_health_state,
        "Host_Health_State_SD_Broad": broad_standardization_category(host_health_state),
        "Host_Health_State_SD_Detail": host_health_state,
        "Host_Health_State_SD_Method": host_health_state_method,
        "Host_Health_State_Ontology_ID": host_health_state_ontology_id,
    }


def standardize_primary_metadata_value(field: str, value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if field == "Collection Date":
        return standardize_date(text)
    if field == "Geographic Location":
        return standardize_location(text)
    if field == "Host":
        return standardize_host(text)
    if field == "Isolation Source":
        return standardize_isolation_source(text)
    return standardize_isolation_source(text)


def fuzzy_similarity_score(left: str, right: str) -> int:
    return int(round(difflib.SequenceMatcher(None, left, right).ratio() * 100))


def best_fuzzy_string_match(value: Any, choices: list[str], min_score: int = 92) -> tuple[str, int] | None:
    cleaned = normalize_standardization_lookup(value)
    if len(cleaned) < 4:
        return None
    best_choice = ""
    best_score = 0
    compact_cleaned = cleaned.replace(" ", "")
    for choice in choices:
        normalized_choice = normalize_standardization_lookup(choice)
        if len(normalized_choice) < 4:
            continue
        score = max(
            fuzzy_similarity_score(cleaned, normalized_choice),
            fuzzy_similarity_score(compact_cleaned, normalized_choice.replace(" ", "")),
        )
        if score > best_score:
            best_choice = choice
            best_score = score
    if not best_choice or best_score < min_score:
        return None
    return best_choice, best_score


def fuzzy_refinement_candidate(source_column: str, value: Any) -> dict[str, str] | None:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text):
        return None

    if source_column == "Host":
        host_choices = list(HOST_SYNONYMS.keys()) + list(HOST_BROAD_SYNONYMS.keys())
        match = best_fuzzy_string_match(text, host_choices, min_score=93)
        if not match:
            return None
        key, score = match
        name, taxid = HOST_SYNONYMS.get(key) or HOST_BROAD_SYNONYMS.get(key) or ("", "")
        if not name or not taxid:
            return None
        return {
            "category": "host organism",
            "destination": "Host_SD",
            "proposed_value": name,
            "ontology_id": taxid,
            "method": f"fuzzy_match:{score}",
            "confidence": "medium",
            "action": "review",
            "suggestion_score": str(score),
            "note": f"Fuzzy host match to '{key}'. Admin approval required.",
        }

    if source_column in {"Environment Medium", "Environment (Broad Scale)", "Environment (Local Scale)"}:
        destination = "Environment_Medium_SD"
        category = "environment medium"
        synonyms = ENVIRONMENT_MEDIUM_SYNONYMS
    elif source_column == "Sample Type":
        destination = "Sample_Type_SD"
        category = "sample type"
        synonyms = SAMPLE_TYPE_SYNONYMS
    elif source_column == "Isolation Source":
        destination = "Isolation_Source_SD"
        category = "isolation source"
        synonyms = {**SAMPLE_TYPE_SYNONYMS, **ENVIRONMENT_MEDIUM_SYNONYMS}
    else:
        return None

    match = best_fuzzy_string_match(text, list(synonyms.keys()), min_score=91)
    if not match:
        return None
    key, score = match
    proposed = synonyms.get(key, "")
    if not proposed:
        return None
    return {
        "category": category,
        "destination": destination,
        "proposed_value": proposed,
        "ontology_id": CONTROLLED_CATEGORY_ONTOLOGY_IDS.get(proposed, ""),
        "method": f"fuzzy_match:{score}",
        "confidence": "medium",
        "action": "review",
        "suggestion_score": str(score),
        "note": f"Fuzzy metadata match to '{key}'. Admin approval required.",
    }


def harmonize_primary_metadata_aliases(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for primary, aliases in PRIMARY_METADATA_ALIASES.items():
        current = normalized.get(primary)
        if not metadata_value_is_missing(current):
            normalized[primary] = standardize_primary_metadata_value(primary, current)
            continue
        for alias in aliases:
            candidate = normalized.get(alias)
            if metadata_value_is_missing(candidate):
                continue
            normalized[primary] = standardize_primary_metadata_value(primary, candidate)
            break
    return normalized


COLLECTION_DATE_PRIMARY_COLUMNS = ["Collection Date", *PRIMARY_METADATA_ALIASES["Collection Date"]]
COLLECTION_DATE_SECONDARY_TEXT_COLUMNS = [
    "BioSample Description",
    "BioSample Title",
    "BioSample Comment",
    "BioSample Comments",
    "BioSample Collection Date Remark",
    "BioSample Isolation Source",
    "Isolation Source",
    "BioSample Source Name",
]
COLLECTION_DATE_CONTEXT_PATTERN = re.compile(
    r"\b(?:collection|collected|collecting|sampled|sampling|isolation\s+date|isolated|harvest|harvested|"
    r"specimen\s+collection|date\s+of\s+collection|collection\s+year)\b",
    re.IGNORECASE,
)
COLLECTION_DATE_FALSE_POSITIVE_PATTERN = re.compile(
    r"\b(?:protocols?|described\s+previously|et\s+al\.|publication|published|submitted|submission|"
    r"sequenc(?:e|ed|ing)|assembly|bioproject|biosample|accession|created|modified|updated|"
    r"data\s+agreement)\b",
    re.IGNORECASE,
)
REVIEWED_COLLECTION_DATE_VALUES: dict[str, str] = {}


def reviewed_collection_year(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    return REVIEWED_COLLECTION_DATE_VALUES.get(re.sub(r"\s+", " ", text).strip().lower())


def load_reviewed_collection_date_rules() -> None:
    current_year = datetime.now(timezone.utc).year
    for path in [STANDARDIZATION_DIR / "collection_date_reviewed_rules.csv", DATA_DIR / "collection_date_reviewed_rules.csv"]:
        if not path.exists():
            continue
        for row in load_standardization_csv(path):
            source_value = re.sub(r"\s+", " ", str(row.get("source_value") or "")).strip().lower()
            year = str(row.get("year") or "").strip()
            if source_value and re.fullmatch(r"(?:19|20)\d{2}", year) and 1900 <= int(year) <= current_year:
                REVIEWED_COLLECTION_DATE_VALUES[source_value] = year


load_reviewed_collection_date_rules()


def standardize_collection_year_value(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text) or COLLECTION_DATE_FALSE_POSITIVE_PATTERN.search(text):
        return None
    if re.search(r"\bmissing\b", text, re.IGNORECASE):
        return None
    if not re.search(r"\b(?:19|20)\d{2}\b", text) and re.search(r"\b\d{3}\b", text):
        return None
    parsed = standardize_date(text)
    return None if metadata_value_is_missing(parsed) else parsed


def extract_year_from_collection_text(value: Any, require_context: bool = True) -> tuple[str, str, str] | None:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text) or COLLECTION_DATE_FALSE_POSITIVE_PATTERN.search(text):
        return None
    reviewed_year = reviewed_collection_year(text)
    if reviewed_year:
        return reviewed_year, text[:180], "reviewed_secondary"

    compact = re.sub(r"\s+", " ", text)
    for match in re.finditer(r"\b(?:19|20)\d{2}\b", compact):
        year = match.group(0)
        current_year = datetime.now(timezone.utc).year
        if not (1900 <= int(year) <= current_year):
            continue
        window = compact[max(0, match.start() - 80) : match.end() + 80]
        before = compact[max(0, match.start() - 80) : match.start()]
        after = compact[match.end() : match.end() + 80]
        if require_context and not (
            COLLECTION_DATE_CONTEXT_PATTERN.search(before) or COLLECTION_DATE_CONTEXT_PATTERN.search(after)
        ):
            continue
        return year, window.strip()[:180], "rule_secondary" if require_context else "trusted_primary"

    if require_context:
        return None

    parsed = standardize_collection_year_value(compact)
    if parsed:
        return parsed, compact[:180], "rule_secondary" if require_context else "trusted_primary"
    return None


def recover_collection_date(row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    existing_status = str(row.get("Collection_Date_Recovery_Status") or "").strip()
    existing_year = standardize_collection_year_value(row.get("Collection Date"))
    if existing_status in {"reviewed_secondary", "rule_secondary"} and existing_year:
        source = str(row.get("Collection_Date_Source") or "Collection Date").strip()
        evidence = str(row.get("Collection_Date_Evidence") or row.get("Collection Date") or "").strip()
        return existing_year, source, evidence[:180], existing_status
    for column in COLLECTION_DATE_PRIMARY_COLUMNS:
        parsed = standardize_collection_year_value(row.get(column))
        if parsed:
            value = "" if row.get(column) is None else str(row.get(column)).strip()
            return parsed, column, value[:180], "trusted_primary"
    for column in COLLECTION_DATE_SECONDARY_TEXT_COLUMNS:
        recovered = extract_year_from_collection_text(row.get(column), require_context=True)
        if recovered:
            year, evidence, status = recovered
            return year, column, evidence, status
    return None


def harmonize_collection_date_metadata(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    recovered = recover_collection_date(normalized)
    if recovered:
        year, source, evidence, status = recovered
        normalized["Collection Date"] = year
        normalized["Collection_Date_Source"] = source
        normalized["Collection_Date_Evidence"] = evidence
        normalized["Collection_Date_Recovery_Status"] = status
        return normalized

    current = normalized.get("Collection Date")
    normalized["Collection Date"] = "absent" if metadata_value_is_missing(current) else "unknown"
    normalized["Collection_Date_Source"] = ""
    normalized["Collection_Date_Evidence"] = ""
    normalized["Collection_Date_Recovery_Status"] = normalized["Collection Date"]
    return normalized


SECONDARY_GEO_DIRECT_COLUMNS = [
    "BioSample ENV Local Scale",
    "BioSample ENV Broad Scale",
]
SECONDARY_GEO_TEXT_COLUMNS = [
    "BioSample Isolation Source",
    "Isolation Source",
    "BioSample Source Name",
    "BioSample Description",
    "BioSample Title",
]
SECONDARY_GEO_FALSE_POSITIVE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bprotocols?:",
        r"\bground\s+turkey\b",
        r"\bgound\s+turkey\b",
        r"\bturkey\s+(?:embryo|embryos|meat|product|farm|flock|litter|cecum|caecum|cloaca|feces|faeces|gut|intestine|poult|salad|sinus|trachea|tracheae)\b",
        r"\bturkey\s+(?:pork|beef|hot\s+dog|frank|filet|goulash|steak|patty)\b",
        r"\bguinea[-\s]?pig\b",
        r"\bguinea\s+fowl\b",
        r"\bnorway\s+rat\b",
        r"\b(?:a\.|aspergillus)\s+niger\b",
        r"\bcordylus\s+niger\b",
        r"\blizard\s*\([^)]*\bniger\b[^)]*\)",
        r"\bniger\s+(?:mycelia|strain|isolate|culture|spore|hyphae)\b",
        r"\bdeschampsia\s+antarctica\b",
    ]
]
SECONDARY_GEO_LOCATION_CUE_PATTERN = re.compile(
    r"\b(?:in|from|at|near|within|collected\s+(?:in|from|at)|isolated\s+(?:in|from|at)|"
    r"sampled\s+(?:in|from|at)|obtained\s+from|originating\s+from|region[, ]+|province[, ]+|site[, ]+)",
    re.IGNORECASE,
)
SECONDARY_GEO_DIRECTIONAL_PREFIXES = ("north", "south", "east", "west", "northern", "southern", "eastern", "western", "central")
REVIEWED_SECONDARY_GEO_VALUES = {
    "usa": "United States",
    "soil around the arctic ocean": "Arctic Ocean",
    "blood - animal united kingdom": "United Kingdom",
    "soil, moscow, ussr": "Northern Asia",
    "gulf of mexico": "Mexico",
    "seawater off the coast of georgia": "Georgia",
    "arctic ocean sediment": "Arctic Ocean",
    "blue lagoon, iceland at 20 cm": "Iceland",
    "the mediterranean sea": "Mediterranean Sea",
    "boston harbor massachusetts, united states isolation date: 1999": "United States",
    "plant (bean pod) australia": "Australia",
    "oral cavity - animal (dental plaque of dairy cattle, belfast, northern) belfast ireland": "Ireland",
    "clinical specimen - human pennsylvania, united states isolation date: 1988": "United States",
    "blood - human jonkoping sweden isolation date: 1993": "Sweden",
    "alkaline kenya isolation date: june, 2002": "Kenya",
    "blood - human houston texas, united states isolation date: january 18, 2000": "United States",
}


def load_reviewed_secondary_geo_rules() -> None:
    for path in [STANDARDIZATION_DIR / "geography_reviewed_rules.csv", DATA_DIR / "geography_reviewed_rules.csv"]:
        if not path.exists():
            continue
        for row in load_standardization_csv(path):
            source_value = re.sub(r"\s+", " ", str(row.get("source_value") or "")).strip().lower()
            country = str(row.get("country") or "").strip()
            if source_value and country in COUNTRY_MAPPING:
                REVIEWED_SECONDARY_GEO_VALUES[source_value] = country


load_reviewed_secondary_geo_rules()


def normalize_country_candidate(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text):
        return None
    country = extract_country(text)
    if country in COUNTRY_MAPPING:
        return str(country)
    country = normalize_country_name(text)
    return str(country) if country in COUNTRY_MAPPING else None


def secondary_geo_text_blocked(text: str) -> bool:
    return any(pattern.search(text) for pattern in SECONDARY_GEO_FALSE_POSITIVE_PATTERNS)


def reviewed_secondary_geo_country(text: Any) -> str | None:
    value = "" if text is None else str(text).strip()
    if not value:
        return None
    compact = re.sub(r"\s+", " ", value).strip().lower()
    return REVIEWED_SECONDARY_GEO_VALUES.get(compact)


def secondary_geo_country_context_blocked(country: str, text: str, match: re.Match[str]) -> bool:
    after = text[match.end() : match.end() + 80].lower()
    before = text[max(0, match.start() - 80) : match.start()].lower()
    phrase = text[max(0, match.start() - 80) : match.end() + 80].lower()

    if re.match(r"\s+style\b", after):
        return True
    if country == "Turkey" and re.search(
        r"\bturkey\s+(?:embryo|embryos|meat|product|farm|flock|litter|cecum|caecum|cloaca|"
        r"feces|faeces|gut|intestine|pork|beef|hot\s+dog|frank|filet|goulash|patty|poult|"
        r"salad|sinus|steak|trachea|tracheae)\b",
        phrase,
    ):
        return True
    if country == "Turkey" and re.search(r"\b(?:ground|gound)\s+turkey\b", phrase):
        return True
    if country == "Guinea" and re.search(r"\bguinea[-\s]?(?:pig|fowl)\b", phrase):
        return True
    if country == "Norway" and re.search(r"\bnorway\s+rat\b", phrase):
        return True
    if country == "Niger" and re.search(r"\b(?:a\.|aspergillus|cordylus)\s+niger\b|\bniger\s+(?:mycelia|strain|isolate|culture|spore|hyphae)\b", phrase):
        return True
    if country == "Niger" and re.search(r"\blizard\s*\([^)]*\bniger\b[^)]*\)", phrase):
        return True
    if country == "Antarctica" and re.search(r"\bdeschampsia\s+antarctica\b", phrase):
        return True
    if country in {"Turkey", "Guinea", "Norway", "Niger"} and re.search(r"\b(?:host|animal)\s*$", before):
        return True
    return False


def recover_country_from_secondary_text(text: Any) -> tuple[str, str, str] | None:
    value = "" if text is None else str(text).strip()
    if metadata_value_is_missing(value) or secondary_geo_text_blocked(value):
        return None

    reviewed_country = reviewed_secondary_geo_country(value)
    if reviewed_country:
        return reviewed_country, value, "reviewed_secondary"

    direct = normalize_country_candidate(value)
    if direct:
        if direct in {"Turkey", "Guinea", "Norway", "Niger"} and not SECONDARY_GEO_LOCATION_CUE_PATTERN.search(value):
            return None
        return direct, value, "rule_secondary"

    compact = re.sub(r"\s+", " ", value)
    for country in sorted(COUNTRY_MAPPING, key=len, reverse=True):
        if len(country) < 4:
            continue
        escaped = re.escape(country)
        country_pattern = re.compile(rf"(?<![A-Za-z]){escaped}(?![A-Za-z])", re.IGNORECASE)
        match = country_pattern.search(compact)
        if not match:
            continue
        before = compact[max(0, match.start() - 50) : match.start()]
        after = compact[match.end() : match.end() + 50]
        phrase = compact[max(0, match.start() - 60) : match.end() + 60].strip()
        prefix = before.strip().split()[-1].lower() if before.strip().split() else ""
        if secondary_geo_country_context_blocked(str(country), compact, match):
            continue
        if country in {"Turkey", "Guinea", "Norway", "Niger"} and not SECONDARY_GEO_LOCATION_CUE_PATTERN.search(before):
            continue
        if prefix in SECONDARY_GEO_DIRECTIONAL_PREFIXES:
            return str(country), phrase, "rule_secondary"
        if SECONDARY_GEO_LOCATION_CUE_PATTERN.search(before):
            return str(country), phrase, "rule_secondary"
        if re.match(r"^\s*(?:[,;:.]|$)", after) and re.search(r"[,;:]\s*$", before):
            return str(country), phrase, "rule_secondary"
    return None


def recover_secondary_geography(row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    for column in SECONDARY_GEO_DIRECT_COLUMNS:
        candidate = normalize_country_candidate(row.get(column))
        if candidate:
            value = "" if row.get(column) is None else str(row.get(column)).strip()
            return candidate, column, value[:180], "rule_secondary"
    for column in ["Host", "BioSample Host"]:
        reviewed_country = reviewed_secondary_geo_country(row.get(column))
        if reviewed_country:
            value = "" if row.get(column) is None else str(row.get(column)).strip()
            return reviewed_country, column, value[:180], "reviewed_secondary"
    for column in SECONDARY_GEO_TEXT_COLUMNS:
        recovered = recover_country_from_secondary_text(row.get(column))
        if recovered:
            country, evidence, status = recovered
            return country, column, evidence[:180], status
    return None


def harmonize_geography_metadata(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    country = normalize_country_candidate(normalized.get("Country"))
    country_source = "Country" if country else ""
    country_confidence = "trusted" if country_source else ""
    country_evidence = "" if not country else str(normalized.get("Country")).strip()[:180]
    geo_recovery_status = "trusted_primary" if country_source else ""
    if metadata_value_is_missing(country):
        country = extract_country(normalized.get("Geographic Location"))
        if not metadata_value_is_missing(country):
            country_source = "Geographic Location"
            country_confidence = "trusted"
            country_evidence = "" if normalized.get("Geographic Location") is None else str(normalized.get("Geographic Location")).strip()[:180]
            geo_recovery_status = "trusted_primary"
    else:
        country = extract_country(str(country).split(":", 1)[0])

    if metadata_value_is_missing(country):
        recovered = recover_secondary_geography(normalized)
        if recovered:
            country, country_source, country_evidence, geo_recovery_status = recovered
            country_confidence = "high"

    if metadata_value_is_missing(country):
        normalized["Country"] = "absent" if metadata_value_is_missing(normalized.get("Geographic Location")) else "unknown"
        normalized["Continent"] = "absent" if normalized["Country"] == "absent" else "unknown"
        normalized["Subcontinent"] = normalized["Continent"]
        normalized["Country_Source"] = ""
        normalized["Country_Confidence"] = ""
        normalized["Country_Evidence"] = ""
        normalized["Geo_Recovery_Status"] = "absent" if normalized["Country"] == "absent" else "unknown"
        return normalized

    if str(country) not in COUNTRY_MAPPING:
        normalized["Country"] = "unknown"
        normalized["Continent"] = "unknown"
        normalized["Subcontinent"] = "unknown"
        normalized["Country_Source"] = ""
        normalized["Country_Confidence"] = ""
        normalized["Country_Evidence"] = ""
        normalized["Geo_Recovery_Status"] = "unknown"
        return normalized

    normalized["Country"] = country
    mapping = COUNTRY_MAPPING.get(str(country), {})
    normalized["Continent"] = mapping.get("Continent") or "unknown"
    normalized["Subcontinent"] = mapping.get("Subcontinent") or "unknown"
    normalized["Country_Source"] = country_source
    normalized["Country_Confidence"] = country_confidence
    normalized["Country_Evidence"] = country_evidence
    normalized["Geo_Recovery_Status"] = geo_recovery_status or "trusted_primary"
    return normalized


def ensure_managed_metadata_schema(row: dict[str, Any]) -> dict[str, Any]:
    normalized = harmonize_geography_metadata(harmonize_collection_date_metadata(harmonize_primary_metadata_aliases(row)))
    for column in SPECIES_TSV_COLUMNS:
        normalized.setdefault(column, None)
    host_source_value = normalized.get("Host")
    host_standardization = standardize_host_metadata(normalized.get("Host"))
    if not host_standardization.get("Host_TaxID"):
        context_host_standardization = standardize_host_from_metadata_context(normalized)
        if context_host_standardization is not None:
            host_standardization = context_host_standardization
            host_source_value = context_host_standardization.get("_Host_Source_Value") or host_source_value
    host_standardization = enrich_host_standardization(host_source_value, host_standardization)
    for column in HOST_STANDARDIZATION_COLUMNS:
        normalized[column] = host_standardization[column]
    secondary_standardization = standardize_secondary_metadata(normalized, host_standardization)
    for column in SECONDARY_STANDARDIZATION_COLUMNS:
        normalized[column] = secondary_standardization[column]
    if not str(normalized.get("Host_Anatomical_Site_SD") or "").strip():
        anatomical_site = canonical_anatomical_site(secondary_standardization.get("Isolation_Site_SD"))
        if anatomical_site:
            normalized["Host_Anatomical_Site_SD"] = anatomical_site
    return normalized


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


def load_taxon_metadata_row_chunk(
    species_id: int,
    *,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    with get_sqlite_connection() as db:
        rows = db.execute(
            """
            SELECT row_json
            FROM assembly_metadata
            WHERE species_id = ?
            ORDER BY assembly_accession
            LIMIT ? OFFSET ?
            """,
            (species_id, limit, offset),
        ).fetchall()
    return [json.loads(str(row["row_json"])) for row in rows]


def load_taxon_metadata_rows_for_accessions(
    species_id: int,
    accessions: list[str] | set[str],
) -> dict[str, dict[str, Any]]:
    unique_accessions = sorted({str(accession).strip() for accession in accessions if str(accession).strip()})
    if not unique_accessions:
        return {}
    payloads: dict[str, dict[str, Any]] = {}
    with get_sqlite_connection() as db:
        for start in range(0, len(unique_accessions), SQLITE_VARIABLE_CHUNK_SIZE):
            chunk = unique_accessions[start : start + SQLITE_VARIABLE_CHUNK_SIZE]
            placeholders = ", ".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT assembly_accession, row_json
                FROM assembly_metadata
                WHERE species_id = ?
                  AND assembly_accession IN ({placeholders})
                """,
                (species_id, *chunk),
            ).fetchall()
            for row in rows:
                payloads[str(row["assembly_accession"])] = json.loads(str(row["row_json"]))
    return payloads


def count_taxon_metadata_rows_for_accessions(species_id: int, accessions: set[str]) -> int:
    if not accessions:
        return 0
    sorted_accessions = sorted(accessions)
    total = 0
    with get_sqlite_connection() as db:
        for start in range(0, len(sorted_accessions), SQLITE_VARIABLE_CHUNK_SIZE):
            chunk = sorted_accessions[start : start + SQLITE_VARIABLE_CHUNK_SIZE]
            placeholders = ", ".join("?" for _ in chunk)
            row = db.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM assembly_metadata
                WHERE species_id = ?
                  AND assembly_accession IN ({placeholders})
                """,
                (species_id, *chunk),
            ).fetchone()
            total += int(row["total"] or 0) if row is not None else 0
    return total


def count_taxon_metadata_rows(species_id: int) -> int:
    with get_sqlite_connection() as db:
        row = db.execute(
            "SELECT COUNT(*) AS total FROM assembly_metadata WHERE species_id = ?",
            (species_id,),
        ).fetchone()
    return int(row["total"] or 0) if row is not None else 0


def save_taxon_metadata_rows(
    species_id: int,
    rows: list[dict[str, Any]],
    *,
    refreshed_at: str,
    normalize_rows: bool = True,
) -> None:
    if not rows:
        return
    serialized_rows = []
    accessions: list[str] = []
    for row in rows:
        if normalize_rows:
            row = ensure_managed_metadata_schema(row)
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
            if len(accessions) <= SQLITE_VARIABLE_CHUNK_SIZE:
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
                db.execute("CREATE TEMP TABLE IF NOT EXISTS temp_keep_accessions (assembly_accession TEXT PRIMARY KEY)")
                db.execute("DELETE FROM temp_keep_accessions")
                db.executemany(
                    "INSERT OR IGNORE INTO temp_keep_accessions (assembly_accession) VALUES (?)",
                    ((accession,) for accession in accessions),
                )
                db.execute(
                    """
                    DELETE FROM assembly_metadata
                    WHERE species_id = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM temp_keep_accessions
                          WHERE temp_keep_accessions.assembly_accession = assembly_metadata.assembly_accession
                      )
                    """,
                    (species_id,),
                )
                db.execute("DELETE FROM temp_keep_accessions")
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
    try:
        species = load_species(species_id)
        refresh_metadata_species_search_entries(species, rows)
    except Exception:
        logging.exception("Failed to refresh metadata species search entries for taxon %s.", species_id)


def upsert_taxon_metadata_rows(
    species_id: int,
    rows: list[dict[str, Any]],
    *,
    refreshed_at: str,
) -> int:
    if not rows:
        return 0
    serialized_rows = []
    for row in rows:
        row = ensure_managed_metadata_schema(row)
        accession = metadata_row_accession(row)
        if not accession:
            continue
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
    if not serialized_rows:
        return 0
    with get_sqlite_connection() as db:
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
    return len(serialized_rows)


def update_taxon_metadata_progress(
    species_id: int,
    *,
    total: int,
    completed: int,
    current_accession: str | None = None,
) -> None:
    now = utc_now()
    with get_sqlite_connection() as db:
        db.execute(
            """
            UPDATE species
            SET metadata_progress_total = ?,
                metadata_progress_completed = ?,
                metadata_progress_current_accession = ?,
                metadata_progress_updated_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (total, completed, current_accession, now, now, species_id),
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
        "metadata_progress_total": "INTEGER NOT NULL DEFAULT 0",
        "metadata_progress_completed": "INTEGER NOT NULL DEFAULT 0",
        "metadata_progress_current_accession": "TEXT",
        "metadata_progress_updated_at": "TEXT",
        "assembly_backfill_status": "TEXT NOT NULL DEFAULT 'idle'",
        "assembly_backfill_requested_at": "TEXT",
        "assembly_backfill_claimed_by": "TEXT",
        "assembly_backfill_claimed_at": "TEXT",
        "assembly_backfill_last_built_at": "TEXT",
        "assembly_backfill_error": "TEXT",
        "is_live": "INTEGER NOT NULL DEFAULT 1",
        "staging_dataset_version_id": "TEXT",
        "live_status": "TEXT",
        "live_tsv_path": "TEXT",
        "live_metadata_status": "TEXT",
        "live_metadata_path": "TEXT",
        "live_metadata_clean_path": "TEXT",
        "live_genome_count": "INTEGER",
        "live_taxon_id": "INTEGER",
        "live_last_synced_at": "TEXT",
        "live_metadata_last_built_at": "TEXT",
        "live_updated_at": "TEXT",
    }
    for column, definition in additions.items():
        if column not in columns:
            db.execute(f"ALTER TABLE species ADD COLUMN {column} {definition}")
    db.execute(
        """
        UPDATE species
        SET live_status = COALESCE(live_status, status),
            live_tsv_path = COALESCE(live_tsv_path, tsv_path),
            live_metadata_status = COALESCE(live_metadata_status, metadata_status),
            live_metadata_path = COALESCE(live_metadata_path, metadata_path),
            live_metadata_clean_path = COALESCE(live_metadata_clean_path, metadata_clean_path),
            live_genome_count = COALESCE(live_genome_count, genome_count),
            live_taxon_id = COALESCE(live_taxon_id, taxon_id),
            live_last_synced_at = COALESCE(live_last_synced_at, last_synced_at),
            live_metadata_last_built_at = COALESCE(live_metadata_last_built_at, metadata_last_built_at),
            live_updated_at = COALESCE(live_updated_at, updated_at)
        WHERE is_live = 1
          AND (
              live_status IS NULL
              OR (tsv_path IS NOT NULL AND live_tsv_path IS NULL)
              OR (metadata_clean_path IS NOT NULL AND live_metadata_clean_path IS NULL)
          )
        """
    )
    db.commit()


def ensure_metadata_chunk_table(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            claimed_by TEXT,
            claimed_at TEXT,
            total_rows INTEGER NOT NULL DEFAULT 0,
            completed_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (species_id) REFERENCES species (id),
            UNIQUE(species_id, chunk_index)
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_chunks_status
        ON metadata_chunks (status, species_id, chunk_index);

        CREATE INDEX IF NOT EXISTS idx_metadata_chunks_species
        ON metadata_chunks (species_id, status);
        """
    )
    db.commit()


def ensure_standardization_refresh_table(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS standardization_refresh_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_id INTEGER NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            requested_at TEXT NOT NULL,
            claimed_by TEXT,
            claimed_at TEXT,
            completed_at TEXT,
            total_rows INTEGER NOT NULL DEFAULT 0,
            updated_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            FOREIGN KEY (species_id) REFERENCES species (id)
        );

        CREATE INDEX IF NOT EXISTS idx_standardization_refresh_tasks_status
        ON standardization_refresh_tasks (status, requested_at);

        CREATE TABLE IF NOT EXISTS standardization_refresh_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            species_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            claimed_by TEXT,
            claimed_at TEXT,
            completed_at TEXT,
            total_rows INTEGER NOT NULL DEFAULT 0,
            updated_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            FOREIGN KEY (task_id) REFERENCES standardization_refresh_tasks (id),
            FOREIGN KEY (species_id) REFERENCES species (id),
            UNIQUE(task_id, chunk_index)
        );

        CREATE INDEX IF NOT EXISTS idx_standardization_refresh_chunks_status
        ON standardization_refresh_chunks (status, task_id, chunk_index);

        CREATE INDEX IF NOT EXISTS idx_standardization_refresh_chunks_task
        ON standardization_refresh_chunks (task_id, status);
        """
    )
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
    for key, value in [
        ("dataset_pipeline_enabled", "0"),
        ("dataset_pipeline_discovery_schedule_enabled", "0"),
        ("dataset_pipeline_discovery_interval_days", "7"),
        ("dataset_pipeline_schedule_hour_utc", "18"),
        ("dataset_pipeline_scope", "2"),
        ("dataset_pipeline_auto_publish_insights", "1"),
        ("dataset_pipeline_catalog_sequential", "1"),
        ("dataset_pipeline_metadata_sequential", "1"),
        ("dataset_pipeline_standardization_sequential", "1"),
        ("dataset_pipeline_replace_sequential", "0"),
        ("dataset_pipeline_global_insights_sequential", "1"),
        ("active_dataset_version_id", "legacy-live"),
        ("previous_dataset_version_id", ""),
    ]:
        db.execute(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (key, value, now),
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
    suffix = Path(filename).suffix.lower()
    return suffix == expected_extension and suffix in ALLOWED_UPLOAD_EXTENSIONS


def safe_upload_name(filename: str, expected_extension: str) -> str:
    if not allowed_extension(filename, expected_extension):
        abort(400, f"Unsupported upload type. Expected {expected_extension}.")
    safe_name = secure_filename(filename)
    if not safe_name or Path(safe_name).name != safe_name:
        abort(400, "Invalid upload filename.")
    if Path(safe_name).suffix.lower() != expected_extension:
        abort(400, f"Unsupported upload type. Expected {expected_extension}.")
    return safe_name


def save_validated_upload(uploaded: Any, input_path: Path) -> None:
    uploaded.save(input_path)
    try:
        size = input_path.stat().st_size
    except FileNotFoundError:
        abort(400, "Upload could not be saved.")
    if size <= 0:
        input_path.unlink(missing_ok=True)
        abort(400, "Uploaded file is empty.")
    if size > MAX_UPLOAD_BYTES:
        input_path.unlink(missing_ok=True)
        abort(413, f"Uploaded file exceeds the {MAX_UPLOAD_BYTES} byte limit.")


def normalize_username(value: str) -> str:
    return value.strip().lower()


def normalize_email(value: str) -> str:
    return value.strip().lower()


def normalize_species_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def species_search_name(value: str) -> str:
    return normalize_species_name(value).lower()


NON_CANONICAL_SPECIES_TOKENS = {
    "sp",
    "sp.",
    "spp",
    "spp.",
    "bacterium",
    "archaeon",
    "microorganism",
    "metagenome",
    "uncultured",
    "unclassified",
    "endosymbiont",
    "symbiont",
}


def canonical_species_from_organism_name(value: str, expected_genus: str | None = None) -> str | None:
    name = normalize_species_name(value)
    if not name:
        return None
    parts = name.split()
    if not parts:
        return None
    offset = 0
    if parts[0].lower() == "candidatus":
        offset = 1
    if len(parts) < offset + 2:
        return None
    genus = parts[offset].strip()
    species_epithet = parts[offset + 1].strip()
    if expected_genus and genus.lower() != normalize_species_name(expected_genus).lower():
        return None
    cleaned_epithet = species_epithet.rstrip(".,;:").lower()
    if cleaned_epithet in NON_CANONICAL_SPECIES_TOKENS:
        return None
    if not re.match(r"^[a-z][a-z0-9-]*$", cleaned_epithet):
        return None
    if offset:
        return " ".join([parts[0], genus, species_epithet])
    return " ".join([genus, species_epithet])


def refresh_metadata_species_search_entries(species: SpeciesRecord, rows: list[dict[str, Any]]) -> None:
    if species.taxon_rank != "genus" or not rows:
        return
    genus_prefix = f"{species_search_name(species.species_name)} "
    counts: Counter[str] = Counter()
    for row in rows:
        organism_name = normalize_species_name(str(row.get("Organism Name") or ""))
        if not organism_name:
            continue
        search_name = species_search_name(organism_name)
        if not search_name.startswith(genus_prefix):
            continue
        if search_name == species_search_name(species.species_name):
            continue
        counts[organism_name] += 1

    now = utc_now()
    records = [
        (species.id, species.species_name, organism_name, species_search_name(organism_name), count, now)
        for organism_name, count in counts.items()
    ]
    with get_sqlite_connection() as db:
        if species.staging_dataset_version_id:
            dataset_version_id = species.staging_dataset_version_id
            db.execute(
                "DELETE FROM metadata_species_search_staging WHERE dataset_version_id = ? AND source_taxon_id = ?",
                (dataset_version_id, species.id),
            )
            if records:
                db.executemany(
                    """
                    INSERT INTO metadata_species_search_staging (
                        dataset_version_id, source_taxon_id, source_taxon_name, species_name,
                        search_name, genome_count, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(dataset_version_id, source_taxon_id, species_name) DO UPDATE SET
                        source_taxon_name = excluded.source_taxon_name,
                        search_name = excluded.search_name,
                        genome_count = excluded.genome_count,
                        updated_at = excluded.updated_at
                    """,
                    ((dataset_version_id, *record) for record in records),
                )
        else:
            db.execute("DELETE FROM metadata_species_search WHERE source_taxon_id = ?", (species.id,))
            if records:
                db.executemany(
                    """
                    INSERT INTO metadata_species_search (
                        source_taxon_id, source_taxon_name, species_name, search_name, genome_count, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_taxon_id, species_name) DO UPDATE SET
                        source_taxon_name = excluded.source_taxon_name,
                        search_name = excluded.search_name,
                        genome_count = excluded.genome_count,
                        updated_at = excluded.updated_at
                    """,
                    records,
                )
        db.commit()


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


DATASET_PIPELINE_STEPS = [
    ("discovery", "Discover bacterial genera"),
    ("catalog", "Update genus and species catalog"),
    ("metadata", "Update metadata build"),
    ("standardization", "Standardize newly updated metadata"),
    ("verify", "Verify staged update"),
    ("replace", "Replace existing data"),
    ("global_insights", "Generate Global Insights"),
]

DATASET_PIPELINE_STEP_COPY = {
    "discovery": {
        "short": "Find bacterial genera first; species are expanded later from genus catalogs and metadata.",
        "run_label": "Run discovery now",
        "metric_label": "Discovery scopes",
    },
    "catalog": {
        "short": "Refresh genus TSV catalogs first, then species catalogs.",
        "run_label": "Run catalog now",
        "metric_label": "Catalog ready",
    },
    "metadata": {
        "short": "Build genus metadata first, then derive/build species metadata.",
        "run_label": "Run metadata now",
        "metric_label": "Metadata ready",
    },
    "standardization": {
        "short": "Apply metadata standardization to newly updated metadata rows.",
        "run_label": "Run standardization now",
        "metric_label": "Standardization",
    },
    "verify": {
        "short": "Confirm no update work is active or failed before any live dataset switch is allowed.",
        "run_label": "Verify staged update",
        "metric_label": "Verification",
    },
    "replace": {
        "short": "Switch live pointers only after the upstream update pipeline has completed successfully.",
        "run_label": "Replace after verified run",
        "metric_label": "Live version",
    },
    "global_insights": {
        "short": "Regenerate the Global Insights snapshot from the active metadata.",
        "run_label": "Generate insights now",
        "metric_label": "Insights snapshot",
    },
}

DATASET_PIPELINE_SEQUENTIAL_KEYS = {
    "catalog": "dataset_pipeline_catalog_sequential",
    "metadata": "dataset_pipeline_metadata_sequential",
    "standardization": "dataset_pipeline_standardization_sequential",
    "replace": "dataset_pipeline_replace_sequential",
    "global_insights": "dataset_pipeline_global_insights_sequential",
}


def dataset_versions_root() -> Path:
    return DATA_DIR / DATASET_VERSIONS_DIR_NAME


def dataset_version_root(version_id: str) -> Path:
    return dataset_versions_root() / version_id


def get_active_dataset_version_id(db: sqlite3.Connection | None = None) -> str:
    return str(get_setting("active_dataset_version_id", "legacy-live", db) or "legacy-live")


def ensure_legacy_live_dataset_version(db: sqlite3.Connection | None = None) -> None:
    connection = db or get_db()
    existing = connection.execute(
        "SELECT version_id FROM dataset_versions WHERE version_id = ?",
        ("legacy-live",),
    ).fetchone()
    if existing is not None:
        return
    now = utc_now()
    summary = {
        "mode": "legacy",
        "note": "Initial live dataset registered from existing FetchM Web managed paths.",
    }
    connection.execute(
        """
        INSERT INTO dataset_versions (
            version_id, status, created_at, promoted_at, root_path, summary_json
        )
        VALUES (?, 'live', ?, ?, ?, ?)
        """,
        ("legacy-live", now, now, str(DATA_DIR), json.dumps(summary, sort_keys=True)),
    )
    if get_setting("active_dataset_version_id", None, connection) is None:
        set_setting("active_dataset_version_id", "legacy-live", connection)
    connection.commit()


def latest_dataset_update_run(db: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    connection = db or get_db()
    row = connection.execute(
        """
        SELECT *
        FROM dataset_update_pipeline_runs
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row is not None else None


def active_dataset_update_run(db: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    connection = db or get_db()
    row = connection.execute(
        """
        SELECT *
        FROM dataset_update_pipeline_runs
        WHERE status IN ('pending', 'running', 'blocked', 'paused')
        ORDER BY requested_at ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row is not None else None


def active_pipeline_dataset_version_for_step(step_key: str, db: sqlite3.Connection | None = None) -> str | None:
    connection = db or get_db()
    row = connection.execute(
        """
        SELECT r.dataset_version_id
        FROM dataset_update_pipeline_runs r
        JOIN dataset_update_pipeline_steps s ON s.run_id = r.run_id
        WHERE r.status IN ('pending', 'running')
          AND s.step_key = ?
          AND s.status IN ('pending', 'running')
        ORDER BY r.requested_at ASC, r.id ASC
        LIMIT 1
        """,
        (step_key,),
    ).fetchone()
    return str(row["dataset_version_id"]) if row is not None else None


def active_pipeline_dataset_version(db: sqlite3.Connection | None = None) -> str | None:
    run = active_dataset_update_run(db)
    return str(run["dataset_version_id"]) if run is not None and run.get("dataset_version_id") else None


def dataset_pipeline_step_keys() -> list[str]:
    return [step_key for step_key, _label in DATASET_PIPELINE_STEPS]


def dataset_pipeline_step_label(step_key: str) -> str:
    return dict(DATASET_PIPELINE_STEPS).get(step_key, step_key.replace("_", " ").title())


def normalize_dataset_pipeline_step(value: str | None) -> str:
    candidate = str(value or "discovery").strip().lower()
    return candidate if candidate in dataset_pipeline_step_keys() else "discovery"


def dataset_pipeline_sequential_enabled(step_key: str, db: sqlite3.Connection | None = None) -> bool:
    setting_key = DATASET_PIPELINE_SEQUENTIAL_KEYS.get(step_key)
    if setting_key is None:
        return True
    default = "0" if step_key == "replace" else "1"
    return get_setting(setting_key, default, db) == "1"


def dataset_pipeline_steps_for_start(start_step: str, db: sqlite3.Connection | None = None) -> list[str]:
    ordered = dataset_pipeline_step_keys()
    start = normalize_dataset_pipeline_step(start_step)
    start_index = ordered.index(start)
    selected = [start]
    for step_key in ordered[start_index + 1 :]:
        if not dataset_pipeline_sequential_enabled(step_key, db):
            break
        selected.append(step_key)
    return selected


def dataset_update_steps(run_id: str, db: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    connection = db or get_db()
    rows = connection.execute(
        """
        SELECT *
        FROM dataset_update_pipeline_steps
        WHERE run_id = ?
        ORDER BY step_order ASC
        """,
        (run_id,),
    ).fetchall()
    steps: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["progress"] = json.loads(str(item.get("progress_json") or "{}"))
        except json.JSONDecodeError:
            item["progress"] = {}
        try:
            item["blockers"] = json.loads(str(item.get("blockers_json") or "[]"))
        except json.JSONDecodeError:
            item["blockers"] = []
        item["label"] = dataset_pipeline_step_label(str(item["step_key"]))
        steps.append(item)
    return steps


def queue_dataset_update_pipeline_run(
    trigger_type: str,
    requested_by: int | None = None,
    *,
    start_step: str = "discovery",
) -> tuple[str | None, str | None]:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        active = active_dataset_update_run(db)
        if active is not None:
            db.commit()
            return None, f"Pipeline run {active['run_id']} is already {active['status']}."
        now = utc_now()
        normalized_start = normalize_dataset_pipeline_step(start_step)
        selected_steps = dataset_pipeline_steps_for_start(normalized_start, db)
        run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{normalized_start}_update"
        version_id = f"{run_id}_staging"
        root = dataset_version_root(version_id)
        root.mkdir(parents=True, exist_ok=True)
        summary = {
            "scope": get_setting("dataset_pipeline_scope", "2", db),
            "start_step": normalized_start,
            "step_keys": selected_steps,
            "discovery_interval_days": get_setting("dataset_pipeline_discovery_interval_days", "7", db),
            "schedule_hour_utc": get_setting("dataset_pipeline_schedule_hour_utc", "18", db),
            "auto_publish_insights": get_setting("dataset_pipeline_auto_publish_insights", "0", db) == "1",
        }
        db.execute(
            """
            INSERT INTO dataset_versions (
                version_id, status, created_at, root_path, summary_json
            )
            VALUES (?, 'staging', ?, ?, ?)
            """,
            (version_id, now, str(root), json.dumps(summary, sort_keys=True)),
        )
        db.execute(
            """
            INSERT INTO dataset_update_pipeline_runs (
                run_id, dataset_version_id, trigger_type, status, requested_by, requested_at, summary_json
            )
            VALUES (?, ?, ?, 'pending', ?, ?, ?)
            """,
            (run_id, version_id, trigger_type, requested_by, now, json.dumps(summary, sort_keys=True)),
        )
        step_rows = [
            (run_id, step_key, index, "pending" if index == 0 else "waiting", now if index == 0 else None)
            for index, step_key in enumerate(selected_steps)
        ]
        db.executemany(
            """
            INSERT INTO dataset_update_pipeline_steps (
                run_id, step_key, step_order, status, queued_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            step_rows,
        )
        db.commit()
        return run_id, None


def dataset_pipeline_rank_counts(db: sqlite3.Connection) -> dict[str, int]:
    catalog_rows = db.execute(
        """
        SELECT taxon_rank,
               COUNT(*) AS total,
               SUM(CASE WHEN status = 'ready' AND tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS ready,
               SUM(CASE WHEN status = 'no_data' THEN 1 ELSE 0 END) AS no_data,
               SUM(CASE WHEN status IN ('pending', 'syncing') OR refresh_requested = 1 OR claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        GROUP BY taxon_rank
        """
    ).fetchall()
    metadata_rows = db.execute(
        """
        SELECT taxon_rank,
               COUNT(*) AS total,
               SUM(CASE WHEN metadata_status = 'ready' AND metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS ready,
               SUM(CASE WHEN metadata_status IN ('pending', 'building') OR metadata_refresh_requested = 1 OR metadata_claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        WHERE status = 'ready'
          AND tsv_path IS NOT NULL
        GROUP BY taxon_rank
        """
    ).fetchall()
    counts: dict[str, int] = {}
    for prefix, rows in [("catalog", catalog_rows), ("metadata", metadata_rows)]:
        for row in rows:
            rank = str(row["taxon_rank"] or "unknown")
            counts[f"{prefix}_{rank}_total"] = int(row["total"] or 0)
            counts[f"{prefix}_{rank}_ready"] = int(row["ready"] or 0)
            if prefix == "catalog":
                counts[f"{prefix}_{rank}_no_data"] = int(row["no_data"] or 0)
            counts[f"{prefix}_{rank}_active"] = int(row["active"] or 0)
            counts[f"{prefix}_{rank}_failed"] = int(row["failed"] or 0)
    discovery = db.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) AS ready,
               SUM(CASE WHEN status IN ('pending', 'discovering') OR refresh_requested = 1 THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
               COALESCE(SUM(discovered_species_count), 0) AS discovered
        FROM discovery_scopes
        """
    ).fetchone()
    counts.update(
        {
            "discovery_total": int(discovery["total"] or 0),
            "discovery_ready": int(discovery["ready"] or 0),
            "discovery_active": int(discovery["active"] or 0),
            "discovery_failed": int(discovery["failed"] or 0),
            "discovery_found_total": int(discovery["discovered"] or 0),
        }
    )
    standardization = db.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status IN ('pending', 'running', 'chunking', 'finalizing') THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
               COALESCE(SUM(total_rows), 0) AS total_rows,
               COALESCE(SUM(updated_rows), 0) AS updated_rows
        FROM standardization_refresh_tasks
        """
    ).fetchone()
    counts.update(
        {
            "standardization_total": int(standardization["total"] or 0),
            "standardization_active": int(standardization["active"] or 0),
            "standardization_completed": int(standardization["completed"] or 0),
            "standardization_failed": int(standardization["failed"] or 0),
            "standardization_total_rows": int(standardization["total_rows"] or 0),
            "standardization_updated_rows": int(standardization["updated_rows"] or 0),
        }
    )
    insights = latest_global_insight_task(db)
    counts["global_insights_active"] = 1 if insights and str(insights.get("status")) in {"pending", "running"} else 0
    counts["global_insights_completed"] = 1 if insights and str(insights.get("status")) == "completed" else 0
    return counts


def progress_percent(done: int, total: int) -> int:
    if total <= 0:
        return 0
    return max(0, min(100, int(round((done / total) * 100))))


def build_dataset_pipeline_step_cards(
    db: sqlite3.Connection,
    step_rows_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    counts = dataset_pipeline_rank_counts(db)
    active_version = get_active_dataset_version_id(db)
    latest_insight = latest_global_insight_task(db)
    discovery_status_rows = db.execute(
        """
        SELECT status, COUNT(*) AS total
        FROM discovery_scopes
        GROUP BY status
        """
    ).fetchall()
    discovery_status_counts = {
        str(row["status"]): int(row["total"] or 0)
        for row in discovery_status_rows
    }
    active_discovery_scope = db.execute(
        """
        SELECT scope_label, scope_value, target_rank, claimed_at, updated_at
        FROM discovery_scopes
        WHERE status = 'discovering'
        ORDER BY claimed_at ASC, updated_at ASC
        LIMIT 1
        """
    ).fetchone()
    latest_ready_discovery_scope = db.execute(
        """
        SELECT scope_label, target_rank, discovered_species_count, updated_at
        FROM discovery_scopes
        WHERE status = 'ready'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    cards: list[dict[str, Any]] = []
    for index, (step_key, label) in enumerate(DATASET_PIPELINE_STEPS, start=1):
        row = step_rows_by_key.get(step_key)
        status = str(row["status"]) if row else "idle"
        progress = row.get("progress", {}) if row else {}
        detail_lines: list[str] = []
        percent = 0
        if step_key == "discovery":
            total = counts.get("discovery_total", 0)
            ready = counts.get("discovery_ready", 0)
            percent = progress_percent(ready, total)
            pending = discovery_status_counts.get("pending", 0)
            discovering = discovery_status_counts.get("discovering", 0)
            failed = discovery_status_counts.get("failed", 0)
            genus_before = int(progress.get("genus_before") or counts.get("catalog_genus_total", 0))
            new_genera = max(0, counts.get("catalog_genus_total", 0) - genus_before)
            detail_lines = [
                f"{ready}/{total} scopes completed; {discovering} running, {pending} queued",
                f"{new_genera} new genera found in latest run",
            ]
            if active_discovery_scope is not None:
                active_label = str(active_discovery_scope["scope_label"] or active_discovery_scope["scope_value"])
                detail_lines.append(
                    f"Active now: {active_label} ({active_discovery_scope['target_rank']})"
                )
            if latest_ready_discovery_scope is not None:
                latest_label = str(latest_ready_discovery_scope["scope_label"] or "discovery scope")
                discovered_count = int(latest_ready_discovery_scope["discovered_species_count"] or 0)
                detail_lines.append(f"Last completed: {latest_label}, {discovered_count} taxa found")
            if failed:
                detail_lines.append(f"{failed} discovery scopes failed")
            if status == "running":
                detail_lines.append("Species expansion waits until genus catalogs and genus metadata are ready.")
        elif step_key == "catalog":
            genus_ready = counts.get("catalog_genus_ready", 0)
            genus_no_data = counts.get("catalog_genus_no_data", 0)
            genus_total = counts.get("catalog_genus_total", 0)
            species_ready = counts.get("catalog_species_ready", 0)
            species_no_data = counts.get("catalog_species_no_data", 0)
            species_total = counts.get("catalog_species_total", 0)
            genus_resolved = genus_ready + genus_no_data
            species_resolved = species_ready + species_no_data
            percent = progress_percent(genus_resolved + species_resolved, genus_total + species_total)
            detail_lines = [
                f"{genus_resolved}/{genus_total} genus catalog scopes resolved ({genus_ready} ready, {genus_no_data} no genome data)",
                f"{species_resolved}/{species_total} species catalog scopes resolved ({species_ready} ready, {species_no_data} no genome data)",
            ]
        elif step_key == "metadata":
            genus_ready = counts.get("metadata_genus_ready", 0)
            genus_total = counts.get("metadata_genus_total", 0)
            species_ready = counts.get("metadata_species_ready", 0)
            species_total = counts.get("metadata_species_total", 0)
            percent = progress_percent(genus_ready + species_ready, genus_total + species_total)
            detail_lines = [
                f"{genus_ready}/{genus_total} genus metadata builds ready",
                f"{species_ready}/{species_total} species metadata builds ready",
            ]
        elif step_key == "standardization":
            completed = counts.get("standardization_completed", 0)
            total = counts.get("standardization_total", 0)
            percent = progress_percent(completed, total)
            detail_lines = [
                f"{counts.get('standardization_active', 0)} standardization tasks active",
                f"{counts.get('standardization_updated_rows', 0)} standardized rows updated",
            ]
        elif step_key == "verify":
            active_total = (
                counts.get("discovery_active", 0)
                + counts.get("catalog_active", 0)
                + counts.get("metadata_active", 0)
                + counts.get("standardization_active", 0)
            )
            failed_total = (
                counts.get("discovery_failed", 0)
                + counts.get("catalog_failed", 0)
                + counts.get("metadata_failed", 0)
                + counts.get("standardization_failed", 0)
            )
            percent = 100 if status == "completed" else 0
            detail_lines = [
                f"{active_total} update tasks still active",
                f"{failed_total} update tasks failed",
                "Replacement remains blocked until verification completes.",
            ]
        elif step_key == "replace":
            percent = 100 if active_version != "legacy-live" else 0
            detail_lines = [
                f"Active dataset: {active_version}",
                "Replacement is blocked unless upstream steps in the same run completed successfully.",
            ]
        elif step_key == "global_insights":
            percent = 100 if latest_insight and str(latest_insight.get("status")) == "completed" else 0
            if latest_insight:
                detail_lines = [
                    f"Latest snapshot: {latest_insight.get('snapshot_id')}",
                    f"Status: {latest_insight.get('status')}",
                ]
            else:
                detail_lines = ["No Global Insights snapshot task has run yet."]
        if status == "completed":
            percent = 100
        elif status in {"running", "pending"} and progress:
            if step_key == "standardization" and progress.get("queued"):
                total = int(progress.get("eligible") or progress.get("queued") or 0)
                done = max(0, total - counts.get("standardization_active", 0))
                percent = progress_percent(done, total)
        cards.append(
            {
                "key": step_key,
                "index": index,
                "label": label,
                "short": DATASET_PIPELINE_STEP_COPY[step_key]["short"],
                "run_label": DATASET_PIPELINE_STEP_COPY[step_key]["run_label"],
                "metric_label": DATASET_PIPELINE_STEP_COPY[step_key]["metric_label"],
                "status": status,
                "percent": percent,
                "details": detail_lines,
                "row": row,
                "sequential_enabled": dataset_pipeline_sequential_enabled(step_key, db),
                "has_sequential_control": step_key in DATASET_PIPELINE_SEQUENTIAL_KEYS,
            }
        )
    return cards


def build_dataset_pipeline_dashboard(db: sqlite3.Connection | None = None) -> dict[str, Any]:
    connection = db or get_db()
    ensure_legacy_live_dataset_version(connection)
    active_version_id = get_active_dataset_version_id(connection)
    live = connection.execute(
        "SELECT * FROM dataset_versions WHERE version_id = ?",
        (active_version_id,),
    ).fetchone()
    staging = connection.execute(
        """
        SELECT *
        FROM dataset_versions
        WHERE status IN ('staging', 'verified', 'failed')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_run = latest_dataset_update_run(connection)
    active_run = active_dataset_update_run(connection)
    run_for_steps = active_run or latest_run
    steps = dataset_update_steps(str(run_for_steps["run_id"]), connection) if run_for_steps else []
    step_rows_by_key = {str(step["step_key"]): step for step in steps}
    step_cards = build_dataset_pipeline_step_cards(connection, step_rows_by_key)
    discovery_schedule_enabled = get_setting("dataset_pipeline_discovery_schedule_enabled", "0", connection) == "1"
    discovery_interval_days = get_setting("dataset_pipeline_discovery_interval_days", "7", connection)
    schedule_hour_utc = get_setting("dataset_pipeline_schedule_hour_utc", "18", connection)
    return {
        "enabled": get_setting("dataset_pipeline_enabled", "0", connection) == "1",
        "discovery_schedule_enabled": discovery_schedule_enabled,
        "discovery_interval_days": discovery_interval_days,
        "schedule_hour_utc": schedule_hour_utc,
        "scope": get_setting("dataset_pipeline_scope", "2", connection),
        "auto_publish_insights": get_setting("dataset_pipeline_auto_publish_insights", "0", connection) == "1",
        "sequential": {
            step_key: dataset_pipeline_sequential_enabled(step_key, connection)
            for step_key in DATASET_PIPELINE_SEQUENTIAL_KEYS
        },
        "live_version": dict(live) if live is not None else {"version_id": active_version_id, "status": "live"},
        "staging_version": dict(staging) if staging is not None else None,
        "latest_run": latest_run,
        "active_run": active_run,
        "steps": steps,
        "step_cards": step_cards,
        "can_run_now": active_run is None,
        "can_promote": bool(staging is not None and str(staging["status"]) == "verified"),
        "can_rollback": bool(get_setting("previous_dataset_version_id", "", connection)),
    }


def scheduled_dataset_pipeline_due_at(db: sqlite3.Connection | None = None) -> datetime:
    connection = db or get_db()
    try:
        interval_days = max(1, int(get_setting("dataset_pipeline_discovery_interval_days", "7", connection) or "7"))
    except ValueError:
        interval_days = 7
    try:
        hour = max(0, min(23, int(get_setting("dataset_pipeline_schedule_hour_utc", "18", connection) or "18")))
    except ValueError:
        hour = 18
    now = datetime.now(timezone.utc)
    latest = connection.execute(
        """
        SELECT requested_at
        FROM dataset_update_pipeline_runs
        WHERE trigger_type = 'scheduled'
          AND summary_json LIKE '%"start_step": "discovery"%'
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if latest is not None:
        try:
            return parse_utc(str(latest["requested_at"])) + timedelta(days=interval_days)
        except ValueError:
            pass
    candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate > now:
        candidate -= timedelta(days=1)
    return candidate


def schedule_due_dataset_pipeline_run() -> None:
    with get_sqlite_connection() as db:
        if get_setting("dataset_pipeline_discovery_schedule_enabled", "0", db) != "1":
            return
        if active_dataset_update_run(db) is not None:
            return
        due = scheduled_dataset_pipeline_due_at(db)
        try:
            hour = int(get_setting("dataset_pipeline_schedule_hour_utc", "18", db) or "18")
        except ValueError:
            hour = 18
        now = datetime.now(timezone.utc)
        if now < due or now.hour < hour:
            return
    queue_dataset_update_pipeline_run("scheduled", None, start_step="discovery")


def global_insights_root() -> Path:
    return DATA_DIR / GLOBAL_INSIGHTS_DIR_NAME


def global_insights_latest_path() -> Path:
    return global_insights_root() / "latest.json"


def load_latest_global_insights_summary() -> tuple[dict[str, Any] | None, Path | None]:
    latest_path = global_insights_latest_path()
    if not latest_path.exists():
        return None, None
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        summary_path = Path(str(latest.get("summary_path") or ""))
        if not summary_path.is_absolute():
            summary_path = global_insights_root() / summary_path
        root = global_insights_root().resolve()
        resolved_summary = summary_path.resolve()
        if root not in resolved_summary.parents and resolved_summary != root:
            return None, None
        if not resolved_summary.exists():
            return None, None
        summary = json.loads(resolved_summary.read_text(encoding="utf-8"))
        return summary, resolved_summary.parent
    except Exception:
        logging.exception("Failed to load latest Global Insights snapshot.")
        return None, None


def latest_global_insight_task(db: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    connection = db or get_db()
    row = connection.execute(
        """
        SELECT *
        FROM global_insight_tasks
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row is not None else None


def global_insight_generation_blockers(db: sqlite3.Connection | None = None) -> list[str]:
    connection = db or get_db()
    blockers: list[str] = []
    discovery_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM discovery_scopes
        WHERE status IN ('pending', 'discovering')
           OR refresh_requested = 1
        """
    ).fetchone()
    catalog_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM species
        WHERE status IN ('pending', 'syncing')
           OR refresh_requested = 1
           OR claimed_at IS NOT NULL
        """
    ).fetchone()
    metadata_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM species
        WHERE metadata_status IN ('pending', 'building')
           OR metadata_refresh_requested = 1
           OR metadata_claimed_at IS NOT NULL
        """
    ).fetchone()
    standardization_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM standardization_refresh_tasks
        WHERE status IN ('pending', 'running', 'chunking', 'finalizing')
        """
    ).fetchone()
    standardization_chunks_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM standardization_refresh_chunks
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    insight_active = connection.execute(
        """
        SELECT COUNT(*) AS total
        FROM global_insight_tasks
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    if int(discovery_active["total"] or 0):
        blockers.append("discovery is still active")
    if int(catalog_active["total"] or 0):
        blockers.append("catalog syncs are still active")
    if int(metadata_active["total"] or 0):
        blockers.append("metadata builds or refreshes are still active")
    if int(standardization_active["total"] or 0) or int(standardization_chunks_active["total"] or 0):
        blockers.append("standardization refresh is still active")
    if int(insight_active["total"] or 0):
        blockers.append("a Global Insights generation task is already queued or running")
    return blockers


def queue_global_insights_generation(requested_by: int | None, *, demo: bool = False) -> tuple[str | None, list[str]]:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        blockers = [] if demo else global_insight_generation_blockers(db)
        if blockers:
            db.commit()
            return None, blockers
        snapshot_id = (
            "demo_global_insights"
            if demo
            else f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_global_insights"
        )
        if demo:
            db.execute("DELETE FROM global_insight_tasks WHERE snapshot_id = ?", (snapshot_id,))
        db.execute(
            """
            INSERT INTO global_insight_tasks (
                snapshot_id, status, requested_by, requested_at, is_demo
            )
            VALUES (?, 'pending', ?, ?, ?)
            """,
            (snapshot_id, requested_by, utc_now(), 1 if demo else 0),
        )
        db.commit()
        return snapshot_id, []


def claim_next_global_insight_task(worker_name: str) -> sqlite3.Row | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        stale_rows = db.execute(
            """
            SELECT id, claimed_by
            FROM global_insight_tasks
            WHERE status = 'running'
              AND claimed_by IS NOT NULL
            """
        ).fetchall()
        for row in stale_rows:
            claimed_by = str(row["claimed_by"] or "")
            if claimed_by and worker_heartbeat_is_live(claimed_by):
                continue
            db.execute(
                """
                UPDATE global_insight_tasks
                SET status = 'pending',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    error = 'Stale Global Insights claim from a dead worker was reset.'
                WHERE id = ?
                """,
                (row["id"],),
            )
        task = db.execute(
            """
            SELECT *
            FROM global_insight_tasks
            WHERE status = 'pending'
            ORDER BY requested_at ASC, id ASC
            LIMIT 1
            """
        ).fetchone()
        if task is None:
            db.commit()
            return None
        claimed_at = utc_now()
        cursor = db.execute(
            """
            UPDATE global_insight_tasks
            SET status = 'running',
                claimed_by = ?,
                claimed_at = ?,
                error = NULL
            WHERE id = ?
              AND status = 'pending'
            """,
            (worker_name, claimed_at, task["id"]),
        )
        if int(cursor.rowcount or 0) != 1:
            db.commit()
            return None
        db.commit()
        return task


def process_global_insight_task(task: sqlite3.Row) -> None:
    task_id = int(task["id"])
    snapshot_id = str(task["snapshot_id"])
    is_demo = bool(task["is_demo"])
    try:
        with get_sqlite_connection() as db:
            rows = db.execute(
                """
                SELECT id, species_name, taxon_rank,
                       live_genome_count AS genome_count,
                       live_metadata_clean_path AS metadata_clean_path,
                       live_last_synced_at AS last_synced_at
                FROM species
                WHERE is_live = 1
                  AND live_metadata_status = 'ready'
                  AND live_metadata_clean_path IS NOT NULL
                """
            ).fetchall()
        taxa = [
            {
                "id": int(row["id"]),
                "species_name": str(row["species_name"]),
                "taxon_rank": str(row["taxon_rank"]),
                "genome_count": row["genome_count"],
                "metadata_clean_path": str(row["metadata_clean_path"] or ""),
                "last_synced_at": str(row["last_synced_at"] or ""),
            }
            for row in rows
            if row["metadata_clean_path"]
        ]
        if is_demo:
            summary = generate_demo_snapshot(
                global_insights_root(),
                app_version=APP_VERSION,
                app_commit=APP_COMMIT,
                snapshot_id=snapshot_id,
            )
        else:
            summary = generate_global_insights_snapshot(
                taxa,
                global_insights_root(),
                app_version=APP_VERSION,
                app_commit=APP_COMMIT,
                snapshot_id=snapshot_id,
            )
        output_dir = str(global_insights_root() / "snapshots" / snapshot_id)
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE global_insight_tasks
                SET status = 'completed',
                    completed_at = ?,
                    output_dir = ?,
                    error = NULL
                WHERE id = ?
                """,
                (utc_now(), output_dir, task_id),
            )
            set_setting("global_insights_latest_snapshot_id", str(summary.get("snapshot_id") or snapshot_id), db)
            db.commit()
    except Exception as exc:
        logging.exception("Global Insights generation failed for %s.", snapshot_id)
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE global_insight_tasks
                SET status = 'failed',
                    completed_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (utc_now(), str(exc)[:4000], task_id),
            )
            db.commit()
        raise


def dataset_update_active_counts(db: sqlite3.Connection) -> dict[str, int]:
    discovery = db.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('pending', 'discovering') THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM discovery_scopes
        """
    ).fetchone()
    catalog = db.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'pending' OR status = 'syncing' OR refresh_requested = 1 OR claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        """
    ).fetchone()
    metadata = db.execute(
        """
        SELECT
            SUM(CASE WHEN metadata_status IN ('pending', 'building') OR metadata_refresh_requested = 1 OR metadata_claimed_at IS NOT NULL THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        WHERE status = 'ready'
          AND tsv_path IS NOT NULL
        """
    ).fetchone()
    standardization_tasks = db.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('pending', 'running', 'chunking', 'finalizing') THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM standardization_refresh_tasks
        """
    ).fetchone()
    standardization_chunks = db.execute(
        """
        SELECT
            SUM(CASE WHEN status IN ('pending', 'running') THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM standardization_refresh_chunks
        """
    ).fetchone()
    ready_taxa = db.execute(
        """
        SELECT
            COUNT(*) AS taxa,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS metadata_taxa,
            COALESCE(SUM(genome_count), 0) AS taxon_scoped_genomes
        FROM species
        WHERE status = 'ready'
        """
    ).fetchone()
    return {
        "discovery_active": int(discovery["active"] or 0),
        "discovery_failed": int(discovery["failed"] or 0),
        "catalog_active": int(catalog["active"] or 0),
        "catalog_failed": int(catalog["failed"] or 0),
        "metadata_active": int(metadata["active"] or 0),
        "metadata_failed": int(metadata["failed"] or 0),
        "standardization_active": int(standardization_tasks["active"] or 0) + int(standardization_chunks["active"] or 0),
        "standardization_failed": int(standardization_tasks["failed"] or 0) + int(standardization_chunks["failed"] or 0),
        "ready_taxa": int(ready_taxa["taxa"] or 0),
        "metadata_taxa": int(ready_taxa["metadata_taxa"] or 0),
        "taxon_scoped_genomes": int(ready_taxa["taxon_scoped_genomes"] or 0),
    }


def request_pipeline_discovery_refresh(db: sqlite3.Connection) -> dict[str, Any]:
    now = utc_now()
    scope = get_setting("dataset_pipeline_scope", "2", db) or "2"
    root_scope = create_discovery_scope(
        str(scope),
        label="Bacteria genera",
        assembly_source=DEFAULT_ASSEMBLY_SOURCE,
        target_rank="genus",
        db=db,
    )
    cursor = db.execute(
        """
        UPDATE discovery_scopes
        SET status = 'pending',
            refresh_requested = 1,
            updated_at = ?,
            last_error = NULL,
            claimed_by = NULL,
            claimed_at = NULL
        WHERE id = ?
          AND status != 'discovering'
        """,
        (now, root_scope.id),
    )
    counts = dataset_pipeline_rank_counts(db)
    return {
        "queued": int(cursor.rowcount or 0),
        "active": counts.get("discovery_active", 0),
        "ready": counts.get("discovery_ready", 0),
        "total": counts.get("discovery_total", 0),
    }


def request_pipeline_catalog_syncs(db: sqlite3.Connection, rank: str, dataset_version_id: str | None = None) -> dict[str, Any]:
    rank = normalize_taxon_rank(rank)
    now = utc_now()
    cursor = db.execute(
        """
        UPDATE species
        SET status = CASE WHEN tsv_path IS NULL THEN 'pending' ELSE status END,
            refresh_requested = 1,
            staging_dataset_version_id = COALESCE(?, staging_dataset_version_id),
            updated_at = ?,
            sync_error = NULL,
            claimed_by = NULL,
            claimed_at = NULL,
            sync_attempt_count = CASE WHEN tsv_path IS NULL THEN 0 ELSE sync_attempt_count END,
            sync_first_claimed_at = CASE WHEN tsv_path IS NULL THEN NULL ELSE sync_first_claimed_at END
        WHERE taxon_rank = ?
          AND status != 'syncing'
          AND claimed_at IS NULL
        """,
        (dataset_version_id, now, rank),
    )
    counts = dataset_pipeline_rank_counts(db)
    return {
        "queued": int(cursor.rowcount or 0),
        "rank": rank,
        "active": counts.get(f"catalog_{rank}_active", 0),
        "ready": counts.get(f"catalog_{rank}_ready", 0),
        "total": counts.get(f"catalog_{rank}_total", 0),
    }


def request_pipeline_metadata_builds(db: sqlite3.Connection, rank: str, dataset_version_id: str | None = None) -> dict[str, Any]:
    rank = normalize_taxon_rank(rank)
    now = utc_now()
    cursor = db.execute(
        """
        UPDATE species
        SET metadata_status = CASE WHEN metadata_path IS NULL THEN 'pending' ELSE metadata_status END,
            metadata_refresh_requested = 1,
            staging_dataset_version_id = COALESCE(?, staging_dataset_version_id),
            metadata_error = NULL,
            metadata_claimed_by = NULL,
            metadata_claimed_at = NULL,
            metadata_attempt_count = CASE WHEN metadata_path IS NULL THEN 0 ELSE metadata_attempt_count END,
            metadata_first_claimed_at = CASE WHEN metadata_path IS NULL THEN NULL ELSE metadata_first_claimed_at END,
            updated_at = ?
        WHERE taxon_rank = ?
          AND status = 'ready'
          AND tsv_path IS NOT NULL
          AND metadata_status != 'building'
          AND metadata_claimed_at IS NULL
        """,
        (dataset_version_id, now, rank),
    )
    counts = dataset_pipeline_rank_counts(db)
    return {
        "queued": int(cursor.rowcount or 0),
        "rank": rank,
        "active": counts.get(f"metadata_{rank}_active", 0),
        "ready": counts.get(f"metadata_{rank}_ready", 0),
        "total": counts.get(f"metadata_{rank}_total", 0),
    }


def set_pipeline_step_status(
    db: sqlite3.Connection,
    step_id: int,
    status: str,
    *,
    progress: dict[str, Any] | None = None,
    blockers: list[str] | None = None,
    error: str | None = None,
) -> None:
    now = utc_now()
    fields = ["status = ?", "progress_json = ?", "blockers_json = ?", "error = ?"]
    params: list[Any] = [
        status,
        json.dumps(progress or {}, sort_keys=True),
        json.dumps(blockers or []),
        error,
    ]
    if status == "running":
        fields.append("started_at = COALESCE(started_at, ?)")
        params.append(now)
    if status in {"completed", "failed", "skipped"}:
        fields.append("completed_at = COALESCE(completed_at, ?)")
        params.append(now)
    params.append(step_id)
    db.execute(f"UPDATE dataset_update_pipeline_steps SET {', '.join(fields)} WHERE id = ?", tuple(params))


def queue_next_pipeline_step(db: sqlite3.Connection, run_id: str, completed_order: int) -> None:
    next_step = db.execute(
        """
        SELECT id
        FROM dataset_update_pipeline_steps
        WHERE run_id = ?
          AND step_order > ?
          AND status = 'waiting'
        ORDER BY step_order ASC
        LIMIT 1
        """,
        (run_id, completed_order),
    ).fetchone()
    if next_step is None:
        db.execute(
            """
            UPDATE dataset_update_pipeline_runs
            SET status = 'completed',
                completed_at = COALESCE(completed_at, ?)
            WHERE run_id = ?
              AND status IN ('pending', 'running')
            """,
            (utc_now(), run_id),
        )
        return
    db.execute(
        """
        UPDATE dataset_update_pipeline_steps
        SET status = 'pending',
            queued_at = COALESCE(queued_at, ?)
        WHERE id = ?
        """,
        (utc_now(), next_step["id"]),
    )


def mark_pipeline_step_completed(db: sqlite3.Connection, step: sqlite3.Row, progress: dict[str, Any] | None = None) -> None:
    set_pipeline_step_status(db, int(step["id"]), "completed", progress=progress)
    queue_next_pipeline_step(db, str(step["run_id"]), int(step["step_order"]))


def replacement_step_blockers(db: sqlite3.Connection, step: sqlite3.Row) -> list[str]:
    """Keep live data swaps gated behind a completed upstream pipeline."""
    blockers: list[str] = []
    run_id = str(step["run_id"])
    upstream = db.execute(
        """
        SELECT step_key, status
        FROM dataset_update_pipeline_steps
        WHERE run_id = ?
          AND step_order < ?
        ORDER BY step_order ASC
        """,
        (run_id, step["step_order"]),
    ).fetchall()
    required = {"discovery", "catalog", "metadata", "standardization", "verify"}
    upstream_by_key = {str(row["step_key"]): str(row["status"]) for row in upstream}
    missing = sorted(required.difference(upstream_by_key))
    incomplete = [
        dataset_pipeline_step_label(key)
        for key, status in upstream_by_key.items()
        if key in required and status != "completed"
    ]
    if missing:
        blockers.append(
            "Replacement must be queued after a full update run; missing upstream steps: "
            + ", ".join(dataset_pipeline_step_label(key) for key in missing)
        )
    if incomplete:
        blockers.append("Upstream steps are not complete: " + ", ".join(incomplete))
    counts = dataset_update_active_counts(db)
    active_parts = {
        "discovery": counts.get("discovery_active", 0),
        "catalog": counts.get("catalog_active", 0),
        "metadata": counts.get("metadata_active", 0),
        "standardization": counts.get("standardization_active", 0),
    }
    still_active = [f"{count} {label}" for label, count in active_parts.items() if int(count or 0)]
    if still_active:
        blockers.append("Update work is still active: " + ", ".join(still_active))
    failed_parts = {
        "discovery": counts.get("discovery_failed", 0),
        "catalog": counts.get("catalog_failed", 0),
        "metadata": counts.get("metadata_failed", 0),
        "standardization": counts.get("standardization_failed", 0),
    }
    failures = [f"{count} {label}" for label, count in failed_parts.items() if int(count or 0)]
    if failures:
        blockers.append("Resolve failed update tasks before replacement: " + ", ".join(failures))
    return blockers


def snapshot_live_species_state(db: sqlite3.Connection, dataset_version_id: str) -> int:
    now = utc_now()
    existing = db.execute(
        "SELECT COUNT(*) AS total FROM dataset_species_live_snapshots WHERE dataset_version_id = ?",
        (dataset_version_id,),
    ).fetchone()
    if int(existing["total"] or 0):
        return 0
    cursor = db.execute(
        """
        INSERT INTO dataset_species_live_snapshots (
            dataset_version_id, species_id, species_name, taxon_rank, is_live, live_status,
            live_tsv_path, live_metadata_status, live_metadata_path, live_metadata_clean_path,
            live_genome_count, live_taxon_id, live_last_synced_at, live_metadata_last_built_at,
            live_updated_at, captured_at
        )
        SELECT ?, id, species_name, taxon_rank, is_live, live_status,
               live_tsv_path, live_metadata_status, live_metadata_path, live_metadata_clean_path,
               live_genome_count, live_taxon_id, live_last_synced_at, live_metadata_last_built_at,
               live_updated_at, ?
        FROM species
        """,
        (dataset_version_id, now),
    )
    return int(cursor.rowcount or 0)


def promote_staged_species_state(db: sqlite3.Connection, dataset_version_id: str) -> int:
    now = utc_now()
    cursor = db.execute(
        """
        UPDATE species
        SET is_live = 1,
            live_status = status,
            live_tsv_path = tsv_path,
            live_metadata_status = metadata_status,
            live_metadata_path = metadata_path,
            live_metadata_clean_path = metadata_clean_path,
            live_genome_count = genome_count,
            live_taxon_id = taxon_id,
            live_last_synced_at = last_synced_at,
            live_metadata_last_built_at = metadata_last_built_at,
            live_updated_at = ?,
            staging_dataset_version_id = NULL
        WHERE staging_dataset_version_id = ?
        """,
        (now, dataset_version_id),
    )
    return int(cursor.rowcount or 0)


def restore_live_species_snapshot(db: sqlite3.Connection, dataset_version_id: str) -> int:
    snapshots = db.execute(
        """
        SELECT *
        FROM dataset_species_live_snapshots
        WHERE dataset_version_id = ?
        """,
        (dataset_version_id,),
    ).fetchall()
    restored = 0
    now = utc_now()
    for row in snapshots:
        cursor = db.execute(
            """
            UPDATE species
            SET is_live = ?,
                live_status = ?,
                live_tsv_path = ?,
                live_metadata_status = ?,
                live_metadata_path = ?,
                live_metadata_clean_path = ?,
                live_genome_count = ?,
                live_taxon_id = ?,
                live_last_synced_at = ?,
                live_metadata_last_built_at = ?,
                live_updated_at = COALESCE(?, ?),
                staging_dataset_version_id = NULL
            WHERE id = ?
            """,
            (
                int(row["is_live"] or 0),
                row["live_status"],
                row["live_tsv_path"],
                row["live_metadata_status"],
                row["live_metadata_path"],
                row["live_metadata_clean_path"],
                row["live_genome_count"],
                row["live_taxon_id"],
                row["live_last_synced_at"],
                row["live_metadata_last_built_at"],
                row["live_updated_at"],
                now,
                row["species_id"],
            ),
        )
        restored += int(cursor.rowcount or 0)
    return restored


def snapshot_live_metadata_species_search(db: sqlite3.Connection, dataset_version_id: str) -> int:
    now = utc_now()
    existing = db.execute(
        "SELECT COUNT(*) AS total FROM dataset_metadata_species_search_snapshots WHERE dataset_version_id = ?",
        (dataset_version_id,),
    ).fetchone()
    if int(existing["total"] or 0):
        return 0
    cursor = db.execute(
        """
        INSERT INTO dataset_metadata_species_search_snapshots (
            dataset_version_id, source_taxon_id, source_taxon_name, species_name,
            search_name, genome_count, updated_at, captured_at
        )
        SELECT ?, source_taxon_id, source_taxon_name, species_name,
               search_name, genome_count, updated_at, ?
        FROM metadata_species_search
        """,
        (dataset_version_id, now),
    )
    return int(cursor.rowcount or 0)


def promote_staged_metadata_species_search(db: sqlite3.Connection, dataset_version_id: str) -> int:
    source_rows = db.execute(
        """
        SELECT DISTINCT source_taxon_id
        FROM metadata_species_search_staging
        WHERE dataset_version_id = ?
        """,
        (dataset_version_id,),
    ).fetchall()
    source_ids = [int(row["source_taxon_id"]) for row in source_rows]
    if not source_ids:
        return 0
    placeholders = ", ".join("?" for _ in source_ids)
    db.execute(
        f"DELETE FROM metadata_species_search WHERE source_taxon_id IN ({placeholders})",
        tuple(source_ids),
    )
    cursor = db.execute(
        """
        INSERT INTO metadata_species_search (
            source_taxon_id, source_taxon_name, species_name, search_name, genome_count, updated_at
        )
        SELECT source_taxon_id, source_taxon_name, species_name, search_name, genome_count, updated_at
        FROM metadata_species_search_staging
        WHERE dataset_version_id = ?
        ON CONFLICT(source_taxon_id, species_name) DO UPDATE SET
            source_taxon_name = excluded.source_taxon_name,
            search_name = excluded.search_name,
            genome_count = excluded.genome_count,
            updated_at = excluded.updated_at
        """,
        (dataset_version_id,),
    )
    db.execute(
        "DELETE FROM metadata_species_search_staging WHERE dataset_version_id = ?",
        (dataset_version_id,),
    )
    return int(cursor.rowcount or 0)


def restore_live_metadata_species_search_snapshot(db: sqlite3.Connection, dataset_version_id: str) -> int:
    snapshot_count = db.execute(
        """
        SELECT COUNT(*) AS total
        FROM dataset_metadata_species_search_snapshots
        WHERE dataset_version_id = ?
        """,
        (dataset_version_id,),
    ).fetchone()
    if int(snapshot_count["total"] or 0) <= 0:
        return 0
    db.execute("DELETE FROM metadata_species_search")
    cursor = db.execute(
        """
        INSERT INTO metadata_species_search (
            source_taxon_id, source_taxon_name, species_name, search_name, genome_count, updated_at
        )
        SELECT source_taxon_id, source_taxon_name, species_name, search_name, genome_count, updated_at
        FROM dataset_metadata_species_search_snapshots
        WHERE dataset_version_id = ?
        """,
        (dataset_version_id,),
    )
    return int(cursor.rowcount or 0)


def advance_dataset_update_pipeline_runs() -> None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute(
            """
            SELECT s.*, r.dataset_version_id, r.requested_at
            FROM dataset_update_pipeline_steps s
            JOIN dataset_update_pipeline_runs r ON r.run_id = s.run_id
            WHERE s.status = 'running'
            ORDER BY s.step_order ASC
            """
        ).fetchall()
        counts = dataset_update_active_counts(db)
        rank_counts = dataset_pipeline_rank_counts(db)
        for step in rows:
            step_key = str(step["step_key"])
            blockers: list[str] = []
            failed = False
            try:
                progress = json.loads(str(step["progress_json"] or "{}"))
            except json.JSONDecodeError:
                progress = {}
            progress.update(counts)
            if step_key == "discovery":
                if counts["discovery_active"]:
                    blockers.append(f"{counts['discovery_active']} discovery scopes still active")
                if counts["discovery_failed"]:
                    blockers.append(f"{counts['discovery_failed']} discovery scopes failed")
                    if not counts["discovery_active"]:
                        failed = True
            elif step_key == "catalog":
                phase = str(progress.get("phase") or "genus")
                if phase == "genus":
                    if rank_counts.get("catalog_genus_active", 0):
                        blockers.append(f"{rank_counts.get('catalog_genus_active', 0)} genus catalogs still active")
                    elif not blockers:
                        species_queue = request_pipeline_catalog_syncs(db, "species", str(step["dataset_version_id"]))
                        progress.update({"phase": "species", "species_queue": species_queue})
                        blockers.append(f"{species_queue['queued']} species catalogs queued")
                elif rank_counts.get("catalog_species_active", 0):
                    blockers.append(f"{rank_counts.get('catalog_species_active', 0)} species catalogs still active")
                relevant_catalog_failures = (
                    rank_counts.get("catalog_genus_failed", 0)
                    if phase == "genus"
                    else rank_counts.get("catalog_species_failed", 0)
                )
                relevant_catalog_active = (
                    rank_counts.get("catalog_genus_active", 0)
                    if phase == "genus"
                    else rank_counts.get("catalog_species_active", 0)
                )
                if relevant_catalog_failures:
                    if relevant_catalog_active:
                        blockers.append(f"{relevant_catalog_failures} {phase} catalog syncs need retry/no-data handling")
                    else:
                        failed = True
                        blockers.append(f"{relevant_catalog_failures} {phase} catalog syncs failed")
            elif step_key == "metadata":
                phase = str(progress.get("phase") or "genus")
                if phase == "genus":
                    if rank_counts.get("metadata_genus_active", 0):
                        blockers.append(f"{rank_counts.get('metadata_genus_active', 0)} genus metadata builds still active")
                    elif not blockers:
                        expansion_summary = progress.get("species_expansion")
                        if not isinstance(expansion_summary, dict):
                            db.commit()
                            expansion_summary = expand_species_catalog_from_genus_metadata(
                                staging_dataset_version_id=str(step["dataset_version_id"])
                            )
                            db.execute("BEGIN IMMEDIATE")
                            progress["species_expansion"] = expansion_summary
                            rank_counts = dataset_pipeline_rank_counts(db)
                        species_queue = request_pipeline_metadata_builds(db, "species", str(step["dataset_version_id"]))
                        progress.update({
                            "phase": "species",
                            "species_queue": species_queue,
                            "species_expansion": expansion_summary,
                        })
                        blockers.append(f"{species_queue['queued']} species metadata builds queued")
                elif rank_counts.get("metadata_species_active", 0):
                    blockers.append(f"{rank_counts.get('metadata_species_active', 0)} species metadata builds still active")
                if rank_counts.get("metadata_genus_failed", 0) or rank_counts.get("metadata_species_failed", 0):
                    failed = True
                    blockers.append(
                        f"{rank_counts.get('metadata_genus_failed', 0)} genus and "
                        f"{rank_counts.get('metadata_species_failed', 0)} species metadata builds failed"
                    )
            elif step_key == "standardization":
                if counts["standardization_active"]:
                    blockers.append(f"{counts['standardization_active']} standardization tasks/chunks still active")
                if counts["standardization_failed"]:
                    failed = True
                    blockers.append(f"{counts['standardization_failed']} standardization tasks/chunks failed")
            elif step_key == "verify":
                active_parts = {
                    "discovery": counts.get("discovery_active", 0),
                    "catalog": counts.get("catalog_active", 0),
                    "metadata": counts.get("metadata_active", 0),
                    "standardization": counts.get("standardization_active", 0),
                }
                failed_parts = {
                    "discovery": counts.get("discovery_failed", 0),
                    "catalog": counts.get("catalog_failed", 0),
                    "metadata": counts.get("metadata_failed", 0),
                    "standardization": counts.get("standardization_failed", 0),
                }
                still_active = [f"{count} {label}" for label, count in active_parts.items() if int(count or 0)]
                failures = [f"{count} {label}" for label, count in failed_parts.items() if int(count or 0)]
                if still_active:
                    blockers.append("Update work is still active: " + ", ".join(still_active))
                if failures:
                    failed = True
                    blockers.append("Failed update tasks require review: " + ", ".join(failures))
            if step_key in {"discovery", "catalog", "metadata", "standardization", "verify"}:
                if failed:
                    set_pipeline_step_status(db, int(step["id"]), "failed", progress=progress, blockers=blockers, error="; ".join(blockers))
                    db.execute("UPDATE dataset_update_pipeline_runs SET status = 'blocked', error = ? WHERE run_id = ?", ("; ".join(blockers), step["run_id"]))
                    if step_key == "verify":
                        db.execute("UPDATE dataset_versions SET status = 'failed', error = ? WHERE version_id = ?", ("; ".join(blockers), step["dataset_version_id"]))
                    continue
                if blockers:
                    set_pipeline_step_status(db, int(step["id"]), "running", progress=progress, blockers=blockers)
                    db.execute("UPDATE dataset_update_pipeline_runs SET status = 'running' WHERE run_id = ?", (step["run_id"],))
                    continue
                if step_key == "verify":
                    db.execute(
                        "UPDATE dataset_versions SET status = 'verified', verified_at = ?, error = NULL WHERE version_id = ?",
                        (utc_now(), step["dataset_version_id"]),
                    )
                mark_pipeline_step_completed(db, step, progress)
        db.commit()


def claim_next_dataset_pipeline_step(worker_name: str) -> sqlite3.Row | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT s.*, r.dataset_version_id, r.requested_at
            FROM dataset_update_pipeline_steps s
            JOIN dataset_update_pipeline_runs r ON r.run_id = s.run_id
            WHERE s.status = 'pending'
              AND r.status IN ('pending', 'running')
            ORDER BY r.requested_at ASC, s.step_order ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None
        now = utc_now()
        db.execute(
            """
            UPDATE dataset_update_pipeline_steps
            SET status = 'running',
                started_at = COALESCE(started_at, ?),
                blockers_json = '[]',
                error = NULL
            WHERE id = ?
            """,
            (now, row["id"]),
        )
        db.execute(
            """
            UPDATE dataset_update_pipeline_runs
            SET status = 'running',
                started_at = COALESCE(started_at, ?),
                error = NULL
            WHERE run_id = ?
            """,
            (now, row["run_id"]),
        )
        db.commit()
        return row


def process_dataset_pipeline_step(step: sqlite3.Row) -> None:
    step_key = str(step["step_key"])
    run_id = str(step["run_id"])
    version_id = str(step["dataset_version_id"])
    with get_sqlite_connection() as db:
        if step_key == "discovery":
            before = dataset_pipeline_rank_counts(db)
            queued = request_pipeline_discovery_refresh(db)
            progress = {
                "scope": get_setting("dataset_pipeline_scope", "2", db),
                "queued": queued,
                "species_before": before.get("catalog_species_total", 0),
                "genus_before": before.get("catalog_genus_total", 0),
                "note": "Genus-first discovery queued. Species are expanded after genus catalogs and metadata are available.",
            }
            set_pipeline_step_status(db, int(step["id"]), "running", progress=progress)
            db.commit()
            return
        if step_key == "catalog":
            genus_queue = request_pipeline_catalog_syncs(db, "genus", version_id)
            progress = {
                "phase": "genus",
                "genus_queue": genus_queue,
                "note": "Genus catalogs are refreshed before species catalogs.",
            }
            set_pipeline_step_status(db, int(step["id"]), "running", progress=progress)
            db.commit()
            return
        if step_key == "metadata":
            genus_queue = request_pipeline_metadata_builds(db, "genus", version_id)
            progress = {
                "phase": "genus",
                "genus_queue": genus_queue,
                "note": "Genus metadata is refreshed before species metadata.",
            }
            set_pipeline_step_status(db, int(step["id"]), "running", progress=progress)
            db.commit()
            return
        if step_key == "standardization":
            db.commit()
            changed_since = str(step["requested_at"] or "") or None
            summary = queue_standardization_refresh_for_ready_taxa(
                limit=None,
                dry_run=False,
                rank_scope="all",
                changed_since=changed_since,
            )
            summary["mode"] = "incremental"
            summary["note"] = "Only metadata built or refreshed after this pipeline run began was queued."
            db.execute("BEGIN IMMEDIATE")
            if int(summary.get("queued") or 0) or int(summary.get("running") or 0):
                set_pipeline_step_status(db, int(step["id"]), "running", progress=summary)
            else:
                mark_pipeline_step_completed(db, step, summary)
            db.commit()
            return
        if step_key == "replace":
            blockers = replacement_step_blockers(db, step)
            if blockers:
                error = "; ".join(blockers)
                set_pipeline_step_status(db, int(step["id"]), "failed", progress=dataset_update_active_counts(db), blockers=blockers, error=error)
                db.execute("UPDATE dataset_update_pipeline_runs SET status = 'blocked', error = ? WHERE run_id = ?", (error, run_id))
                db.execute("UPDATE dataset_versions SET status = 'failed', error = ? WHERE version_id = ?", (error, version_id))
                db.commit()
                return
            previous = get_active_dataset_version_id(db)
            now = utc_now()
            snapshot_count = snapshot_live_species_state(db, previous)
            metadata_search_snapshot_count = snapshot_live_metadata_species_search(db, previous)
            promoted_taxa = promote_staged_species_state(db, version_id)
            promoted_metadata_search_entries = promote_staged_metadata_species_search(db, version_id)
            db.execute(
                "UPDATE dataset_versions SET status = 'archived', archived_at = ? WHERE version_id = ? AND status = 'live'",
                (now, previous),
            )
            db.execute(
                """
                UPDATE dataset_versions
                SET status = 'live', promoted_at = ?, promoted_by = ?
                WHERE version_id = ?
                """,
                (now, None, version_id),
            )
            set_setting("previous_dataset_version_id", previous, db)
            set_setting("active_dataset_version_id", version_id, db)
            mark_pipeline_step_completed(
                db,
                step,
                {
                    "previous_version": previous,
                    "active_version": version_id,
                    "snapshot_taxa": snapshot_count,
                    "promoted_taxa": promoted_taxa,
                    "metadata_search_snapshot_entries": metadata_search_snapshot_count,
                    "promoted_metadata_search_entries": promoted_metadata_search_entries,
                    "note": "Live species pointers were replaced after upstream steps completed and verification passed.",
                },
            )
            db.commit()
            return
        if step_key == "global_insights":
            rows = db.execute(
                """
                SELECT id, species_name, taxon_rank,
                       live_genome_count AS genome_count,
                       live_metadata_clean_path AS metadata_clean_path,
                       live_last_synced_at AS last_synced_at
                FROM species
                WHERE is_live = 1
                  AND live_metadata_status = 'ready'
                  AND live_metadata_clean_path IS NOT NULL
                """
            ).fetchall()
            taxa = [
                {
                    "id": int(row["id"]),
                    "species_name": str(row["species_name"]),
                    "taxon_rank": str(row["taxon_rank"]),
                    "genome_count": row["genome_count"],
                    "metadata_clean_path": str(row["metadata_clean_path"] or ""),
                    "last_synced_at": str(row["last_synced_at"] or ""),
                }
                for row in rows
                if row["metadata_clean_path"]
            ]
            staging_root = global_insights_root()
            snapshot_id = f"{version_id}_global_insights"
            db.commit()
            summary = generate_global_insights_snapshot(
                taxa,
                staging_root,
                app_version=APP_VERSION,
                app_commit=APP_COMMIT,
                snapshot_id=snapshot_id,
            )
            with get_sqlite_connection() as update_db:
                update_db.execute(
                    """
                    UPDATE dataset_versions
                    SET summary_json = ?
                    WHERE version_id = ?
                    """,
                    (json.dumps({"global_insights": summary}, sort_keys=True), version_id),
                )
                mark_pipeline_step_completed(
                    update_db,
                    step,
                    {
                        "snapshot_id": summary.get("snapshot_id"),
                        "unique_assemblies": (summary.get("overview") or {}).get("unique_assemblies"),
                    },
                )
                update_db.commit()
            return
        if step_key == "verify":
            counts = dataset_update_active_counts(db)
            set_pipeline_step_status(
                db,
                int(step["id"]),
                "running",
                progress={
                    **counts,
                    "note": "Verification waits until no discovery, catalog, metadata, or standardization work remains active or failed.",
                },
            )
            db.commit()
            return
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


def species_dir_for_version(slug: str, dataset_version_id: str | None = None) -> Path:
    if dataset_version_id:
        return dataset_version_root(dataset_version_id) / "species" / slug
    return species_dir(slug)


def species_tsv_path(slug: str, dataset_version_id: str | None = None) -> Path:
    return species_dir_for_version(slug, dataset_version_id) / "ncbi_dataset.tsv"


def metadata_taxon_dir(slug: str, dataset_version_id: str | None = None) -> Path:
    if dataset_version_id:
        return dataset_version_root(dataset_version_id) / "metadata" / slug
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


@contextlib.contextmanager
def maintain_worker_heartbeat(worker_name: str | None) -> Any:
    if not worker_name:
        yield
        return

    interval_seconds = max(1.0, WORKER_HEARTBEAT_SECONDS / 2.0)
    stop_event = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_event.wait(interval_seconds):
            try:
                touch_worker_heartbeat(worker_name)
            except Exception:
                logging.exception("Failed to refresh worker heartbeat for %s during long-running work.", worker_name)

    touch_worker_heartbeat(worker_name)
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        name=f"worker-heartbeat-{worker_name.replace(':', '-')}",
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        yield
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=1.0)
        touch_worker_heartbeat(worker_name)


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


def metadata_dataset_path(slug: str, dataset_version_id: str | None = None) -> Path:
    return metadata_taxon_dir(slug, dataset_version_id) / "metadata_output" / "ncbi_dataset_updated.tsv"


def metadata_clean_path(slug: str, dataset_version_id: str | None = None) -> Path:
    return metadata_taxon_dir(slug, dataset_version_id) / "metadata_output" / "ncbi_clean.csv"


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
    try:
        user = get_user_by_id(int(user_id))
    except (TypeError, ValueError):
        session.clear()
        return None
    if user is None:
        session.clear()
        return None
    return user


def login_user(user: sqlite3.Row) -> None:
    session.clear()
    session.permanent = True
    session["user_id"] = int(user["id"])


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
    if password != confirm:
        return "Passwords do not match."
    if len(password) < PASSWORD_MIN_LENGTH:
        return f"Password must be at least {PASSWORD_MIN_LENGTH} characters."
    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        return "Password must include at least one letter and one number."
    return None


def build_security_posture() -> dict[str, Any]:
    return {
        "app_version": APP_VERSION,
        "password_policy": f"{PASSWORD_MIN_LENGTH}+ characters, including at least one letter and one number",
        "session_lifetime": str(app.config["PERMANENT_SESSION_LIFETIME"]),
        "cookie_httponly": bool(app.config["SESSION_COOKIE_HTTPONLY"]),
        "cookie_secure": bool(app.config["SESSION_COOKIE_SECURE"]),
        "cookie_samesite": app.config["SESSION_COOKIE_SAMESITE"],
        "csrf": "enabled for POST forms",
        "auth_rate_limit": f"{AUTH_RATE_LIMIT_MAX_ATTEMPTS} attempts per {AUTH_RATE_LIMIT_WINDOW_SECONDS // 60} minutes per IP",
        "secret_key": "custom" if app.config["SECRET_KEY"] != DEFAULT_SECRET_KEY else "development default",
        "password_reset": "configured" if mail_is_configured() else "SMTP not configured",
        "admin_model": "single admin role from FETCHM_WEBAPP_ADMIN_USERS",
    }


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
        claimed_by=str(row["claimed_by"]) if row["claimed_by"] else None,
        claimed_at=str(row["claimed_at"]) if row["claimed_at"] else None,
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
        metadata_progress_total=int(row["metadata_progress_total"] or 0),
        metadata_progress_completed=int(row["metadata_progress_completed"] or 0),
        metadata_progress_current_accession=str(row["metadata_progress_current_accession"]) if row["metadata_progress_current_accession"] else None,
        metadata_progress_updated_at=str(row["metadata_progress_updated_at"]) if row["metadata_progress_updated_at"] else None,
        assembly_backfill_status=str(row["assembly_backfill_status"] or "idle"),
        assembly_backfill_requested_at=str(row["assembly_backfill_requested_at"]) if row["assembly_backfill_requested_at"] else None,
        assembly_backfill_claimed_by=str(row["assembly_backfill_claimed_by"]) if row["assembly_backfill_claimed_by"] else None,
        assembly_backfill_claimed_at=str(row["assembly_backfill_claimed_at"]) if row["assembly_backfill_claimed_at"] else None,
        assembly_backfill_last_built_at=str(row["assembly_backfill_last_built_at"]) if row["assembly_backfill_last_built_at"] else None,
        assembly_backfill_error=str(row["assembly_backfill_error"]) if row["assembly_backfill_error"] else None,
        is_live=bool(row["is_live"]) if "is_live" in row.keys() else True,
        staging_dataset_version_id=str(row["staging_dataset_version_id"]) if "staging_dataset_version_id" in row.keys() and row["staging_dataset_version_id"] else None,
        live_status=str(row["live_status"]) if "live_status" in row.keys() and row["live_status"] else None,
        live_tsv_path=str(row["live_tsv_path"]) if "live_tsv_path" in row.keys() and row["live_tsv_path"] else None,
        live_metadata_status=str(row["live_metadata_status"]) if "live_metadata_status" in row.keys() and row["live_metadata_status"] else None,
        live_metadata_path=str(row["live_metadata_path"]) if "live_metadata_path" in row.keys() and row["live_metadata_path"] else None,
        live_metadata_clean_path=str(row["live_metadata_clean_path"]) if "live_metadata_clean_path" in row.keys() and row["live_metadata_clean_path"] else None,
        live_genome_count=row["live_genome_count"] if "live_genome_count" in row.keys() else None,
        live_taxon_id=row["live_taxon_id"] if "live_taxon_id" in row.keys() else None,
        live_last_synced_at=str(row["live_last_synced_at"]) if "live_last_synced_at" in row.keys() and row["live_last_synced_at"] else None,
        live_metadata_last_built_at=str(row["live_metadata_last_built_at"]) if "live_metadata_last_built_at" in row.keys() and row["live_metadata_last_built_at"] else None,
        live_updated_at=str(row["live_updated_at"]) if "live_updated_at" in row.keys() and row["live_updated_at"] else None,
    )


def public_species_record(species: SpeciesRecord) -> SpeciesRecord | None:
    if not species.is_live:
        return None
    live_status = species.live_status or species.status
    live_tsv_path = species.live_tsv_path or species.tsv_path
    if live_status != "ready" or not live_tsv_path:
        return None
    return dataclass_replace(
        species,
        status=live_status,
        tsv_path=live_tsv_path,
        metadata_status=species.live_metadata_status or species.metadata_status,
        metadata_path=species.live_metadata_path or species.metadata_path,
        metadata_clean_path=species.live_metadata_clean_path or species.metadata_clean_path,
        genome_count=species.live_genome_count if species.live_genome_count is not None else species.genome_count,
        taxon_id=species.live_taxon_id if species.live_taxon_id is not None else species.taxon_id,
        last_synced_at=species.live_last_synced_at or species.last_synced_at,
        metadata_last_built_at=species.live_metadata_last_built_at or species.metadata_last_built_at,
        updated_at=species.live_updated_at or species.updated_at,
        refresh_requested=False,
        claimed_by=None,
        claimed_at=None,
        metadata_refresh_requested=False,
        metadata_claimed_by=None,
        metadata_claimed_at=None,
    )


def require_public_species(species_id: int) -> SpeciesRecord:
    species = load_species(species_id)
    public_species = public_species_record(species)
    if public_species is None:
        raise FileNotFoundError(str(species_id))
    return public_species


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
            metadata_source_taxon_id, metadata_progress_total, metadata_progress_completed,
            metadata_progress_current_accession, metadata_progress_updated_at, is_live, staging_dataset_version_id,
            live_status, live_tsv_path, live_metadata_status, live_metadata_path, live_metadata_clean_path,
            live_genome_count, live_taxon_id, live_last_synced_at, live_metadata_last_built_at, live_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            metadata_source_taxon_id = excluded.metadata_source_taxon_id,
            metadata_progress_total = excluded.metadata_progress_total,
            metadata_progress_completed = excluded.metadata_progress_completed,
            metadata_progress_current_accession = excluded.metadata_progress_current_accession,
            metadata_progress_updated_at = excluded.metadata_progress_updated_at,
            is_live = excluded.is_live,
            staging_dataset_version_id = excluded.staging_dataset_version_id,
            live_status = excluded.live_status,
            live_tsv_path = excluded.live_tsv_path,
            live_metadata_status = excluded.live_metadata_status,
            live_metadata_path = excluded.live_metadata_path,
            live_metadata_clean_path = excluded.live_metadata_clean_path,
            live_genome_count = excluded.live_genome_count,
            live_taxon_id = excluded.live_taxon_id,
            live_last_synced_at = excluded.live_last_synced_at,
            live_metadata_last_built_at = excluded.live_metadata_last_built_at,
            live_updated_at = excluded.live_updated_at
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
            species.metadata_progress_total,
            species.metadata_progress_completed,
            species.metadata_progress_current_accession,
            species.metadata_progress_updated_at,
            int(species.is_live),
            species.staging_dataset_version_id,
            species.live_status,
            species.live_tsv_path,
            species.live_metadata_status,
            species.live_metadata_path,
            species.live_metadata_clean_path,
            species.live_genome_count,
            species.live_taxon_id,
            species.live_last_synced_at,
            species.live_metadata_last_built_at,
            species.live_updated_at,
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
        WHERE is_live = 1
          AND live_status = 'ready'
          AND live_tsv_path IS NOT NULL
        ORDER BY taxon_rank, species_name COLLATE NOCASE ASC
        """
    ).fetchall()
    return [species for row in rows if (species := public_species_record(row_to_species(row))) is not None]


def list_recent_metadata_taxa(limit: int = 100) -> list[SpeciesRecord]:
    rows = get_db().execute(
        """
        SELECT *
        FROM species
        WHERE metadata_status IS NOT NULL
          AND metadata_status != 'missing'
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
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
            cancel_requested, claimed_by, claimed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            cancel_requested = excluded.cancel_requested,
            claimed_by = excluded.claimed_by,
            claimed_at = excluded.claimed_at
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
            job.claimed_by,
            job.claimed_at,
        ),
    )
    connection.commit()


def list_jobs_for_user(user_id: int) -> list[JobRecord]:
    reconcile_cancelled_running_jobs()
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
            SUM(CASE WHEN taxon_rank = 'species' AND is_live = 1 THEN 1 ELSE 0 END) AS species_total,
            SUM(CASE WHEN taxon_rank = 'species' AND is_live = 1 AND live_tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS species_ready,
            SUM(CASE WHEN taxon_rank = 'genus' AND is_live = 1 THEN 1 ELSE 0 END) AS genus_total,
            SUM(CASE WHEN taxon_rank = 'genus' AND is_live = 1 AND live_tsv_path IS NOT NULL THEN 1 ELSE 0 END) AS genus_ready,
            SUM(CASE WHEN is_live = 1 AND live_tsv_path IS NOT NULL AND live_genome_count IS NOT NULL THEN live_genome_count ELSE 0 END) AS genome_total,
            SUM(CASE WHEN is_live = 1 AND live_metadata_clean_path IS NOT NULL AND live_genome_count IS NOT NULL THEN live_genome_count ELSE 0 END) AS metadata_genome_total,
            SUM(CASE WHEN is_live = 1 AND live_metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS metadata_taxa_ready
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
    reconcile_cancelled_running_jobs()
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


def count_active_quality_jobs() -> int:
    reconcile_cancelled_running_jobs()
    row = get_db().execute(
        """
        SELECT COUNT(*)
        FROM jobs
        WHERE mode = 'qc'
          AND status IN ('queued', 'running')
        """
    ).fetchone()
    return int(row[0]) if row is not None else 0


def quality_submission_blockers() -> list[str]:
    blockers: list[str] = []
    memory = read_memory_usage()
    if memory["total_bytes"]:
        if memory["available_bytes"] < MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES:
            blockers.append(
                "The server is low on available memory "
                f"({memory['available_label']} available; quality jobs require at least "
                f"{format_bytes(MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES)})."
            )
        if memory["used_percent"] >= MAX_MEMORY_PERCENT_FOR_QUALITY:
            blockers.append(
                "The server memory load is too high "
                f"({memory['used_percent']}% used; quality jobs pause at "
                f"{MAX_MEMORY_PERCENT_FOR_QUALITY:g}% or higher)."
            )
    active_quality_jobs = count_active_quality_jobs()
    if active_quality_jobs >= MAX_ACTIVE_QUALITY_JOBS:
        blockers.append(
            f"{active_quality_jobs} quality job(s) are already queued or running; "
            f"the current limit is {MAX_ACTIVE_QUALITY_JOBS}."
        )
    return blockers


def list_all_jobs() -> list[JobRecord]:
    reconcile_cancelled_running_jobs()
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


def build_admin_job_analytics(jobs: list[JobRecord]) -> dict[str, Any]:
    status_counts = Counter(job.status for job in jobs)
    mode_counts = Counter(job.mode for job in jobs)
    user_counts = Counter(job.owner_username or "no owner" for job in jobs)
    now = utc_now_dt()
    recent_jobs = []
    durations: list[float] = []
    for job in jobs:
        try:
            created_at = parse_utc(job.created_at)
        except ValueError:
            continue
        if created_at >= now - timedelta(days=7):
            recent_jobs.append(job)
        if job.status in {"completed", "failed", "cancelled"}:
            try:
                updated_at = parse_utc(job.updated_at)
            except ValueError:
                continue
            durations.append(max(0.0, (updated_at - created_at).total_seconds()))

    avg_duration = statistics.mean(durations) if durations else None
    success_count = status_counts.get("completed", 0)
    finished_count = sum(status_counts.get(status, 0) for status in ["completed", "failed", "cancelled"])
    success_rate = round((success_count / finished_count) * 100, 1) if finished_count else None
    total_jobs = len(jobs)

    def analytics_rows(counter: Counter[str], limit: int | None = None) -> list[dict[str, Any]]:
        rows = counter.most_common(limit)
        return [
            {
                "label": key,
                "count": value,
                "percent": round((value / total_jobs) * 100, 1) if total_jobs else 0.0,
            }
            for key, value in rows
        ]

    return {
        "total": total_jobs,
        "active": status_counts.get("queued", 0) + status_counts.get("running", 0),
        "queued": status_counts.get("queued", 0),
        "running": status_counts.get("running", 0),
        "completed": status_counts.get("completed", 0),
        "failed": status_counts.get("failed", 0),
        "cancelled": status_counts.get("cancelled", 0),
        "recent_7d": len(recent_jobs),
        "avg_duration_label": format_elapsed_brief(int(avg_duration)) if avg_duration is not None else "n/a",
        "success_rate": success_rate,
        "status_rows": analytics_rows(status_counts),
        "mode_rows": analytics_rows(mode_counts),
        "user_rows": analytics_rows(user_counts, 8),
    }


def latest_audit_subdir(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    dirs = [path for path in parent.iterdir() if path.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda path: path.name)


def iter_admin_audit_bundle_files() -> list[tuple[Path, str]]:
    review_dir = STANDARDIZATION_DIR / "review"
    candidates: list[tuple[Path, str]] = []

    for subdir_name in ["final_audit", "quality_audit", "source_sample_environment_audit"]:
        latest_dir = latest_audit_subdir(review_dir / subdir_name)
        if latest_dir is None:
            continue
        for path in sorted(latest_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in {".csv", ".json", ".md", ".txt"}:
                candidates.append((path, f"standardization/{subdir_name}/{latest_dir.name}/{path.name}"))

    for name in [
        "final_metadata_standardization_dashboard.md",
        "final_release_note.md",
        "metadata_standardization_publication_summary.md",
    ]:
        path = review_dir / name
        if path.exists() and path.is_file():
            candidates.append((path, f"standardization/{name}"))

    remaining_dir = latest_audit_subdir(review_dir / "remaining_batches")
    if remaining_dir is not None:
        for path in sorted(remaining_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in {".csv", ".md", ".txt"}:
                candidates.append((path, f"standardization/remaining_batches/{remaining_dir.name}/{path.name}"))

    seen: set[str] = set()
    unique: list[tuple[Path, str]] = []
    for path, arcname in candidates:
        if arcname in seen:
            continue
        seen.add(arcname)
        unique.append((path, arcname))
    return unique


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
            (SELECT COUNT(DISTINCT assembly_accession) FROM assembly_metadata) AS catalog_genomes_total,
            (SELECT COUNT(DISTINCT assembly_accession) FROM assembly_metadata) AS metadata_genomes_total,
            SUM(CASE WHEN tsv_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS taxon_scoped_catalog_genomes_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL AND genome_count IS NOT NULL THEN genome_count ELSE 0 END) AS taxon_scoped_metadata_genomes_total,
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


def summarize_metadata_build_progress(total: int | None, completed: int | None) -> dict[str, Any]:
    total_value = max(int(total or 0), 0)
    completed_value = max(0, min(int(completed or 0), total_value)) if total_value else max(int(completed or 0), 0)
    percent = int(round((completed_value / total_value) * 100)) if total_value else 0
    return {
        "total": total_value,
        "completed": completed_value,
        "remaining": max(total_value - completed_value, 0),
        "percent": percent,
        "headline": f"{completed_value} / {total_value} rows assembled" if total_value else "Sizing metadata scope",
    }


def build_metadata_dashboard() -> dict[str, Any]:
    db = get_db()
    live_metadata_claim_ids = {
        int(row["id"])
        for row in db.execute(
            """
            SELECT id, metadata_claimed_by
            FROM species
            WHERE metadata_claimed_at IS NOT NULL
              AND metadata_claimed_by IS NOT NULL
            """
        ).fetchall()
        if row["metadata_claimed_by"] and worker_heartbeat_is_live(str(row["metadata_claimed_by"]))
    }

    totals = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN metadata_path IS NOT NULL THEN 1 ELSE 0 END) AS ready_total,
            SUM(CASE WHEN metadata_status = 'pending' THEN 1 ELSE 0 END) AS pending_total,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'building' AND id IN ({live_ids}) THEN 1 ELSE 0 END) AS building_total,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_claimed_at IS NOT NULL AND id IN ({live_ids}) THEN 1 ELSE 0 END) AS refreshing_total,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_refresh_requested = 1 AND metadata_claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued_total,
            SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed_total,
            SUM(CASE WHEN metadata_clean_path IS NOT NULL THEN 1 ELSE 0 END) AS clean_ready_total,
            SUM(CASE WHEN assembly_backfill_status = 'pending' THEN 1 ELSE 0 END) AS assembly_backfill_pending_total,
            SUM(CASE WHEN assembly_backfill_status = 'running' THEN 1 ELSE 0 END) AS assembly_backfill_running_total,
            SUM(CASE WHEN assembly_backfill_status = 'done' THEN 1 ELSE 0 END) AS assembly_backfill_done_total,
            SUM(CASE WHEN assembly_backfill_status = 'failed' THEN 1 ELSE 0 END) AS assembly_backfill_failed_total
        FROM species
        WHERE tsv_path IS NOT NULL
        """
        .format(live_ids=", ".join(str(item) for item in sorted(live_metadata_claim_ids)) or "NULL")
    ).fetchone()
    rank_rows = db.execute(
        """
        SELECT
            taxon_rank,
            COUNT(*) AS total,
            SUM(CASE WHEN metadata_path IS NOT NULL THEN 1 ELSE 0 END) AS ready,
            SUM(CASE WHEN metadata_status = 'pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN metadata_path IS NULL AND metadata_status = 'building' AND id IN ({live_ids}) THEN 1 ELSE 0 END) AS building,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_claimed_at IS NOT NULL AND id IN ({live_ids}) THEN 1 ELSE 0 END) AS refreshing,
            SUM(CASE WHEN metadata_path IS NOT NULL AND metadata_refresh_requested = 1 AND metadata_claimed_at IS NULL THEN 1 ELSE 0 END) AS refresh_queued,
            SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM species
        WHERE tsv_path IS NOT NULL
        GROUP BY taxon_rank
        ORDER BY taxon_rank
        """
        .format(live_ids=", ".join(str(item) for item in sorted(live_metadata_claim_ids)) or "NULL")
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
            metadata_source_taxon_id,
            metadata_progress_total,
            metadata_progress_completed,
            metadata_progress_current_accession,
            metadata_progress_updated_at
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
        claimed_by_value = str(row["metadata_claimed_by"]) if row["metadata_claimed_by"] else None
        worker_live = worker_heartbeat_is_live(claimed_by_value) if claimed_by_value else False
        if row["metadata_claimed_at"] and not worker_live:
            continue
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
                "progress": summarize_metadata_build_progress(row["metadata_progress_total"], row["metadata_progress_completed"]),
                "current_accession": str(row["metadata_progress_current_accession"]) if row["metadata_progress_current_accession"] else None,
                "progress_updated_at": str(row["metadata_progress_updated_at"]) if row["metadata_progress_updated_at"] else None,
            }
        )
    standardization_rows = db.execute(
        """
        SELECT status, COUNT(*) AS total, SUM(total_rows) AS total_rows, SUM(updated_rows) AS updated_rows
        FROM standardization_refresh_tasks
        GROUP BY status
        """
    ).fetchall()
    standardization_refresh = {
        "pending": 0,
        "running": 0,
        "chunking": 0,
        "finalizing": 0,
        "done": 0,
        "failed": 0,
        "deferred": 0,
        "active": 0,
        "pending_chunks": 0,
        "running_chunks": 0,
        "done_chunks": 0,
        "failed_chunks": 0,
        "chunk_total_rows": 0,
        "chunk_updated_rows": 0,
        "total_rows": 0,
        "updated_rows": 0,
        "recent": [],
    }
    for row in standardization_rows:
        status = str(row["status"])
        standardization_refresh[status] = int(row["total"] or 0)
        standardization_refresh["total_rows"] += int(row["total_rows"] or 0)
        standardization_refresh["updated_rows"] += int(row["updated_rows"] or 0)
    chunk_rows = db.execute(
        """
        SELECT status, COUNT(*) AS total, SUM(total_rows) AS total_rows, SUM(updated_rows) AS updated_rows
        FROM standardization_refresh_chunks
        GROUP BY status
        """
    ).fetchall()
    for row in chunk_rows:
        status = str(row["status"])
        key = f"{status}_chunks"
        if key in standardization_refresh:
            standardization_refresh[key] = int(row["total"] or 0)
        standardization_refresh["chunk_total_rows"] += int(row["total_rows"] or 0)
        standardization_refresh["chunk_updated_rows"] += int(row["updated_rows"] or 0)
    standardization_refresh["active"] = (
        int(standardization_refresh["running"] or 0)
        + int(standardization_refresh["finalizing"] or 0)
        + int(standardization_refresh["running_chunks"] or 0)
    )
    recent_standardization = db.execute(
        """
        SELECT
            t.status,
            t.requested_at,
            t.claimed_by,
            t.claimed_at,
            t.completed_at,
            t.total_rows,
            t.updated_rows,
            t.error,
            s.species_name,
            s.taxon_rank
        FROM standardization_refresh_tasks t
        JOIN species s ON s.id = t.species_id
        ORDER BY
            CASE
                WHEN t.status = 'running' THEN 0
                WHEN t.status = 'pending' THEN 1
                WHEN t.status = 'failed' THEN 2
                ELSE 3
            END,
            COALESCE(t.claimed_at, t.requested_at) DESC
        LIMIT 20
        """
    ).fetchall()
    standardization_refresh["recent"] = [dict(row) for row in recent_standardization]
    latest_insight = latest_global_insight_task(db)
    insight_blockers = global_insight_generation_blockers(db)
    latest_summary, _latest_summary_dir = load_latest_global_insights_summary()
    global_insights = {
        "latest_task": latest_insight,
        "blockers": insight_blockers,
        "can_generate": not insight_blockers,
        "latest_snapshot": {
            "snapshot_id": latest_summary.get("snapshot_id"),
            "generated_at": latest_summary.get("generated_at"),
            "is_demo": bool(latest_summary.get("is_demo")),
            "unique_assemblies": ((latest_summary.get("overview") or {}).get("unique_assemblies")),
        }
        if latest_summary
        else None,
    }
    return {
        "totals": dict(totals) if totals is not None else {},
        "rank_breakdown": [dict(row) for row in rank_rows],
        "active_items": items,
        "metadata_build_policy": get_metadata_build_policy(db),
        "metadata_refresh_policy": get_metadata_refresh_policy(db),
        "host_refinement": build_host_refinement_review(db),
        "standardization_refresh": standardization_refresh,
        "global_insights": global_insights,
    }


def build_host_refinement_review(
    db: sqlite3.Connection,
    *,
    limit: int = 30,
    sample_rows: int = 50000,
) -> dict[str, Any]:
    rows = db.execute(
        """
        SELECT json_extract(row_json, '$."Host"') AS host, COUNT(*) AS total
        FROM (
            SELECT row_json
            FROM assembly_metadata
            ORDER BY rowid DESC
            LIMIT ?
        )
        GROUP BY host
        ORDER BY total DESC
        LIMIT 1000
        """,
        (sample_rows,),
    ).fetchall()
    mapped_total = 0
    unmapped_total = 0
    top_unmapped: list[dict[str, Any]] = []
    top_mapped: list[dict[str, Any]] = []
    for row in rows:
        host = "" if row["host"] is None else str(row["host"])
        count = int(row["total"] or 0)
        standardized = standardize_host_metadata(host)
        item = {
            "host": host or "(blank)",
            "count": count,
            "host_sd": standardized["Host_SD"],
            "taxid": standardized["Host_TaxID"],
            "method": standardized["Host_SD_Method"],
            "confidence": standardized["Host_SD_Confidence"],
        }
        if standardized["Host_TaxID"]:
            mapped_total += count
            if len(top_mapped) < limit:
                top_mapped.append(item)
        else:
            unmapped_total += count
            if len(top_unmapped) < limit:
                top_unmapped.append(item)
    return {
        "mapped_top_rows": mapped_total,
        "unmapped_top_rows": unmapped_total,
        "top_unmapped": top_unmapped,
        "top_mapped": top_mapped,
        "reviewed_distinct_values": len(rows),
        "sample_rows": sample_rows,
    }


REFINEMENT_SOURCE_COLUMNS = {
    "Host": {
        "label": "Host",
        "json_path": '$."Host"',
        "primary_destination": "Host_SD",
    },
    "Isolation Source": {
        "label": "Isolation Source",
        "json_path": '$."Isolation Source"',
        "primary_destination": "Isolation_Source_SD",
    },
    "Environment Medium": {
        "label": "Environment Medium",
        "json_path": '$."Environment Medium"',
        "primary_destination": "Environment_Medium_SD",
    },
    "Environment (Broad Scale)": {
        "label": "Environment Broad Scale",
        "json_path": '$."Environment (Broad Scale)"',
        "primary_destination": "Environment_Medium_SD",
    },
    "Environment (Local Scale)": {
        "label": "Environment Local Scale",
        "json_path": '$."Environment (Local Scale)"',
        "primary_destination": "Environment_Medium_SD",
    },
    "Sample Type": {
        "label": "Sample Type",
        "json_path": '$."Sample Type"',
        "primary_destination": "Sample_Type_SD",
    },
}


def classify_refinement_value(source_column: str, value: Any) -> dict[str, str]:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text):
        return {
            "category": "missing",
            "destination": "",
            "proposed_value": "",
            "ontology_id": "",
            "method": "missing",
            "confidence": "none",
            "action": "ignore",
            "suggestion_score": "",
            "note": "Missing or explicitly absent value.",
        }

    row = {
        "Host": text if source_column == "Host" else "",
        "Isolation Source": text if source_column == "Isolation Source" else "",
        "Environment Medium": text if source_column == "Environment Medium" else "",
        "Environment (Broad Scale)": text if source_column == "Environment (Broad Scale)" else "",
        "Environment (Local Scale)": text if source_column == "Environment (Local Scale)" else "",
        "Sample Type": text if source_column == "Sample Type" else "",
    }

    host_standardization = standardize_host_metadata(text if source_column == "Host" else "")
    secondary = standardize_secondary_metadata(row, host_standardization)
    if source_column == "Host" and host_standardization["Host_TaxID"]:
        return {
            "category": "host organism",
            "destination": "Host_SD",
            "proposed_value": host_standardization["Host_SD"],
            "ontology_id": host_standardization["Host_TaxID"],
            "method": host_standardization["Host_SD_Method"],
            "confidence": host_standardization["Host_SD_Confidence"],
            "action": "review",
            "suggestion_score": "100",
            "note": "Taxonomy-backed host standardization candidate.",
        }

    if source_column == "Host":
        fuzzy_candidate = fuzzy_refinement_candidate(source_column, text)
        if fuzzy_candidate is not None:
            return fuzzy_candidate
        return {
            "category": "ambiguous",
            "destination": "",
            "proposed_value": text,
            "ontology_id": "",
            "method": "unmapped",
            "confidence": "none",
            "action": "leave",
            "suggestion_score": "",
            "note": "Host value needs manual review before mapping.",
        }

    for destination, category in [
        ("Sample_Type_SD", "sample type"),
        ("Environment_Medium_SD", "environment medium"),
        ("Isolation_Source_SD", "isolation source"),
    ]:
        proposed = secondary.get(destination, "")
        method = secondary.get(f"{destination}_Method", "")
        if proposed and method not in {"missing", "original"}:
            return {
            "category": category,
            "destination": destination,
            "proposed_value": proposed,
            "ontology_id": secondary.get(f"{destination.replace('_SD', '')}_Ontology_ID", ""),
            "method": method,
                "confidence": "medium",
                "action": "review",
                "suggestion_score": "100",
                "note": "MIxS/BioSample-style non-host metadata candidate.",
            }

    fuzzy_candidate = fuzzy_refinement_candidate(source_column, text)
    if fuzzy_candidate is not None:
        return fuzzy_candidate

    return {
        "category": "ambiguous",
        "destination": "",
        "proposed_value": text,
        "ontology_id": "",
        "method": "unmapped",
        "confidence": "none",
        "action": "leave",
        "suggestion_score": "",
        "note": "Needs manual review before mapping.",
    }


def approved_refinement_rule(
    db: sqlite3.Connection,
    source_column: str,
    original_value: Any,
    destination: str,
) -> sqlite3.Row | None:
    normalized_value = normalize_standardization_lookup(original_value)
    if not normalized_value or not destination:
        return None
    return db.execute(
        """
        SELECT *
        FROM standardization_rules
        WHERE source_column = ?
          AND normalized_value = ?
          AND destination = ?
          AND status = 'approved'
        LIMIT 1
        """,
        (source_column, normalized_value, destination),
    ).fetchone()


def save_approved_standardization_rule(
    *,
    source_column: str,
    original_value: str,
    category: str,
    destination: str,
    proposed_value: str,
    ontology_id: str,
    method: str,
    confidence: str,
    note: str,
    approved_by: str,
) -> None:
    normalized_value = normalize_standardization_lookup(original_value)
    method = method.strip()
    empty_host_rule = (
        destination == "Host_SD"
        and proposed_value == ""
        and method in {"missing", "non_host_source", "not_identifiable"}
    )
    if not source_column or not normalized_value or not destination or (not proposed_value and not empty_host_rule):
        raise ValueError("source column, original value, destination, and proposed value are required")
    db = get_db()
    approved_at = utc_now()
    db.execute(
        """
        INSERT INTO standardization_rules (
            source_column, original_value, normalized_value, category, destination,
            proposed_value, ontology_id, method, confidence, status, approved_by,
            approved_at, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?, ?)
        ON CONFLICT(source_column, normalized_value, destination)
        DO UPDATE SET
            original_value = excluded.original_value,
            category = excluded.category,
            proposed_value = excluded.proposed_value,
            ontology_id = excluded.ontology_id,
            method = excluded.method,
            confidence = excluded.confidence,
            status = 'approved',
            approved_by = excluded.approved_by,
            approved_at = excluded.approved_at,
            note = excluded.note
        """,
        (
            source_column,
            original_value,
            normalized_value,
            category,
            destination,
            proposed_value,
            ontology_id,
            method,
            confidence,
            approved_by,
            approved_at,
            note,
        ),
    )
    db.commit()
    apply_approved_standardization_rule_to_memory(
        {
            "normalized_value": normalized_value,
            "original_value": original_value,
            "destination": destination,
            "proposed_value": proposed_value,
            "ontology_id": ontology_id,
            "method": method,
            "confidence": confidence,
        }
    )
    if source_column == "Host" and destination == "Host_SD":
        clear_host_curation_cache()


def refinement_filters_from_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    source = str(values.get("source") or "all").strip()
    status = str(values.get("status") or "review").strip()
    confidence = str(values.get("confidence") or "all").strip()
    min_count = parse_optional_int(values.get("min_count"))
    return {
        "source": source if source in {"all", *REFINEMENT_SOURCE_COLUMNS.keys()} else "all",
        "status": status if status in {"all", "review", "approved", "ambiguous", "missing"} else "review",
        "confidence": confidence if confidence in {"all", "high", "medium", "none"} else "all",
        "min_count": max(0, min_count or 0),
    }


def refinement_item_matches_filters(item: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    min_count = int(filters.get("min_count") or 0)
    if min_count and int(item.get("count") or 0) < min_count:
        return False

    status_filter = str(filters.get("status") or "review")
    if status_filter == "review" and (item.get("is_approved") or item.get("action") != "review"):
        return False
    if status_filter == "approved" and not item.get("is_approved"):
        return False
    if status_filter == "ambiguous" and item.get("category") != "ambiguous":
        return False
    if status_filter == "missing" and item.get("category") != "missing":
        return False

    confidence_filter = str(filters.get("confidence") or "all")
    if confidence_filter != "all" and item.get("confidence") != confidence_filter:
        return False
    return True


def build_refinement_dashboard(
    limit_per_column: int = 50,
    sample_rows: int = 100000,
    filters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    limit = max(10, min(int(limit_per_column or 50), 200))
    sample_limit = max(1000, min(int(sample_rows or 100000), 1000000))
    active_filters = refinement_filters_from_mapping(filters or {})
    db = get_db()
    sections: list[dict[str, Any]] = []
    totals = {
        "values_reviewed": 0,
        "rows_represented": 0,
        "review_candidates": 0,
        "missing_values": 0,
        "ambiguous_values": 0,
        "approved_rules": 0,
        "visible_items": 0,
        "selectable_items": 0,
    }
    for source_column, config in REFINEMENT_SOURCE_COLUMNS.items():
        if active_filters["source"] != "all" and active_filters["source"] != source_column:
            continue
        rows = db.execute(
            f"""
            SELECT json_extract(row_json, ?) AS value, COUNT(*) AS total
            FROM (
                SELECT row_json
                FROM assembly_metadata
                ORDER BY rowid DESC
                LIMIT ?
            )
            GROUP BY value
            ORDER BY total DESC
            LIMIT ?
            """,
            (config["json_path"], sample_limit, limit),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            value = "" if row["value"] is None else str(row["value"])
            count = int(row["total"] or 0)
            proposal = classify_refinement_value(source_column, value)
            approved_rule = approved_refinement_rule(db, source_column, value, proposal.get("destination", ""))
            item = {
                "source_column": source_column,
                "source_label": config["label"],
                "value": value or "(blank)",
                "count": count,
                "is_approved": approved_rule is not None,
                **proposal,
            }
            if approved_rule is not None:
                item["action"] = "approved"
                item["approved_by"] = approved_rule["approved_by"] or ""
                item["approved_at"] = approved_rule["approved_at"] or ""
            totals["values_reviewed"] += 1
            totals["rows_represented"] += count
            if approved_rule is not None:
                totals["approved_rules"] += 1
            elif proposal["action"] == "review":
                totals["review_candidates"] += 1
            if proposal["category"] == "missing":
                totals["missing_values"] += 1
            if proposal["category"] == "ambiguous":
                totals["ambiguous_values"] += 1
            item["selectable"] = not item["is_approved"] and item["action"] == "review"
            if not refinement_item_matches_filters(item, active_filters):
                continue
            item["rule_payload"] = serialize_refinement_rule(item) if item["selectable"] else ""
            items.append(item)
            totals["visible_items"] += 1
            if item["selectable"]:
                totals["selectable_items"] += 1
        sections.append(
            {
                "source_column": source_column,
                "label": config["label"],
                "primary_destination": config["primary_destination"],
                "items": items,
            }
        )
    return {
        "limit_per_column": limit,
        "sample_rows": sample_limit,
        "filters": active_filters,
        "source_options": [
            {"value": key, "label": config["label"]}
            for key, config in REFINEMENT_SOURCE_COLUMNS.items()
        ],
        "sections": sections,
        "totals": totals,
    }


def refinement_rows_for_export(limit_per_column: int = 200) -> list[dict[str, Any]]:
    dashboard = build_refinement_dashboard(limit_per_column, sample_rows=500000)
    rows: list[dict[str, Any]] = []
    for section in dashboard["sections"]:
        rows.extend(section["items"])
    return rows


def serialize_refinement_rule(item: Mapping[str, Any]) -> str:
    return json.dumps(
        {
            "source_column": item.get("source_column", ""),
            "original_value": item.get("value", ""),
            "category": item.get("category", ""),
            "destination": item.get("destination", ""),
            "proposed_value": item.get("proposed_value", ""),
            "ontology_id": item.get("ontology_id", ""),
            "method": item.get("method", ""),
            "confidence": item.get("confidence", ""),
            "note": item.get("note", ""),
        },
        separators=(",", ":"),
    )


def approve_refinement_rule_payloads(payloads: list[str], approved_by: str, max_rules: int = 1000) -> dict[str, int]:
    approved = 0
    skipped = 0
    failed = 0
    for payload in payloads[:max_rules]:
        try:
            item = json.loads(payload)
            save_approved_standardization_rule(
                source_column=str(item.get("source_column") or "").strip(),
                original_value=str(item.get("original_value") or "").strip(),
                category=str(item.get("category") or "").strip(),
                destination=str(item.get("destination") or "").strip(),
                proposed_value=str(item.get("proposed_value") or "").strip(),
                ontology_id=str(item.get("ontology_id") or "").strip(),
                method=str(item.get("method") or "bulk_review").strip(),
                confidence=str(item.get("confidence") or "medium").strip(),
                note=str(item.get("note") or "").strip(),
                approved_by=approved_by,
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            failed += 1
            continue
        approved += 1
    if len(payloads) > max_rules:
        skipped += len(payloads) - max_rules
    return {"approved": approved, "skipped": skipped, "failed": failed}


def visible_high_confidence_refinement_payloads(filters: Mapping[str, Any], limit: int) -> list[str]:
    dashboard = build_refinement_dashboard(limit, filters=filters)
    payloads: list[str] = []
    for section in dashboard["sections"]:
        for item in section["items"]:
            if item.get("selectable") and item.get("confidence") == "high":
                payloads.append(serialize_refinement_rule(item))
    return payloads


HOST_CURATION_DECISIONS = {
    "all",
    "resolved",
    "not_identifiable",
    "non_host_source",
    "missing",
    "taxonomy_candidate",
    "broad_host",
    "ambiguous",
}


def normalize_host_curation_decision(value: Any, default: str = "ambiguous") -> str:
    text = "" if value is None else str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    aliases = {
        "review_needed": "ambiguous",
        "needs_review": "ambiguous",
        "manual_review": "ambiguous",
        "unmapped": "ambiguous",
        "approve_blank": "missing",
        "blank": "missing",
        "absent": "missing",
        "source": "non_host_source",
        "non_host": "non_host_source",
        "non_host_source": "non_host_source",
        "taxonomy": "taxonomy_candidate",
        "taxonomic_candidate": "taxonomy_candidate",
        "approve_direct": "taxonomy_candidate",
        "approve_genus_level": "taxonomy_candidate",
        "host_organism": "taxonomy_candidate",
        "broad": "broad_host",
        "broad_host": "broad_host",
        "not_identifiable": "not_identifiable",
        "not_identifiable_token": "not_identifiable",
    }
    decision = aliases.get(text, text)
    return decision if decision in HOST_CURATION_DECISIONS else default


def host_curation_live_state(value: Any) -> dict[str, str]:
    standardized = standardize_host_metadata(value)
    method = str(standardized.get("Host_SD_Method") or "")
    host_sd = str(standardized.get("Host_SD") or "")
    taxid = str(standardized.get("Host_TaxID") or "")
    confidence = str(standardized.get("Host_SD_Confidence") or "")
    if taxid:
        if method == "broad_dictionary":
            decision = "broad_host"
        else:
            decision = "taxonomy_candidate"
        return {
            "live_status": "resolved",
            "live_decision": decision,
            "live_host": host_sd,
            "live_taxid": taxid,
            "live_method": method,
            "live_confidence": confidence,
            "needs_review": "0",
        }
    if method in {"missing", "non_host_source", "not_identifiable"}:
        return {
            "live_status": "resolved",
            "live_decision": method,
            "live_host": "",
            "live_taxid": "",
            "live_method": method,
            "live_confidence": confidence or "none",
            "needs_review": "0",
        }
    return {
        "live_status": "pending",
        "live_decision": "ambiguous",
        "live_host": host_sd if method == "unmapped" else "",
        "live_taxid": "",
        "live_method": method or "unmapped",
        "live_confidence": confidence or "none",
        "needs_review": "1",
    }


def host_curation_source_path() -> Path | None:
    candidates = [
        DATA_DIR / "host_manual_review_suggestions.csv",
        STANDARDIZATION_DIR / "host_manual_review_suggestions.csv",
        BASE_DIR / "standardization" / "host_manual_review_suggestions.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def approved_host_curation_rule_lookup(db: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = db.execute(
        """
        SELECT *
        FROM standardization_rules
        WHERE source_column = 'Host'
          AND destination = 'Host_SD'
          AND status = 'approved'
        """
    ).fetchall()
    return {str(row["normalized_value"] or ""): row for row in rows}


def host_curation_cache_path() -> Path:
    return DATA_DIR / "host_curation_live_cache.json"


def host_curation_cache_key(db: sqlite3.Connection, source_path: Path) -> dict[str, Any]:
    source_stat = source_path.stat()
    rule_files = [
        STANDARDIZATION_DIR / "host_synonyms.csv",
        STANDARDIZATION_DIR / "host_negative_rules.csv",
        STANDARDIZATION_DIR / "controlled_categories.csv",
    ]
    rule_file_state = []
    for path in rule_files:
        if path.exists():
            stat = path.stat()
            rule_file_state.append({"path": path.name, "mtime_ns": stat.st_mtime_ns, "size": stat.st_size})
    row = db.execute(
        """
        SELECT COUNT(*) AS total, COALESCE(MAX(approved_at), '') AS latest
        FROM standardization_rules
        WHERE source_column = 'Host'
          AND destination = 'Host_SD'
          AND status = 'approved'
        """
    ).fetchone()
    return {
        "source_path": str(source_path),
        "source_mtime_ns": source_stat.st_mtime_ns,
        "source_size": source_stat.st_size,
        "rule_files": rule_file_state,
        "approved_rule_total": int(row["total"] or 0) if row else 0,
        "approved_rule_latest": str(row["latest"] or "") if row else "",
    }


def load_host_curation_cached_rows(cache_key: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    path = host_curation_cache_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("cache_key") != cache_key:
        return None
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else None


def save_host_curation_cached_rows(cache_key: Mapping[str, Any], rows: list[dict[str, Any]]) -> None:
    path = host_curation_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cache_key": cache_key,
        "created_at": utc_now(),
        "rows": rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def clear_host_curation_cache() -> None:
    host_curation_cache_path().unlink(missing_ok=True)


def host_curation_read_rows() -> list[dict[str, Any]]:
    source_path = host_curation_source_path()
    if source_path is None:
        return []
    db = get_db()
    cache_key = host_curation_cache_key(db, source_path)
    cached_rows = load_host_curation_cached_rows(cache_key)
    if cached_rows is not None:
        return cached_rows
    rows: list[dict[str, Any]] = []
    approved_rules = approved_host_curation_rule_lookup(db)
    with source_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            raw_host = (raw.get("raw_host") or raw.get("host") or raw.get("value") or "").strip()
            if not raw_host:
                continue
            decision = normalize_host_curation_decision(raw.get("decision") or raw.get("review_decision") or "ambiguous")
            count = parse_optional_int(raw.get("count") or raw.get("total_rows") or raw.get("affected_rows")) or 0
            proposed_host = (
                raw.get("proposed_host")
                or raw.get("proposed_host_sd")
                or raw.get("host_standardized")
                or ""
            ).strip()
            taxid = (raw.get("taxid") or raw.get("proposed_taxid") or raw.get("ontology_id") or "").strip()
            confidence = (raw.get("confidence") or ("medium" if decision == "broad_host" else "none")).strip().lower()
            note = (raw.get("note") or raw.get("review_note") or "").strip()
            approved_rule = approved_rules.get(normalize_standardization_lookup(raw_host))
            live_state = host_curation_live_state(raw_host)
            display_decision = decision if live_state["live_status"] == "pending" else live_state["live_decision"]
            rows.append(
                {
                    "raw_host": raw_host,
                    "count": count,
                    "decision": decision,
                    "display_decision": display_decision,
                    "proposed_host": proposed_host,
                    "taxid": taxid,
                    "confidence": confidence,
                    "note": note,
                    **live_state,
                    "is_approved": approved_rule is not None,
                    "approved_by": approved_rule["approved_by"] if approved_rule else "",
                    "approved_at": approved_rule["approved_at"] if approved_rule else "",
                }
            )
    rows.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("raw_host") or "").lower()))
    save_host_curation_cached_rows(cache_key, rows)
    return rows


def host_curation_filters_from_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    decision = normalize_host_curation_decision(values.get("decision") or "all", default="all")
    status = normalize_standardization_lookup(values.get("status") or "pending")
    query = str(values.get("q") or "").strip()
    limit = parse_optional_int(values.get("limit")) or 200
    return {
        "decision": decision if decision in HOST_CURATION_DECISIONS else "all",
        "status": status if status in {"all", "pending", "resolved", "approved", "unapproved"} else "pending",
        "q": query,
        "limit": max(25, min(limit, 2000)),
    }


def host_curation_row_visible(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    if filters["decision"] != "all" and row.get("display_decision") != filters["decision"]:
        return False
    if filters["status"] == "approved" and not row.get("is_approved"):
        return False
    if filters["status"] == "unapproved" and row.get("is_approved"):
        return False
    if filters["status"] == "pending" and row.get("live_status") != "pending":
        return False
    if filters["status"] == "resolved" and row.get("live_status") != "resolved":
        return False
    query = str(filters.get("q") or "").lower()
    if query and query not in str(row.get("raw_host") or "").lower() and query not in str(row.get("proposed_host") or "").lower():
        return False
    return True


def build_host_curation_dashboard(filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    active_filters = host_curation_filters_from_mapping(filters or {})
    all_rows = host_curation_read_rows()
    pending_rows = [row for row in all_rows if row.get("live_status") == "pending"]
    resolved_rows = [row for row in all_rows if row.get("live_status") == "resolved"]
    total_represented_rows = sum(int(row.get("count") or 0) for row in pending_rows)
    source_represented_rows = sum(int(row.get("count") or 0) for row in all_rows)
    resolved_represented_rows = sum(int(row.get("count") or 0) for row in resolved_rows)
    summary = {
        "source_total": len(all_rows),
        "source_total_rows": source_represented_rows,
        "total": len(pending_rows),
        "total_rows": total_represented_rows,
        "resolved_total": len(resolved_rows),
        "resolved_rows": resolved_represented_rows,
        "non_host_source": 0,
        "missing": 0,
        "taxonomy_candidate": 0,
        "broad_host": 0,
        "ambiguous": 0,
        "not_identifiable": 0,
        "approved": 0,
        "visible": 0,
    }
    decision_breakdown = {
        decision: {
            "decision": decision,
            "label": decision.replace("_", " "),
            "distinct_values": 0,
            "represented_rows": 0,
            "approved_values": 0,
            "pending_values": 0,
            "row_percent": 0.0,
        }
        for decision in ["taxonomy_candidate", "broad_host", "non_host_source", "missing", "not_identifiable", "ambiguous"]
    }
    for row in pending_rows:
        decision = row.get("display_decision")
        if decision in summary:
            summary[decision] += 1
        if row.get("is_approved"):
            summary["approved"] += 1
        if decision in decision_breakdown:
            count = int(row.get("count") or 0)
            decision_breakdown[decision]["distinct_values"] += 1
            decision_breakdown[decision]["represented_rows"] += count
            if row.get("is_approved"):
                decision_breakdown[decision]["approved_values"] += 1
            else:
                decision_breakdown[decision]["pending_values"] += 1
    if total_represented_rows:
        for item in decision_breakdown.values():
            item["row_percent"] = round((item["represented_rows"] / total_represented_rows) * 100, 1)
    visible_rows = [row for row in all_rows if host_curation_row_visible(row, active_filters)]
    summary["visible"] = len(visible_rows)
    return {
        "source_path": str(host_curation_source_path() or ""),
        "filters": active_filters,
        "summary": summary,
        "decision_breakdown": list(decision_breakdown.values()),
        "rows": visible_rows[: active_filters["limit"]],
        "total_visible_before_limit": len(visible_rows),
    }


def save_host_curation_decision(row: Mapping[str, Any], action: str, approved_by: str) -> None:
    raw_host = str(row.get("raw_host") or "").strip()
    proposed_host = str(row.get("proposed_host") or "").strip()
    taxid = str(row.get("taxid") or "").strip()
    note = str(row.get("note") or "").strip()
    if action == "exact":
        if not proposed_host or not taxid:
            raise ValueError("Exact host approval requires proposed host and taxid.")
        save_approved_standardization_rule(
            source_column="Host",
            original_value=raw_host,
            category="taxonomy_candidate",
            destination="Host_SD",
            proposed_value=proposed_host,
            ontology_id=taxid,
            method="manual_host_curation",
            confidence="high",
            note=note,
            approved_by=approved_by,
        )
        return
    if action == "broad":
        if not proposed_host or not taxid:
            raise ValueError("Broad host approval requires proposed host and taxid.")
        save_approved_standardization_rule(
            source_column="Host",
            original_value=raw_host,
            category="broad_host",
            destination="Host_SD",
            proposed_value=proposed_host,
            ontology_id=taxid,
            method="manual_broad_host_curation",
            confidence="medium",
            note=note,
            approved_by=approved_by,
        )
        return
    if action == "non_host_source":
        save_approved_standardization_rule(
            source_column="Host",
            original_value=raw_host,
            category="non_host_source",
            destination="Host_SD",
            proposed_value="",
            ontology_id="",
            method="non_host_source",
            confidence="none",
            note=note,
            approved_by=approved_by,
        )
        return
    if action == "missing":
        save_approved_standardization_rule(
            source_column="Host",
            original_value=raw_host,
            category="missing",
            destination="Host_SD",
            proposed_value="",
            ontology_id="",
            method="missing",
            confidence="none",
            note=note,
            approved_by=approved_by,
        )
        return
    raise ValueError("Unsupported host curation action.")


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def read_metric_csv(path: Path) -> dict[str, int]:
    metrics: dict[str, int] = {}
    for row in read_csv_dicts(path):
        key = str(row.get("metric") or "").strip()
        value = parse_optional_int(row.get("value")) or 0
        if key:
            metrics[key] = value
    return metrics


def append_review_rule(path: Path, fieldnames: list[str], row: Mapping[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in fieldnames})


def geography_review_dir() -> Path:
    return DATA_DIR / "geography_review"


def collection_date_review_dir() -> Path:
    return DATA_DIR / "collection_date_review"


def geography_curation_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    review_files = [
        geography_review_dir() / "country_missing_recoverable_primary.csv",
        geography_review_dir() / "country_missing_recoverable_other_columns.csv",
        geography_review_dir() / "geography_secondary_review_queue.csv",
    ]
    for path in review_files:
        for row in read_csv_dicts(path):
            source_value = str(row.get("example_value") or "").strip()
            suggested_country = str(row.get("suggested_country") or "").strip()
            if not source_value or not suggested_country:
                continue
            key = (source_value.lower(), suggested_country)
            if key in seen:
                continue
            seen.add(key)
            live_country = reviewed_secondary_geo_country(source_value) or normalize_country_candidate(source_value)
            status = "resolved" if live_country == suggested_country else "pending"
            rows.append(
                {
                    "count": parse_optional_int(row.get("count")) or 0,
                    "source_column": str(row.get("source_column") or "").strip(),
                    "source_value": source_value,
                    "suggested_value": suggested_country,
                    "live_value": live_country or "",
                    "status": status,
                    "decision": "recoverable_country",
                    "note": str(row.get("review_note") or "").strip(),
                }
            )
    rows.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("source_value") or "").lower()))
    return rows


def build_geography_curation_dashboard(filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    filters = filters or {}
    status = normalize_standardization_lookup(filters.get("status") or "pending")
    if status not in {"pending", "all", "resolved"}:
        status = "pending"
    limit = max(25, min(parse_optional_int(filters.get("limit")) or 200, 2000))
    metrics = read_metric_csv(geography_review_dir() / "geography_audit_summary.csv")
    raw_rows = read_csv_dicts(geography_review_dir() / "country_raw_values.csv")
    rows = geography_curation_rows()
    visible_rows = [row for row in rows if status == "all" or row["status"] == status]
    missing_raw = 0
    resolved_raw = 0
    unresolved_raw = 0
    raw_represented_rows = 0
    for row in raw_rows:
        count = parse_optional_int(row.get("count")) or 0
        raw_represented_rows += count
        raw_country = row.get("raw_country")
        if metadata_value_is_missing(raw_country):
            missing_raw += 1
        elif normalize_country_candidate(raw_country):
            resolved_raw += 1
        else:
            unresolved_raw += 1
    pending_rows = [row for row in rows if row["status"] == "pending"]
    resolved_rows = [row for row in rows if row["status"] == "resolved"]
    return {
        "filters": {"status": status, "limit": limit},
        "source_path": str(geography_review_dir()),
        "summary": {
            "files_scanned": metrics.get("files_scanned", 0),
            "rows_scanned": metrics.get("rows_scanned", 0),
            "country_present_and_mapped": metrics.get("country_present_and_mapped", 0),
            "country_missing_or_unmapped": metrics.get("country_missing_or_unmapped", 0),
            "recoverable_total": len(rows),
            "pending_total": len(pending_rows),
            "resolved_total": len(resolved_rows),
            "pending_rows_represented": sum(int(row.get("count") or 0) for row in pending_rows),
            "raw_country_values": len(raw_rows),
            "raw_country_rows": raw_represented_rows,
            "resolved_raw_values": resolved_raw,
            "missing_raw_values": missing_raw,
            "unresolved_raw_values": unresolved_raw,
            "continent_mismatches": metrics.get("continent_mismatch_when_country_known", 0),
            "subcontinent_mismatches": metrics.get("subcontinent_mismatch_when_country_known", 0),
        },
        "rows": visible_rows[:limit],
        "total_visible_before_limit": len(visible_rows),
    }


def collection_date_curation_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    review_files = [
        collection_date_review_dir() / "collection_date_recoverable_primary.csv",
        collection_date_review_dir() / "collection_date_recoverable_secondary.csv",
        collection_date_review_dir() / "collection_date_secondary_review_queue.csv",
    ]
    for path in review_files:
        for row in read_csv_dicts(path):
            source_value = str(row.get("example_value") or "").strip()
            suggested_year = str(row.get("suggested_year") or "").strip()
            if not source_value or not suggested_year:
                continue
            key = (source_value.lower(), suggested_year)
            if key in seen:
                continue
            seen.add(key)
            live = reviewed_collection_year(source_value) or standardize_collection_year_value(source_value)
            status = "resolved" if live == suggested_year else "pending"
            rows.append(
                {
                    "count": parse_optional_int(row.get("count")) or 0,
                    "source_column": str(row.get("source_column") or "").strip(),
                    "source_value": source_value,
                    "suggested_value": suggested_year,
                    "live_value": live or "",
                    "status": status,
                    "decision": "recoverable_year",
                    "note": str(row.get("review_note") or row.get("recovery_status") or "").strip(),
                }
            )
    rows.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("source_value") or "").lower()))
    return rows


def build_collection_date_curation_dashboard(filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    filters = filters or {}
    status = normalize_standardization_lookup(filters.get("status") or "pending")
    if status not in {"pending", "all", "resolved"}:
        status = "pending"
    limit = max(25, min(parse_optional_int(filters.get("limit")) or 200, 2000))
    metrics = read_metric_csv(collection_date_review_dir() / "collection_date_audit_summary.csv")
    comprehensive_rows = read_csv_dicts(DATA_DIR / "collection_date_comprehensive_review.csv")
    review_rows = collection_date_curation_rows()
    visible_rows = [row for row in review_rows if status == "all" or row["status"] == status]
    status_counts: dict[str, int] = {}
    status_rows: dict[str, int] = {}
    for row in comprehensive_rows:
        review_status = str(row.get("review_status") or "unknown").strip() or "unknown"
        count = parse_optional_int(row.get("count")) or 0
        status_counts[review_status] = status_counts.get(review_status, 0) + 1
        status_rows[review_status] = status_rows.get(review_status, 0) + count
    pending_rows = [row for row in review_rows if row["status"] == "pending"]
    resolved_rows = [row for row in review_rows if row["status"] == "resolved"]
    return {
        "filters": {"status": status, "limit": limit},
        "source_path": str(collection_date_review_dir()),
        "summary": {
            "files_scanned": metrics.get("files_scanned", 0),
            "rows_scanned": metrics.get("rows_scanned", 0),
            "collection_date_present_and_mapped": metrics.get("collection_date_present_and_mapped", 0),
            "collection_date_missing_or_unmapped": metrics.get("collection_date_missing_or_unmapped", 0),
            "recoverable_total": len(review_rows),
            "pending_total": len(pending_rows),
            "resolved_total": len(resolved_rows),
            "pending_rows_represented": sum(int(row.get("count") or 0) for row in pending_rows),
            "raw_values": len(comprehensive_rows),
            "standardized_values": status_counts.get("standardized", 0),
            "standardized_rows": status_rows.get("standardized", 0),
            "missing_values": status_counts.get("missing", 0),
            "missing_rows": status_rows.get("missing", 0),
            "review_status_counts": status_counts,
            "review_status_rows": status_rows,
        },
        "rows": visible_rows[:limit],
        "total_visible_before_limit": len(visible_rows),
    }


def save_geography_curation_rule(source_value: str, country: str, note: str) -> None:
    if not source_value.strip() or country not in COUNTRY_MAPPING:
        raise ValueError("Geography approval requires source value and a recognized country.")
    append_review_rule(
        DATA_DIR / "geography_reviewed_rules.csv",
        ["source_value", "country", "note"],
        {"source_value": source_value.strip(), "country": country.strip(), "note": note.strip()},
    )
    REVIEWED_SECONDARY_GEO_VALUES[re.sub(r"\s+", " ", source_value).strip().lower()] = country.strip()


def save_collection_date_curation_rule(source_value: str, year: str, note: str) -> None:
    current_year = datetime.now(timezone.utc).year
    if not source_value.strip() or not re.fullmatch(r"(?:19|20)\d{2}", year) or not (1900 <= int(year) <= current_year):
        raise ValueError("Collection date approval requires source value and a valid year.")
    append_review_rule(
        DATA_DIR / "collection_date_reviewed_rules.csv",
        ["source_value", "year", "note"],
        {"source_value": source_value.strip(), "year": year.strip(), "note": note.strip()},
    )
    REVIEWED_COLLECTION_DATE_VALUES[re.sub(r"\s+", " ", source_value).strip().lower()] = year.strip()


def taxon_needs_host_refinement(species_id: int) -> bool:
    with get_sqlite_connection() as db:
        row = db.execute(
            """
            SELECT 1
            FROM assembly_metadata
            WHERE species_id = ?
              AND (
                  json_type(row_json, '$."Host_SD"') IS NULL
                  OR json_type(row_json, '$."Isolation_Source_SD"') IS NULL
                  OR json_type(row_json, '$."Environment_Medium_SD"') IS NULL
                  OR json_type(row_json, '$."Sample_Type_SD"') IS NULL
              )
            LIMIT 1
            """,
            (species_id,),
        ).fetchone()
    return row is not None


def refine_taxon_host_standardization(species: SpeciesRecord) -> int:
    rows_by_accession = load_taxon_metadata_rows(species.id)
    if not rows_by_accession:
        return 0
    rows = [ensure_managed_metadata_schema(row) for row in rows_by_accession.values()]
    save_taxon_metadata_rows(species.id, rows, refreshed_at=utc_now())
    metadata_path, clean_path, clean_count = write_taxon_metadata_outputs(
        species.slug,
        rows,
        dataset_version_id=species.staging_dataset_version_id,
    )
    latest = load_species(species.id)
    latest.metadata_path = metadata_path
    latest.metadata_clean_path = clean_path
    latest.genome_count = latest.genome_count or clean_count
    latest.updated_at = utc_now()
    save_species(latest)
    return clean_count


def refine_ready_host_standardization(limit: int = 25) -> dict[str, int]:
    refined = 0
    skipped = 0
    failed = 0
    rows = get_db().execute(
        """
        SELECT s.*
        FROM species s
        WHERE s.metadata_status = 'ready'
          AND s.metadata_clean_path IS NOT NULL
          AND EXISTS (
                SELECT 1
                FROM assembly_metadata am
                WHERE am.species_id = s.id
                AND (
                    json_type(am.row_json, '$."Host_SD"') IS NULL
                    OR json_type(am.row_json, '$."Isolation_Source_SD"') IS NULL
                    OR json_type(am.row_json, '$."Environment_Medium_SD"') IS NULL
                    OR json_type(am.row_json, '$."Sample_Type_SD"') IS NULL
                )
              LIMIT 1
          )
        ORDER BY
          CASE WHEN s.taxon_rank = 'species' THEN 0 ELSE 1 END,
          COALESCE(s.genome_count, 2147483647) ASC,
          s.species_name COLLATE NOCASE ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in rows:
        if refined >= limit:
            break
        species = row_to_species(row)
        if not taxon_needs_host_refinement(species.id):
            skipped += 1
            continue
        try:
            refine_taxon_host_standardization(species)
            refined += 1
        except Exception:
            failed += 1
            logging.exception("Failed to refine host standardization for %s.", species.species_name)
    return {"refined": refined, "skipped": skipped, "failed": failed}


def queue_standardization_refresh_for_ready_taxa(
    *,
    limit: int | None = None,
    dry_run: bool = False,
    rank_scope: str = "genus",
    changed_since: str | None = None,
) -> dict[str, Any]:
    rank_scope = str(rank_scope or "genus").strip().lower()
    if rank_scope not in {"genus", "species", "all"}:
        rank_scope = "genus"
    rank_clause = ""
    params: list[Any] = []
    if rank_scope != "all":
        rank_clause = "AND taxon_rank = ?"
        params.append(rank_scope)
    changed_clause = ""
    if changed_since:
        changed_clause = "AND metadata_last_built_at IS NOT NULL AND metadata_last_built_at >= ?"
        params.append(changed_since)
    rows = get_db().execute(
        f"""
        SELECT id, species_name, taxon_rank, genome_count, metadata_clean_path, metadata_last_built_at
        FROM species
        WHERE status = 'ready'
          AND metadata_status = 'ready'
          AND metadata_clean_path IS NOT NULL
          {rank_clause}
          {changed_clause}
        ORDER BY
          CASE WHEN taxon_rank = 'genus' THEN 0 ELSE 1 END,
          COALESCE(genome_count, 0) DESC,
          species_name COLLATE NOCASE ASC
        """,
        tuple(params),
    ).fetchall()
    eligible: list[sqlite3.Row] = []
    skipped = 0
    for row in rows:
        clean_path = str(row["metadata_clean_path"] or "")
        if not clean_path or not Path(clean_path).exists():
            skipped += 1
            continue
        eligible.append(row)
        if limit is not None and len(eligible) >= limit:
            break
    if dry_run:
        return {
            "eligible": len(eligible),
            "queued": 0,
            "running": 0,
            "skipped": skipped,
            "estimated_rows": sum(int(row["genome_count"] or 0) for row in eligible),
            "rank_scope": rank_scope,
            "changed_since": changed_since,
        }

    now = utc_now()
    queued = 0
    running = 0
    with get_sqlite_connection() as db:
        for row in eligible:
            species_id = int(row["id"])
            existing = db.execute(
                "SELECT status FROM standardization_refresh_tasks WHERE species_id = ?",
                (species_id,),
            ).fetchone()
            if existing is not None and str(existing["status"]) == "running":
                running += 1
                continue
            db.execute(
                """
                INSERT INTO standardization_refresh_tasks (
                    species_id, status, requested_at, claimed_by, claimed_at,
                    completed_at, total_rows, updated_rows, error
                )
                VALUES (?, 'pending', ?, NULL, NULL, NULL, 0, 0, NULL)
                ON CONFLICT(species_id) DO UPDATE SET
                    status = 'pending',
                    requested_at = excluded.requested_at,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = NULL,
                    total_rows = 0,
                    updated_rows = 0,
                    error = NULL
                """,
                (species_id, now),
            )
            task_row = db.execute(
                "SELECT id FROM standardization_refresh_tasks WHERE species_id = ?",
                (species_id,),
            ).fetchone()
            if task_row is not None:
                db.execute(
                    "DELETE FROM standardization_refresh_chunks WHERE task_id = ?",
                    (int(task_row["id"]),),
                )
            queued += 1
        db.commit()
    return {
        "eligible": len(eligible),
        "queued": queued,
        "running": running,
        "skipped": skipped,
        "estimated_rows": sum(int(row["genome_count"] or 0) for row in eligible),
        "rank_scope": rank_scope,
        "changed_since": changed_since,
    }


def claim_next_standardization_refresh_task(worker_name: str) -> dict[str, Any] | None:
    now = utc_now()
    with get_sqlite_connection() as db:
        stale_rows = db.execute(
            """
            SELECT id, claimed_by
            FROM standardization_refresh_tasks
            WHERE status IN ('running', 'finalizing')
              AND claimed_by IS NOT NULL
            """
        ).fetchall()
        stale_ids = [
            int(row["id"])
            for row in stale_rows
            if not worker_heartbeat_is_live(str(row["claimed_by"]))
        ]
        if stale_ids:
            for start in range(0, len(stale_ids), SQLITE_VARIABLE_CHUNK_SIZE):
                chunk = stale_ids[start : start + SQLITE_VARIABLE_CHUNK_SIZE]
                placeholders = ", ".join("?" for _ in chunk)
                db.execute(
                    f"""
                    UPDATE standardization_refresh_tasks
                    SET status = 'pending',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        error = NULL
                    WHERE id IN ({placeholders})
                    """,
                    tuple(chunk),
                )
            db.commit()
        pending_chunk_row = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM standardization_refresh_chunks
            WHERE status = 'pending'
            """
        ).fetchone()
        if int(pending_chunk_row["total"] or 0) > 0:
            return None
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT s.*, t.id AS task_id
            FROM standardization_refresh_tasks t
            JOIN species s ON s.id = t.species_id
            WHERE t.status = 'pending'
              AND s.status = 'ready'
              AND s.metadata_status = 'ready'
              AND s.metadata_clean_path IS NOT NULL
              AND (
                  SELECT COUNT(*)
                  FROM assembly_metadata am
                  WHERE am.species_id = s.id
              ) < ?
            ORDER BY
              CASE WHEN s.taxon_rank = 'genus' THEN 0 ELSE 1 END,
              COALESCE(s.genome_count, 0) DESC,
              t.requested_at ASC
            LIMIT 1
            """,
            (STANDARDIZATION_PARALLEL_CHUNK_MIN_ROWS,),
        ).fetchone()
        if row is None:
            db.commit()
            return None
        staging_version_id = active_pipeline_dataset_version(db)
        updated = db.execute(
            """
            UPDATE standardization_refresh_tasks
            SET status = 'running',
                claimed_by = ?,
                claimed_at = ?,
                completed_at = NULL,
                error = NULL
            WHERE id = ?
              AND status = 'pending'
            """,
            (worker_name, now, int(row["task_id"])),
        ).rowcount
        if updated:
            db.execute(
                """
                UPDATE species
                SET staging_dataset_version_id = COALESCE(staging_dataset_version_id, ?)
                WHERE id = ?
                """,
                (staging_version_id, int(row["id"])),
            )
        db.commit()
    if not updated:
        return None
    with get_sqlite_connection() as db:
        species = get_species_by_id(int(row["id"]), db)
    return {"task_id": int(row["task_id"]), "species": species or row_to_species(row)}


def mark_standardization_refresh_task_failed(task_id: int, error: str) -> None:
    now = utc_now()
    with get_sqlite_connection() as db:
        db.execute(
            """
            UPDATE standardization_refresh_tasks
            SET status = 'failed',
                claimed_by = NULL,
                claimed_at = NULL,
                completed_at = ?,
                error = ?
            WHERE id = ?
            """,
            (now, error[:2000], task_id),
        )
        db.commit()


def ensure_standardization_chunks_for_pending_tasks() -> int:
    prepared = 0
    now = utc_now()
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute(
            """
            SELECT
                t.id AS task_id,
                s.id AS species_id,
                s.species_name,
                COALESCE(s.genome_count, 0) AS genome_count,
                (
                    SELECT COUNT(*)
                    FROM assembly_metadata am
                    WHERE am.species_id = s.id
                ) AS stored_row_total
            FROM standardization_refresh_tasks t
            JOIN species s ON s.id = t.species_id
            WHERE t.status = 'pending'
              AND s.status = 'ready'
              AND s.metadata_status = 'ready'
              AND s.metadata_clean_path IS NOT NULL
            ORDER BY COALESCE(s.genome_count, 0) DESC, t.requested_at ASC
            LIMIT 8
            """,
        ).fetchall()
        for row in rows:
            task_id = int(row["task_id"])
            species_id = int(row["species_id"])
            stored_row_total = int(row["stored_row_total"] or 0)
            if stored_row_total < STANDARDIZATION_PARALLEL_CHUNK_MIN_ROWS:
                continue
            existing = db.execute(
                "SELECT COUNT(*) AS total FROM standardization_refresh_chunks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if int(existing["total"] or 0) > 0:
                db.execute(
                    """
                    UPDATE standardization_refresh_tasks
                    SET status = 'chunking',
                        total_rows = ?,
                        updated_rows = 0,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        completed_at = NULL,
                        error = NULL
                    WHERE id = ?
                    """,
                    (stored_row_total, task_id),
                )
                prepared += 1
                continue
            chunk_rows = []
            for chunk_index, start_offset in enumerate(
                range(0, stored_row_total, STANDARDIZATION_PARALLEL_CHUNK_SIZE)
            ):
                end_offset = min(start_offset + STANDARDIZATION_PARALLEL_CHUNK_SIZE, stored_row_total)
                chunk_rows.append(
                    (
                        task_id,
                        species_id,
                        chunk_index,
                        start_offset,
                        end_offset,
                    )
                )
            db.executemany(
                """
                INSERT OR IGNORE INTO standardization_refresh_chunks (
                    task_id, species_id, chunk_index, start_offset, end_offset
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                chunk_rows,
            )
            db.execute(
                """
                UPDATE standardization_refresh_tasks
                SET status = 'chunking',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = NULL,
                    total_rows = ?,
                    updated_rows = 0,
                    error = NULL
                WHERE id = ?
                """,
                (stored_row_total, task_id),
            )
            prepared += 1
            logging.info(
                "Prepared %s standardization chunks for %s (%s rows).",
                len(chunk_rows),
                row["species_name"],
                stored_row_total,
            )
        db.commit()
    return prepared


def release_stale_standardization_chunk_claims_for_dead_workers(db: sqlite3.Connection) -> int:
    rows = db.execute(
        """
        SELECT id, claimed_by
        FROM standardization_refresh_chunks
        WHERE status = 'running'
          AND claimed_by IS NOT NULL
        """
    ).fetchall()
    stale_ids = [
        int(row["id"])
        for row in rows
        if not worker_heartbeat_is_live(str(row["claimed_by"]))
    ]
    if not stale_ids:
        return 0
    for start in range(0, len(stale_ids), SQLITE_VARIABLE_CHUNK_SIZE):
        chunk = stale_ids[start : start + SQLITE_VARIABLE_CHUNK_SIZE]
        placeholders = ", ".join("?" for _ in chunk)
        db.execute(
            f"""
            UPDATE standardization_refresh_chunks
            SET status = 'pending',
                claimed_by = NULL,
                claimed_at = NULL,
                error = NULL
            WHERE id IN ({placeholders})
            """,
            tuple(chunk),
        )
    return len(stale_ids)


def claim_next_standardization_refresh_chunk(worker_name: str) -> dict[str, Any] | None:
    ensure_standardization_chunks_for_pending_tasks()
    now = utc_now()
    with get_sqlite_connection() as db:
        released = release_stale_standardization_chunk_claims_for_dead_workers(db)
        if released:
            db.commit()
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT
                s.*,
                c.id AS chunk_id,
                c.task_id AS chunk_task_id,
                c.species_id AS chunk_species_id,
                c.chunk_index AS chunk_index,
                c.start_offset AS chunk_start_offset,
                c.end_offset AS chunk_end_offset,
                c.status AS chunk_status,
                c.claimed_by AS chunk_claimed_by,
                c.claimed_at AS chunk_claimed_at,
                c.completed_at AS chunk_completed_at,
                c.total_rows AS chunk_total_rows,
                c.updated_rows AS chunk_updated_rows,
                c.error AS chunk_error
            FROM standardization_refresh_chunks c
            JOIN standardization_refresh_tasks t ON t.id = c.task_id
            JOIN species s ON s.id = c.species_id
            WHERE c.status = 'pending'
              AND t.status = 'chunking'
              AND s.status = 'ready'
              AND s.metadata_status = 'ready'
            ORDER BY
              COALESCE(s.genome_count, 0) DESC,
              c.task_id ASC,
              c.chunk_index ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None
        staging_version_id = active_pipeline_dataset_version(db)
        updated = db.execute(
            """
            UPDATE standardization_refresh_chunks
            SET status = 'running',
                claimed_by = ?,
                claimed_at = ?,
                completed_at = NULL,
                error = NULL
            WHERE id = ?
              AND status = 'pending'
            """,
            (worker_name, now, int(row["chunk_id"])),
        ).rowcount
        if updated:
            db.execute(
                """
                UPDATE species
                SET staging_dataset_version_id = COALESCE(staging_dataset_version_id, ?)
                WHERE id = ?
                """,
                (staging_version_id, int(row["chunk_species_id"])),
            )
        db.commit()
    if not updated:
        return None
    chunk = {
        "id": int(row["chunk_id"]),
        "task_id": int(row["chunk_task_id"]),
        "species_id": int(row["chunk_species_id"]),
        "chunk_index": int(row["chunk_index"]),
        "start_offset": int(row["chunk_start_offset"]),
        "end_offset": int(row["chunk_end_offset"]),
        "status": str(row["chunk_status"]),
        "claimed_by": row["chunk_claimed_by"],
        "claimed_at": row["chunk_claimed_at"],
        "completed_at": row["chunk_completed_at"],
        "total_rows": int(row["chunk_total_rows"] or 0),
        "updated_rows": int(row["chunk_updated_rows"] or 0),
        "error": row["chunk_error"],
    }
    with get_sqlite_connection() as db:
        species = get_species_by_id(int(row["chunk_species_id"]), db)
    return {"chunk": chunk, "species": species or row_to_species(row)}


def finalize_standardization_refresh_task_if_ready(task_id: int, worker_name: str) -> bool:
    now = utc_now()
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        task_row = db.execute(
            """
            SELECT s.*, t.id AS task_id, t.status AS task_status
            FROM standardization_refresh_tasks t
            JOIN species s ON s.id = t.species_id
            WHERE t.id = ?
            """,
            (task_id,),
        ).fetchone()
        if task_row is None or str(task_row["task_status"]) != "chunking":
            db.commit()
            return False
        chunk_status = {
            str(row["status"]): int(row["total"] or 0)
            for row in db.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM standardization_refresh_chunks
                WHERE task_id = ?
                GROUP BY status
                """,
                (task_id,),
            ).fetchall()
        }
        if chunk_status.get("failed", 0):
            db.execute(
                """
                UPDATE standardization_refresh_tasks
                SET status = 'failed',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    error = 'One or more standardization chunks failed.'
                WHERE id = ?
                """,
                (now, task_id),
            )
            db.commit()
            return False
        if chunk_status.get("pending", 0) or chunk_status.get("running", 0):
            db.commit()
            return False
        updated = db.execute(
            """
            UPDATE standardization_refresh_tasks
            SET status = 'finalizing',
                claimed_by = ?,
                claimed_at = ?,
                error = NULL
            WHERE id = ?
              AND status = 'chunking'
            """,
            (worker_name, now, task_id),
        ).rowcount
        db.commit()
    if not updated:
        return False

    species = row_to_species(task_row)
    try:
        rows_by_accession = load_taxon_metadata_rows(species.id)
        # Chunks already persist normalized rows. Avoid re-writing the full taxon
        # during finalization; for very large genera this can dominate runtime.
        rows = list(rows_by_accession.values())
        metadata_path, clean_path, clean_count = write_taxon_metadata_outputs(
            species.slug,
            rows,
            normalize_rows=False,
            dataset_version_id=species.staging_dataset_version_id,
        )
        try:
            refresh_metadata_species_search_entries(species, rows)
        except Exception:
            logging.exception("Failed to refresh metadata species search entries for taxon %s.", species.id)
        with get_sqlite_connection() as db:
            totals = db.execute(
                """
                SELECT COALESCE(SUM(total_rows), 0) AS total_rows,
                       COALESCE(SUM(updated_rows), 0) AS updated_rows
                FROM standardization_refresh_chunks
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
            done_at = utc_now()
            db.execute(
                """
                UPDATE species
                SET metadata_path = ?,
                    metadata_clean_path = ?,
                    genome_count = COALESCE(genome_count, ?),
                    metadata_last_built_at = COALESCE(metadata_last_built_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (metadata_path, clean_path, clean_count, done_at, done_at, species.id),
            )
            db.execute(
                """
                UPDATE standardization_refresh_tasks
                SET status = 'done',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    total_rows = ?,
                    updated_rows = ?,
                    error = NULL
                WHERE id = ?
                """,
                (
                    done_at,
                    int(totals["total_rows"] or 0),
                    min(int(totals["updated_rows"] or 0), clean_count),
                    task_id,
                ),
            )
            db.commit()
        return True
    except Exception as exc:
        mark_standardization_refresh_task_failed(task_id, str(exc))
        raise


def process_standardization_refresh_chunk(payload: dict[str, Any], worker_name: str) -> int:
    chunk = payload["chunk"]
    species = payload["species"]
    assert isinstance(species, SpeciesRecord)
    chunk_id = int(chunk["id"])
    task_id = int(chunk["task_id"])
    start_offset = max(0, int(chunk["start_offset"]))
    end_offset = max(start_offset, int(chunk["end_offset"]))
    try:
        rows = load_taxon_metadata_row_chunk(
            species.id,
            limit=end_offset - start_offset,
            offset=start_offset,
        )
        refreshed_at = utc_now()
        updated_rows = upsert_taxon_metadata_rows(
            species.id,
            rows,
            refreshed_at=refreshed_at,
        )
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE standardization_refresh_chunks
                SET status = 'done',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    total_rows = ?,
                    updated_rows = ?,
                    error = NULL
                WHERE id = ?
                """,
                (utc_now(), len(rows), updated_rows, chunk_id),
            )
            db.commit()
        logging.info(
            "Standardized chunk %s for %s (%s-%s), updated %s rows.",
            chunk["chunk_index"],
            species.species_name,
            start_offset,
            end_offset,
            updated_rows,
        )
        finalize_standardization_refresh_task_if_ready(task_id, worker_name)
        return updated_rows
    except Exception as exc:
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE standardization_refresh_chunks
                SET status = 'failed',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (utc_now(), str(exc)[:2000], chunk_id),
            )
            db.execute(
                """
                UPDATE standardization_refresh_tasks
                SET status = 'failed',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (utc_now(), f"Chunk {chunk.get('chunk_index')} failed: {str(exc)[:1800]}", task_id),
            )
            db.commit()
        raise


def apply_current_standardization_to_taxon(task: dict[str, Any]) -> int:
    task_id = int(task["task_id"])
    species = task["species"]
    assert isinstance(species, SpeciesRecord)
    try:
        stored_row_total = count_taxon_metadata_rows(species.id)
        if not stored_row_total:
            raise RuntimeError("No stored metadata rows are available for this taxon.")
        refreshed_at = utc_now()

        if stored_row_total >= STANDARDIZATION_CHUNK_MIN_ROWS:
            updated_total = 0
            for offset in range(0, stored_row_total, STANDARDIZATION_CHUNK_SIZE):
                chunk_rows = load_taxon_metadata_row_chunk(
                    species.id,
                    limit=STANDARDIZATION_CHUNK_SIZE,
                    offset=offset,
                )
                if not chunk_rows:
                    continue
                updated_total += upsert_taxon_metadata_rows(
                    species.id,
                    chunk_rows,
                    refreshed_at=refreshed_at,
                )
                logging.info(
                    "Standardized %s rows for %s (%s/%s).",
                    len(chunk_rows),
                    species.species_name,
                    min(offset + len(chunk_rows), stored_row_total),
                    stored_row_total,
                )
            rows_by_accession = load_taxon_metadata_rows(species.id)
            # Rows were normalized and persisted chunk-by-chunk above.
            rows = list(rows_by_accession.values())
        else:
            rows_by_accession = load_taxon_metadata_rows(species.id)
            rows = [ensure_managed_metadata_schema(row) for row in rows_by_accession.values()]
            save_taxon_metadata_rows(species.id, rows, refreshed_at=refreshed_at, normalize_rows=False)
            updated_total = len(rows)

        metadata_path, clean_path, clean_count = write_taxon_metadata_outputs(
            species.slug,
            rows,
            normalize_rows=False,
            dataset_version_id=species.staging_dataset_version_id,
        )
        try:
            refresh_metadata_species_search_entries(species, rows)
        except Exception:
            logging.exception("Failed to refresh metadata species search entries for taxon %s.", species.id)
        now = utc_now()
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE species
                SET metadata_path = ?,
                    metadata_clean_path = ?,
                    genome_count = COALESCE(genome_count, ?),
                    metadata_last_built_at = COALESCE(metadata_last_built_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (metadata_path, clean_path, clean_count, now, now, species.id),
            )
            db.execute(
                """
                UPDATE standardization_refresh_tasks
                SET status = 'done',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_at = ?,
                    total_rows = ?,
                    updated_rows = ?,
                    error = NULL
                WHERE id = ?
                """,
                (now, stored_row_total, min(updated_total, clean_count), task_id),
            )
            db.commit()
        return clean_count
    except Exception as exc:
        mark_standardization_refresh_task_failed(task_id, str(exc))
        raise


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


def csv_has_columns(path: Path, columns: list[str]) -> bool:
    if not path.exists():
        return False
    try:
        header = pd.read_csv(path, nrows=0).columns
    except Exception:
        return False
    column_set = set(str(column) for column in header)
    return all(column in column_set for column in columns)


def json_object_path(key: str) -> str:
    escaped = str(key).replace("\\", "\\\\").replace('"', '\\"')
    return f'$."{escaped}"'


def taxon_has_assembly_feature_columns(species: SpeciesRecord) -> bool:
    clean_path = Path(species.metadata_clean_path or "")
    if not csv_has_columns(clean_path, ASSEMBLY_FEATURE_COLUMNS):
        return False
    return taxon_stored_metadata_rows_have_columns(species.id, ASSEMBLY_FEATURE_COLUMNS)


def taxon_stored_metadata_rows_have_columns(species_id: int, columns: list[str]) -> bool:
    if not columns:
        return True
    with get_sqlite_connection() as db:
        row = db.execute(
            "SELECT COUNT(*) AS total FROM assembly_metadata WHERE species_id = ?",
            (species_id,),
        ).fetchone()
        total = int(row["total"] or 0) if row is not None else 0
        if total <= 0:
            return False
        for column in columns:
            missing = db.execute(
                """
                SELECT 1
                FROM assembly_metadata
                WHERE species_id = ?
                  AND json_type(row_json, ?) IS NULL
                LIMIT 1
                """,
                (species_id, json_object_path(column)),
            ).fetchone()
            if missing is not None:
                return False
    return True


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
ANALYSIS_VALUE_CANONICALIZERS = {
    "Sample_Type_SD": {
        "pure culture": "pure/single culture",
    },
}


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
    canonicalizer = ANALYSIS_VALUE_CANONICALIZERS.get(field)
    if canonicalizer:
        series = series.map(lambda value: canonicalizer.get(str(value).strip().lower(), value))
    counts = series.value_counts().head(limit)
    return [(str(index), int(value)) for index, value in counts.items()]


def metadata_analysis_field(frame: pd.DataFrame, standardized_field: str, raw_field: str) -> str:
    """Prefer standardized columns for analysis when they contain usable values."""
    if standardized_field in frame.columns and metadata_present_unknown_absent(frame, standardized_field)["present"] > 0:
        return standardized_field
    return raw_field


def metadata_standardized_coverage(frame: pd.DataFrame, raw_field: str, standardized_field: str, label: str) -> dict[str, Any]:
    raw = metadata_present_unknown_absent(frame, raw_field)
    standardized = metadata_present_unknown_absent(frame, standardized_field)
    recovered = max(0, int(standardized["present"]) - int(raw["present"]))
    return {
        "label": label,
        "raw_field": raw_field,
        "standardized_field": standardized_field,
        "raw_present": raw["present"],
        "raw_percent": raw["present_percent"],
        "standardized_present": standardized["present"],
        "standardized_percent": standardized["present_percent"],
        "recovered": recovered,
    }


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
        "quality_gate": {
            "active_quality_jobs": count_active_quality_jobs(),
            "max_active_quality_jobs": MAX_ACTIVE_QUALITY_JOBS,
            "min_available_memory_label": format_bytes(MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES),
            "max_memory_percent": MAX_MEMORY_PERCENT_FOR_QUALITY,
            "accepting_quality_jobs": not quality_submission_blockers(),
        },
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
    for field in ["Collection Date", "Geographic Location", "Host_SD", "Isolation_Source_SD", "Sample_Type_SD", "Environment_Medium_SD"]:
        row = completeness_map.get(field)
        if not row:
            continue
        label = field.replace("_", " ")
        insights.append(
            f"{label} is informative for {row['present']:,} genomes ({row['present_percent']}%), "
            f"unknown for {row['unknown']:,}, and absent for {row['absent']:,}."
        )

    top_hosts = metadata_value_counts(frame, metadata_analysis_field(frame, "Host_SD", "Host"), limit=3)
    if top_hosts:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_hosts)
        insights.append(f"The most represented standardized hosts are {rendered}.")

    top_countries = metadata_value_counts(frame, "Country", limit=3)
    if top_countries:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_countries)
        insights.append(f"The leading country annotations are {rendered}.")

    if year_start and year_end:
        insights.append(
            f"Collection years currently span from {year_start} to {year_end}, enabling temporal comparisons."
        )

    standardized_pairs = [
        ("Host", "Host_SD", "host"),
        ("Isolation Source", "Isolation_Source_SD", "isolation-source"),
        ("Sample Type", "Sample_Type_SD", "sample-type"),
        ("Environment Medium", "Environment_Medium_SD", "environment-medium"),
    ]
    recovery_fragments: list[str] = []
    for raw_field, standardized_field, label in standardized_pairs:
        if raw_field not in frame.columns or standardized_field not in frame.columns:
            continue
        coverage = metadata_standardized_coverage(frame, raw_field, standardized_field, label)
        if coverage["recovered"] > 0:
            recovery_fragments.append(f"{coverage['recovered']:,} additional {label} rows")
    if recovery_fragments:
        insights.append(
            "Standardized metadata recovery adds " + ", ".join(recovery_fragments) + " beyond the directly populated raw fields."
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


def positive_percent_text(value: Any) -> str:
    text = normalize_metadata_value(value)
    number = parse_optional_float(text)
    if number is None or number <= 0 or number > 100:
        return ""
    return text


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
            options = build_sequence_filter_option_list(frame, config["column"])
            if not options:
                continue
            fields.append(
                {
                    "key": key,
                    "label": config["label"],
                    "column": config["column"],
                    "options": options,
                }
            )
        if fields:
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
        "host_sd",
        "isolation_source_sd",
        "sample_type_sd",
        "environment_local_sd",
        "environment_medium_sd",
        "assembly_level",
    ]
    primary_filter_keys = [key for key in primary_filter_keys if key in filter_field_map]
    advanced_filter_keys = [
        key
        for key in [
            "host",
            "host_rank",
            "host_class",
            "host_order",
            "host_family",
            "host_genus",
            "host_species",
            "host_confidence",
            "host_method",
            "host_disease_sd",
            "host_disease",
            "host_health_state_sd",
            "isolation_source",
            "sample_type",
            "isolation_site_sd",
            "environment_broad_sd",
            "environment_broad",
            "environment_local",
            "environment_medium",
        ]
        if key in filter_field_map and key not in primary_filter_keys
    ]
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
    top_hosts = analysis.get("top_standardized_hosts") or analysis.get("top_hosts") or []
    top_sources = analysis.get("top_standardized_sources") or analysis.get("top_sources") or []
    top_sample_types = analysis.get("top_standardized_sample_types") or []
    top_environment_media = analysis.get("top_standardized_environment_media") or []
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
            f"In parallel, standardized host annotations are dominated by {rendered}, highlighting the principal biological contexts represented "
            f"in the stored dataset and defining the clearest axes for host-associated comparison."
        )
    if top_sources:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_sources[:3])
        paragraphs.append(
            f"Likewise, standardized isolation-source metadata most frequently records {rendered}, showing that the current assembly set is "
            f"anchored around a relatively small number of recurrent sampling origins."
        )
    if top_sample_types:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_sample_types[:3])
        paragraphs.append(
            f"Standardized sample-type analysis further identifies {rendered} as the leading sampled material classes, "
            f"making heterogeneous BioSample descriptors easier to compare across genomes."
        )
    if top_environment_media:
        rendered = ", ".join(f"{name} ({count})" for name, count in top_environment_media[:3])
        paragraphs.append(
            f"Environmental-medium standardization highlights {rendered}, adding a controlled view of sampled matrices beyond the raw environment fields."
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

    def add_categorical_bar(
        key: str,
        field: str,
        title: str,
        scale: str,
        *,
        limit: int = 12,
        label: str | None = None,
    ) -> None:
        if field not in frame.columns:
            return
        values = frame[field].astype(str).str.strip()
        values = values[
            values.ne("")
            & ~values.str.lower().isin({"absent", "unknown", "not provided", "not applicable", "missing", "none", "nan"})
        ]
        if values.empty:
            return
        canonicalizer = ANALYSIS_VALUE_CANONICALIZERS.get(field)
        if canonicalizer:
            values = values.map(lambda value: canonicalizer.get(str(value).strip().lower(), value))
        counts = values.value_counts().head(limit).reset_index()
        display_label = label or field
        counts.columns = [display_label, "Genomes"]
        add_plot(
            key,
            px.bar(
                counts,
                x="Genomes",
                y=display_label,
                orientation="h",
                title=title,
                color="Genomes",
                color_continuous_scale=scale,
                text="Genomes",
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin={"l": 20, "r": 20, "t": 60, "b": 20},
            ),
        )

    def add_standardized_coverage_plot(key: str, rows: list[dict[str, Any]], title: str) -> None:
        if not rows:
            return
        coverage_frame = pd.DataFrame(rows)
        if coverage_frame.empty:
            return
        melted = coverage_frame.melt(
            id_vars=["label"],
            value_vars=["raw_present", "standardized_present"],
            var_name="Layer",
            value_name="Genomes",
        )
        melted["Layer"] = melted["Layer"].replace(
            {
                "raw_present": "Raw field",
                "standardized_present": "Standardized field",
            }
        )
        add_plot(
            key,
            px.bar(
                melted,
                x="Genomes",
                y="label",
                color="Layer",
                barmode="group",
                orientation="h",
                title=title,
                text="Genomes",
                color_discrete_map={"Raw field": "#b8844d", "Standardized field": "#165c4e"},
                labels={"label": "Metadata field"},
            ).update_traces(textposition="outside", cliponaxis=False).update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin={"l": 20, "r": 20, "t": 60, "b": 20},
            ),
        )

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

    host_analysis_field = metadata_analysis_field(frame, "Host_SD", "Host")
    add_categorical_bar(
        "host_bar",
        host_analysis_field,
        "Top standardized host annotations" if host_analysis_field == "Host_SD" else "Top host annotations",
        "Sunsetdark",
        label="Host",
    )
    add_categorical_bar("host_raw_bar", "Host", "Raw host annotations", "Sunsetdark", label="Raw host")
    add_categorical_bar("host_rank_bar", "Host_Rank", "Host taxonomic rank distribution", "Tealgrn", label="Host rank")
    add_categorical_bar("host_superkingdom_bar", "Host_Superkingdom", "Host superkingdom distribution", "Tealgrn", label="Host superkingdom")
    add_categorical_bar("host_phylum_bar", "Host_Phylum", "Host phylum distribution", "Tealgrn", label="Host phylum")
    add_categorical_bar("host_class_bar", "Host_Class", "Host class distribution", "Emrld", label="Host class")
    add_categorical_bar("host_order_bar", "Host_Order", "Host order distribution", "Viridis", label="Host order")
    add_categorical_bar("host_family_bar", "Host_Family", "Host family distribution", "Teal", label="Host family")
    add_categorical_bar("host_genus_bar", "Host_Genus", "Host genus distribution", "Mint", label="Host genus")
    add_categorical_bar("host_species_bar", "Host_Species", "Host species distribution", "Sunsetdark", label="Host species")
    add_categorical_bar("host_confidence_bar", "Host_SD_Confidence", "Host standardization confidence", "Blues", label="Confidence")
    add_categorical_bar("host_method_bar", "Host_SD_Method", "Host standardization method", "Mint", label="Method")

    for key, standardized_field, raw_field, title, scale, label in [
        ("source_bar", "Isolation_Source_SD", "Isolation Source", "Top standardized isolation sources", "Mint", "Isolation source"),
        ("environment_bar", "Environment_Broad_Scale_SD", "Environment (Broad Scale)", "Standardized broad environmental contexts", "Teal", "Broad environment"),
        ("sample_type_bar", "Sample_Type_SD", "Sample Type", "Standardized sample type distribution", "Burg", "Sample type"),
        ("environment_local_bar", "Environment_Local_Scale_SD", "Environment (Local Scale)", "Standardized local environmental contexts", "Emrld", "Local environment"),
        ("environment_medium_bar", "Environment_Medium_SD", "Environment Medium", "Standardized environmental medium distribution", "Darkmint", "Environmental medium"),
        ("isolation_site_bar", "Isolation_Site_SD", "Isolation Site", "Standardized isolation site distribution", "Tealgrn", "Isolation site"),
    ]:
        add_categorical_bar(
            key,
            metadata_analysis_field(frame, standardized_field, raw_field),
            title,
            scale,
            label=label,
        )

    for key, field, title, scale in [
        ("source_raw_bar", "Isolation Source", "Raw isolation sources", "Mint"),
        ("sample_type_raw_bar", "Sample Type", "Raw sample types", "Burg"),
        ("environment_medium_raw_bar", "Environment Medium", "Raw environmental medium", "Darkmint"),
        ("environment_broad_raw_bar", "Environment (Broad Scale)", "Raw broad environment", "Teal"),
        ("environment_local_raw_bar", "Environment (Local Scale)", "Raw local environment", "Emrld"),
        ("assembly_level_bar", "Assembly Level", "Assembly level distribution", "Blues"),
    ]:
        add_categorical_bar(key, field, title, scale)

    add_categorical_bar(
        "host_disease_bar",
        metadata_analysis_field(frame, "Host_Disease_SD", "Host Disease"),
        "Standardized host disease distribution",
        "Sunset",
        label="Host disease",
    )
    add_categorical_bar("host_health_state_bar", "Host_Health_State_SD", "Host health-state distribution", "Teal", label="Host health state")
    add_categorical_bar("host_disease_raw_bar", "Host Disease", "Raw host disease distribution", "Sunset", label="Raw host disease")
    add_categorical_bar("host_health_state_raw_bar", "Host Health State", "Raw host health-state distribution", "Teal", label="Raw host health state")

    add_standardized_coverage_plot(
        "standardized_context_coverage",
        [
            metadata_standardized_coverage(frame, "Host", "Host_SD", "Host"),
            metadata_standardized_coverage(frame, "Isolation Source", "Isolation_Source_SD", "Isolation source"),
            metadata_standardized_coverage(frame, "Sample Type", "Sample_Type_SD", "Sample type"),
            metadata_standardized_coverage(frame, "Environment Medium", "Environment_Medium_SD", "Environment medium"),
            metadata_standardized_coverage(frame, "Environment (Broad Scale)", "Environment_Broad_Scale_SD", "Broad environment"),
            metadata_standardized_coverage(frame, "Environment (Local Scale)", "Environment_Local_Scale_SD", "Local environment"),
        ],
        "Raw vs standardized metadata coverage",
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
        "Host_SD",
        "Host_TaxID",
        "Geographic Location",
        "Country",
        "Continent",
        "Subcontinent",
        "Isolation Source",
        "Isolation_Source_SD",
        "Collection Date",
        "Sample Type",
        "Sample_Type_SD",
        "Environment (Broad Scale)",
        "Environment_Broad_Scale_SD",
        "Environment (Local Scale)",
        "Environment_Local_Scale_SD",
        "Environment Medium",
        "Environment_Medium_SD",
        "Isolation_Site_SD",
        "Host_Disease_SD",
        "Host_Health_State_SD",
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
        "top_hosts": summarize_top_values(rows, metadata_analysis_field(frame, "Host_SD", "Host")),
        "top_sources": summarize_top_values(rows, metadata_analysis_field(frame, "Isolation_Source_SD", "Isolation Source")),
        "top_standardized_hosts": metadata_value_counts(frame, "Host_SD"),
        "top_standardized_sources": metadata_value_counts(frame, "Isolation_Source_SD"),
        "top_standardized_sample_types": metadata_value_counts(frame, "Sample_Type_SD"),
        "top_standardized_environment_media": metadata_value_counts(frame, "Environment_Medium_SD"),
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
        "distinct_standardized_host_count": metadata_distinct_count(frame, "Host_SD"),
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
        "top_standardized_hosts": metadata_value_counts(frame, "Host_SD"),
        "top_host_ranks": metadata_value_counts(frame, "Host_Rank"),
        "top_host_classes": metadata_value_counts(frame, "Host_Class"),
        "top_host_orders": metadata_value_counts(frame, "Host_Order"),
        "host_methods": metadata_value_counts(frame, "Host_SD_Method"),
        "host_confidences": metadata_value_counts(frame, "Host_SD_Confidence"),
        "top_countries": summarize_top_values(rows, "Country"),
        "top_continents": metadata_value_counts(frame, "Continent"),
        "top_sources": summarize_top_values(rows, "Isolation Source"),
        "top_standardized_sources": metadata_value_counts(frame, "Isolation_Source_SD"),
        "top_sample_types": summarize_top_values(rows, "Sample Type"),
        "top_standardized_sample_types": metadata_value_counts(frame, "Sample_Type_SD"),
        "top_environments": summarize_top_values(rows, "Environment (Broad Scale)"),
        "top_standardized_environment_broad": metadata_value_counts(frame, "Environment_Broad_Scale_SD"),
        "top_standardized_environment_local": metadata_value_counts(frame, "Environment_Local_Scale_SD"),
        "top_standardized_environment_media": metadata_value_counts(frame, "Environment_Medium_SD"),
        "top_standardized_isolation_sites": metadata_value_counts(frame, "Isolation_Site_SD"),
        "top_standardized_host_diseases": metadata_value_counts(frame, "Host_Disease_SD"),
        "top_standardized_host_health_states": metadata_value_counts(frame, "Host_Health_State_SD"),
        "assembly_levels": summarize_top_values(rows, "Assembly Level"),
        "assembly_statuses": summarize_top_values(rows, "Assembly Status"),
        "top_subcontinents": metadata_value_counts(frame, "Subcontinent"),
        "top_host_diseases": metadata_value_counts(frame, "Host Disease"),
        "standardized_coverage": [
            metadata_standardized_coverage(frame, "Host", "Host_SD", "Host"),
            metadata_standardized_coverage(frame, "Isolation Source", "Isolation_Source_SD", "Isolation source"),
            metadata_standardized_coverage(frame, "Sample Type", "Sample_Type_SD", "Sample type"),
            metadata_standardized_coverage(frame, "Environment Medium", "Environment_Medium_SD", "Environment medium"),
            metadata_standardized_coverage(frame, "Environment (Broad Scale)", "Environment_Broad_Scale_SD", "Broad environment"),
            metadata_standardized_coverage(frame, "Environment (Local Scale)", "Environment_Local_Scale_SD", "Local environment"),
        ],
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
    if requested_rank == "genus" and is_bacterial_root_scope(scope):
        try:
            return fetch_bacterial_genera_from_local_taxonomy()
        except Exception:
            logging.exception("Local taxonkit genus discovery failed; falling back to NCBI datasets taxonomy discovery.")
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


def fetch_bacterial_genera_from_local_taxonomy() -> list[tuple[str, int | None]]:
    result = subprocess.run(
        [
            TAXONKIT_BINARY,
            "list",
            "--ids",
            "2",
            "-n",
            "-r",
            "--indent",
            "",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(stderr or "taxonkit bacterial genus discovery failed without an error message.")

    candidates: list[tuple[str, int | None]] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        match = re.match(r"^(\d+)\s+\[([^\]]+)\]\s+(.+)$", line)
        if not match:
            continue
        tax_id_text, rank, name = match.groups()
        if rank.strip().lower() != "genus":
            continue
        normalized_name = normalize_species_name(name)
        if not normalized_name:
            continue
        candidates.append((normalized_name, int(tax_id_text)))

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
    staging_dataset_version_id: str | None = None,
    taxon_id: int | None = None,
    query_name: str | None = None,
    queue_catalog: bool = True,
) -> SpeciesRecord:
    normalized = normalize_species_name(species_name)
    if len(normalized) < 3:
        raise ValueError("Taxon name must be at least 3 characters.")
    connection = db or get_db()
    normalized_rank = normalize_taxon_rank(taxon_rank)
    existing = get_taxon_by_name(normalized, normalized_rank, connection)
    if existing is not None:
        if staging_dataset_version_id and existing.staging_dataset_version_id != staging_dataset_version_id:
            existing.staging_dataset_version_id = staging_dataset_version_id
            existing.updated_at = utc_now()
            return save_species(existing, connection)
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
            status="pending" if queue_catalog else "missing",
            created_at=created_at,
            updated_at=created_at,
            query_name=query_name or normalized,
            taxon_id=taxon_id,
            refresh_requested=queue_catalog,
            metadata_status="missing",
            is_live=False if staging_dataset_version_id else True,
            staging_dataset_version_id=staging_dataset_version_id,
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


def request_assembly_feature_backfill(species: SpeciesRecord, db: sqlite3.Connection | None = None) -> None:
    connection = db or get_db()
    now = utc_now()
    connection.execute(
        """
        UPDATE species
        SET assembly_backfill_status = 'pending',
            assembly_backfill_requested_at = ?,
            assembly_backfill_claimed_by = NULL,
            assembly_backfill_claimed_at = NULL,
            assembly_backfill_error = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (now, now, species.id),
    )
    connection.commit()


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
        genus_work_exists = db.execute(
            """
            SELECT 1
            FROM species
            WHERE taxon_rank = 'genus'
              AND status = 'ready'
              AND tsv_path IS NOT NULL
              AND (
                    metadata_status IN ('pending', 'building')
                    OR (metadata_status = 'ready' AND metadata_refresh_requested = 1 AND metadata_path IS NOT NULL)
                  )
            LIMIT 1
            """
        ).fetchone() is not None
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
              AND (
                    taxon_rank = 'genus'
                    OR ? = 0
                  )
            ORDER BY
                CASE
                    WHEN metadata_status = 'pending' THEN 0
                    WHEN metadata_status = 'ready' AND metadata_refresh_requested = 1 THEN 1
                    ELSE 2
                END,
                CASE WHEN taxon_rank = 'genus' THEN 0 ELSE 1 END,
                CASE
                    WHEN metadata_status = 'pending' AND metadata_path IS NULL THEN 0
                    WHEN metadata_status = 'pending' AND metadata_path IS NOT NULL THEN 1
                    ELSE 2
                END,
                CASE WHEN genome_count IS NULL THEN 1 ELSE 0 END,
                COALESCE(genome_count, 2147483647) ASC,
                updated_at ASC,
                created_at ASC
            LIMIT 24
            """,
            (1 if genus_work_exists else 0,),
        ).fetchall()
        if not rows:
            db.commit()
            return None

        claimed_row: sqlite3.Row | None = None
        for row in rows:
            if not metadata_lock_is_available(int(row["id"])):
                continue
            claimed_at = utc_now()
            staging_version_id = active_pipeline_dataset_version(db)
            has_existing_metadata = bool(row["metadata_path"])
            metadata_status = "building" if row["metadata_status"] == "pending" else "ready"
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
                    updated_at = ?,
                    staging_dataset_version_id = COALESCE(staging_dataset_version_id, ?)
                WHERE id = ?
                  AND metadata_claimed_at IS NULL
                  AND (
                        metadata_status = 'pending'
                        OR (metadata_status = 'ready' AND metadata_refresh_requested = 1 AND metadata_path IS NOT NULL)
                      )
                """,
                (metadata_status, worker_name, claimed_at, claimed_at, claimed_at, staging_version_id, row["id"]),
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


def ensure_metadata_chunks_for_species(species: SpeciesRecord) -> int:
    if species.taxon_rank != "genus" or not species.tsv_path:
        return 0
    if species.genome_count is not None and species.genome_count < METADATA_CHUNK_MIN_ROWS:
        return 0
    existing_count = 0
    with get_sqlite_connection() as db:
        row = db.execute(
            "SELECT COUNT(*) AS total FROM metadata_chunks WHERE species_id = ?",
            (species.id,),
        ).fetchone()
        existing_count = int(row["total"] or 0) if row is not None else 0
    if existing_count:
        return existing_count

    rows = load_filtered_taxon_tsv_rows(species)
    total_rows = len(rows)
    if total_rows < METADATA_CHUNK_MIN_ROWS:
        return 0
    now = utc_now()
    chunk_rows = []
    for chunk_index, start_offset in enumerate(range(0, total_rows, METADATA_CHUNK_SIZE)):
        end_offset = min(start_offset + METADATA_CHUNK_SIZE, total_rows)
        chunk_rows.append(
            (
                species.id,
                chunk_index,
                start_offset,
                end_offset,
                "pending",
                end_offset - start_offset,
                0,
                now,
                now,
            )
        )
    with get_sqlite_connection() as db:
        db.executemany(
            """
            INSERT OR IGNORE INTO metadata_chunks (
                species_id, chunk_index, start_offset, end_offset,
                status, total_rows, completed_rows, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            chunk_rows,
        )
        db.commit()
        row = db.execute(
            "SELECT COUNT(*) AS total FROM metadata_chunks WHERE species_id = ?",
            (species.id,),
        ).fetchone()
    created_count = int(row["total"] or 0) if row is not None else 0
    logging.info(
        "Prepared %s metadata helper chunks for %s (%s rows).",
        created_count,
        species.species_name,
        total_rows,
    )
    return created_count


def ensure_metadata_chunks_for_active_builds() -> None:
    with get_sqlite_connection() as db:
        rows = db.execute(
            """
            SELECT s.*
            FROM species AS s
            LEFT JOIN (
                SELECT species_id, COUNT(*) AS chunk_count
                FROM metadata_chunks
                GROUP BY species_id
            ) AS c ON c.species_id = s.id
            WHERE s.metadata_status = 'building'
              AND s.status = 'ready'
              AND s.taxon_rank = 'genus'
              AND s.tsv_path IS NOT NULL
              AND COALESCE(s.genome_count, 0) >= ?
              AND COALESCE(c.chunk_count, 0) = 0
            ORDER BY COALESCE(s.metadata_progress_total, s.genome_count, 2147483647) DESC
            LIMIT 6
            """,
            (METADATA_CHUNK_MIN_ROWS,),
        ).fetchall()
    for row in rows:
        if row["tsv_path"] and not Path(str(row["tsv_path"])).exists():
            continue
        species = row_to_species(row)
        try:
            ensure_metadata_chunks_for_species(species)
        except Exception:
            logging.exception("Failed to prepare metadata helper chunks for %s.", species.species_name)


def release_stale_metadata_chunk_claims_for_dead_workers(db: sqlite3.Connection) -> int:
    rows = db.execute(
        """
        SELECT id, claimed_by
        FROM metadata_chunks
        WHERE status = 'running'
          AND claimed_by IS NOT NULL
        """
    ).fetchall()
    stale_ids = [
        int(row["id"])
        for row in rows
        if row["claimed_by"] and not worker_heartbeat_is_live(str(row["claimed_by"]))
    ]
    if not stale_ids:
        return 0
    placeholders = ", ".join("?" for _ in stale_ids)
    now = utc_now()
    cursor = db.execute(
        f"""
        UPDATE metadata_chunks
        SET status = 'pending',
            claimed_by = NULL,
            claimed_at = NULL,
            error = 'Stale metadata helper claim from a dead worker was reset.',
            updated_at = ?
        WHERE id IN ({placeholders})
        """,
        (now, *stale_ids),
    )
    return int(cursor.rowcount or 0)


def claim_next_metadata_chunk(worker_name: str) -> sqlite3.Row | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        release_stale_metadata_chunk_claims_for_dead_workers(db)
        now = utc_now()
        db.execute(
            """
            UPDATE metadata_chunks
            SET status = 'done',
                completed_rows = total_rows,
                claimed_by = NULL,
                claimed_at = NULL,
                error = NULL,
                updated_at = ?
            WHERE status = 'pending'
              AND end_offset <= (
                    SELECT COALESCE(metadata_progress_completed, 0)
                    FROM species
                    WHERE species.id = metadata_chunks.species_id
                )
            """,
            (now,),
        )
        row = db.execute(
            """
            SELECT c.*
            FROM metadata_chunks AS c
            JOIN species AS s ON s.id = c.species_id
            WHERE c.status = 'pending'
              AND s.metadata_status = 'building'
              AND s.status = 'ready'
              AND s.taxon_rank = 'genus'
              AND s.tsv_path IS NOT NULL
              AND c.start_offset >= COALESCE(s.metadata_progress_completed, 0) + ?
            ORDER BY
                COALESCE(s.metadata_progress_total, s.genome_count, 2147483647)
                    - COALESCE(s.metadata_progress_completed, 0) ASC,
                s.updated_at ASC,
                c.species_id ASC,
                c.chunk_index ASC
            LIMIT 1
            """,
            (METADATA_CHUNK_PROGRESS_BUFFER_ROWS,),
        ).fetchone()
        if row is None:
            db.commit()
            return None
        cursor = db.execute(
            """
            UPDATE metadata_chunks
            SET status = 'running',
                claimed_by = ?,
                claimed_at = ?,
                error = NULL,
                updated_at = ?
            WHERE id = ?
              AND status = 'pending'
              AND claimed_by IS NULL
            """,
            (worker_name, now, now, row["id"]),
        )
        if int(cursor.rowcount or 0) != 1:
            db.commit()
            return None
        db.commit()
        return row


def process_metadata_chunk(chunk: sqlite3.Row) -> None:
    species = get_species_by_id(int(chunk["species_id"]))
    if species is None:
        return
    now = utc_now()
    try:
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE metadata_chunks
                SET status = 'running',
                    updated_at = ?
                WHERE id = ?
                """,
                (now, chunk["id"]),
            )
            db.commit()
        tsv_rows = load_filtered_taxon_tsv_rows(species)
        total_rows = len(tsv_rows)
        start_offset = max(0, int(chunk["start_offset"]))
        end_offset = min(total_rows, max(start_offset, int(chunk["end_offset"])))
        chunk_rows = tsv_rows[start_offset:end_offset]
        logging.info(
            "Metadata helper started %s chunk %s (%s-%s).",
            species.species_name,
            chunk["chunk_index"],
            start_offset,
            end_offset,
        )
        accessions = [metadata_row_accession(row) for row in chunk_rows]
        stored_rows = load_taxon_metadata_rows_for_accessions(species.id, accessions)
        rows_to_fetch = [
            row
            for row in chunk_rows
            if (accession := metadata_row_accession(row)) and accession not in stored_rows
        ]
        if rows_to_fetch:
            biosample_ids = [
                biosample_id
                for biosample_id in {metadata_row_biosample_accession(row) for row in rows_to_fetch}
                if biosample_id
            ]
            cached_biosample_records, _stale_or_missing_biosamples = load_cached_biosample_records(biosample_ids)
            biosample_records = dict(cached_biosample_records)
            missing_biosample_ids = [
                biosample_id
                for biosample_id in biosample_ids
                if biosample_id not in biosample_records
            ]
            fetched_records = fetch_uncached_biosample_records(missing_biosample_ids)
            if fetched_records:
                biosample_records.update(fetched_records)
                save_biosample_cache_records(
                    {
                        biosample_id: (record, biosample_record_has_data(record))
                        for biosample_id, record in fetched_records.items()
                    }
                )
            enriched_rows = [
                enrich_tsv_row_with_biosample_metadata(
                    row,
                    biosample_records.get(metadata_row_biosample_accession(row)),
                )
                for row in rows_to_fetch
            ]
            upsert_taxon_metadata_rows(species.id, enriched_rows, refreshed_at=now)
            with get_sqlite_connection() as db:
                db.execute(
                    """
                    UPDATE metadata_chunks
                    SET updated_at = ?
                    WHERE id = ?
                    """,
                    (utc_now(), chunk["id"]),
                )
                db.commit()

        stored_total = min(count_taxon_metadata_rows(species.id), total_rows)
        update_taxon_metadata_progress(species.id, total=total_rows, completed=stored_total)
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE metadata_chunks
                SET status = 'done',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    completed_rows = ?,
                    error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (len(chunk_rows), utc_now(), chunk["id"]),
            )
            db.commit()
        logging.info(
            "Metadata helper completed %s chunk %s (%s-%s), fetched %s missing rows.",
            species.species_name,
            chunk["chunk_index"],
            start_offset,
            end_offset,
            len(rows_to_fetch),
        )
    except Exception as exc:
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE metadata_chunks
                SET status = 'failed',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(exc)[:4000], utc_now(), chunk["id"]),
            )
            db.commit()
        raise


def claim_next_assembly_feature_backfill(worker_name: str) -> SpeciesRecord | None:
    with get_sqlite_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        stale_rows = db.execute(
            """
            SELECT id, assembly_backfill_claimed_by
            FROM species
            WHERE assembly_backfill_status = 'running'
              AND assembly_backfill_claimed_by IS NOT NULL
            """
        ).fetchall()
        for stale_row in stale_rows:
            claimed_by = str(stale_row["assembly_backfill_claimed_by"] or "")
            if claimed_by and worker_heartbeat_is_live(claimed_by):
                continue
            db.execute(
                """
                UPDATE species
                SET assembly_backfill_status = 'pending',
                    assembly_backfill_claimed_by = NULL,
                    assembly_backfill_claimed_at = NULL,
                    assembly_backfill_error = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND assembly_backfill_status = 'running'
                """,
                (utc_now(), stale_row["id"]),
            )
        row = db.execute(
            """
            SELECT *
            FROM species
            WHERE assembly_backfill_status = 'pending'
              AND status = 'ready'
              AND metadata_clean_path IS NOT NULL
              AND metadata_status = 'ready'
              AND assembly_backfill_claimed_at IS NULL
            ORDER BY
                CASE WHEN taxon_rank = 'genus' THEN 0 ELSE 1 END,
                CASE WHEN genome_count IS NULL THEN 1 ELSE 0 END,
                COALESCE(genome_count, 2147483647) ASC,
                assembly_backfill_requested_at ASC,
                updated_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None
        claimed_at = utc_now()
        cursor = db.execute(
            """
            UPDATE species
            SET assembly_backfill_status = 'running',
                assembly_backfill_claimed_by = ?,
                assembly_backfill_claimed_at = ?,
                updated_at = ?
            WHERE id = ?
              AND assembly_backfill_status = 'pending'
              AND assembly_backfill_claimed_at IS NULL
            """,
            (worker_name, claimed_at, claimed_at, row["id"]),
        )
        if int(cursor.rowcount or 0) != 1:
            db.commit()
            return None
        db.commit()
        claimed = get_species_by_id(int(row["id"]), db)
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
                CASE WHEN taxon_rank = 'genus' THEN 0 ELSE 1 END,
                updated_at ASC,
                created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None

        claimed_at = utc_now()
        staging_version_id = active_pipeline_dataset_version(db)
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
                claim_token = claim_token + 1,
                staging_dataset_version_id = COALESCE(staging_dataset_version_id, ?)
            WHERE id = ?
              AND claimed_at IS NULL
              AND (
                    status = 'pending'
                    OR (status = 'ready' AND refresh_requested = 1 AND tsv_path IS NOT NULL)
                  )
            """,
            (status, claimed_at, worker_name, claimed_at, claimed_at, staging_version_id, row["id"]),
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


def is_no_catalog_data_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "no annotations matching your query" in message
        or "no genome data is currently available" in message
        or "no genome assemblies were found" in message
        or "no assemblies were found" in message
    )


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
        output_path = species_tsv_path(current.slug, current.staging_dataset_version_id)
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
        if is_no_catalog_data_error(exc):
            current.status = "ready" if has_existing_tsv else "no_data"
            current.sync_error = str(exc)
        elif current.sync_attempt_count < SPECIES_MAX_AUTO_RETRIES:
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


def merge_assembly_features_into_metadata_rows(
    stored_rows: dict[str, dict[str, Any]],
    refreshed_tsv_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    refreshed_by_accession = {
        metadata_row_accession(row): row
        for row in refreshed_tsv_rows
        if metadata_row_accession(row)
    }
    merged_rows: list[dict[str, Any]] = []
    for accession, stored_row in stored_rows.items():
        merged = dict(stored_row)
        refreshed = refreshed_by_accession.get(accession)
        if refreshed:
            for column in SPECIES_TSV_COLUMNS:
                value = refreshed.get(column)
                if value not in (None, ""):
                    merged[column] = normalize_json_scalar(value)
        for column in SPECIES_TSV_COLUMNS:
            merged.setdefault(column, None)
        merged_rows.append(merged)
    merged_rows.sort(key=lambda row: str(row.get("Assembly Accession") or ""))
    return merged_rows


def backfill_species_assembly_features(species: SpeciesRecord) -> None:
    try:
        current = load_species(species.id)
        if current.status != "ready" or not current.metadata_clean_path or not Path(current.metadata_clean_path).exists():
            raise RuntimeError("Taxon metadata is not ready for assembly feature backfill.")
        if taxon_has_assembly_feature_columns(current):
            now = utc_now()
            with get_sqlite_connection() as db:
                db.execute(
                    """
                    UPDATE species
                    SET assembly_backfill_status = 'done',
                        assembly_backfill_last_built_at = ?,
                        assembly_backfill_claimed_by = NULL,
                        assembly_backfill_claimed_at = NULL,
                        assembly_backfill_error = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, current.id),
                )
                db.commit()
            return

        refreshed_rows, taxon_id = fetch_species_dataset_rows(current)
        output_path = species_tsv_path(current.slug, current.staging_dataset_version_id)
        write_species_tsv(refreshed_rows, output_path)

        stored_rows = load_taxon_metadata_rows(current.id)
        if not stored_rows and current.metadata_clean_path:
            clean_path = Path(current.metadata_clean_path)
            if clean_path.exists():
                clean_frame = pd.read_csv(clean_path, dtype=str).fillna("")
                for row in clean_frame.to_dict("records"):
                    accession = metadata_row_accession(row)
                    if accession:
                        stored_rows[accession] = row
        if not stored_rows:
            raise RuntimeError("No stored metadata rows are available to merge assembly features.")
        merged_rows = merge_assembly_features_into_metadata_rows(stored_rows, refreshed_rows)
        if not merged_rows:
            raise RuntimeError("No metadata rows matched refreshed assembly accessions.")

        refreshed_at = utc_now()
        save_taxon_metadata_rows(current.id, merged_rows, refreshed_at=refreshed_at)
        metadata_path, clean_path, clean_count = write_taxon_metadata_outputs(
            current.slug,
            merged_rows,
            dataset_version_id=current.staging_dataset_version_id,
        )

        latest = load_species(current.id)
        latest.tsv_path = str(output_path)
        latest.taxon_id = taxon_id or latest.taxon_id
        latest.genome_count = latest.genome_count or clean_count
        latest.metadata_path = metadata_path
        latest.metadata_clean_path = clean_path
        now = utc_now()
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE species
                SET tsv_path = ?,
                    taxon_id = ?,
                    genome_count = COALESCE(genome_count, ?),
                    metadata_path = ?,
                    metadata_clean_path = ?,
                    assembly_backfill_status = 'done',
                    assembly_backfill_last_built_at = ?,
                    assembly_backfill_claimed_by = NULL,
                    assembly_backfill_claimed_at = NULL,
                    assembly_backfill_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(output_path), taxon_id or latest.taxon_id, clean_count, metadata_path, clean_path, now, now, current.id),
            )
            db.commit()
    except Exception as exc:
        now = utc_now()
        with get_sqlite_connection() as db:
            db.execute(
                """
                UPDATE species
                SET assembly_backfill_status = 'failed',
                    assembly_backfill_claimed_by = NULL,
                    assembly_backfill_claimed_at = NULL,
                    assembly_backfill_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(exc), now, species.id),
            )
            db.commit()


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
    target_name = normalize_species_name(organism_name)
    target_parts = target_name.split()
    target_genus = target_parts[1] if target_parts[:1] == ["Candidatus"] and len(target_parts) > 1 else (target_parts[0] if target_parts else None)
    with source_path.open("r", encoding="utf-8", newline="") as src_handle:
        reader = csv.DictReader(src_handle, delimiter=delimiter)
        fieldnames = reader.fieldnames or []
        with output_path.open("w", encoding="utf-8", newline="") as dest_handle:
            writer = csv.DictWriter(dest_handle, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            for row in reader:
                organism_name_value = normalize_species_name(str(row.get("Organism Name") or ""))
                canonical_name = canonical_species_from_organism_name(organism_name_value, target_genus)
                if organism_name_value != target_name and canonical_name != target_name:
                    continue
                writer.writerow(row)
                count += 1
    return count


def derive_species_metadata_from_genus(species: SpeciesRecord) -> tuple[str, str, int, int] | None:
    genus_name = species_parent_genus_name(species)
    if not genus_name:
        return None
    genus = get_taxon_by_name(genus_name, "genus")
    if genus is not None and not species.staging_dataset_version_id:
        genus = public_species_record(genus)
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
            if (
                normalize_species_name(str(row.get("Organism Name") or "")) == species.species_name
                or canonical_species_from_organism_name(str(row.get("Organism Name") or ""), genus_name)
                == species.species_name
            )
        ]
        if filtered_rows:
            save_taxon_metadata_rows(species.id, filtered_rows, refreshed_at=utc_now())
            updated_path, clean_path, clean_count = write_taxon_metadata_outputs(
                species.slug,
                filtered_rows,
                dataset_version_id=species.staging_dataset_version_id,
            )
            return str(updated_path), str(clean_path), int(genus.id), clean_count

    taxon_dir = metadata_taxon_dir(species.slug, species.staging_dataset_version_id)
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


def ensure_species_from_genus_metadata(species_name: str, source_taxon_id: int) -> SpeciesRecord:
    normalized = normalize_species_name(species_name)
    genus = require_public_species(source_taxon_id)
    if genus.taxon_rank != "genus":
        raise ValueError("The source taxon must be a genus.")
    if not species_search_name(normalized).startswith(f"{species_search_name(genus.species_name)} "):
        raise ValueError("The selected species does not belong to the source genus.")
    if not genus.metadata_path or not genus.metadata_clean_path:
        raise ValueError("Source genus metadata is not ready.")

    existing = get_taxon_by_name(normalized, "species")
    if existing is not None:
        public_existing = public_species_record(existing)
        if public_existing is None:
            raise ValueError("That species is currently being updated and is not ready yet.")
        if (
            public_existing.status == "ready"
            and public_existing.tsv_path
            and Path(public_existing.tsv_path).exists()
            and public_existing.metadata_status == "ready"
            and public_existing.metadata_clean_path
            and Path(public_existing.metadata_clean_path).exists()
        ):
            return public_existing

    species = create_species(normalized, taxon_rank="species", assembly_source=genus.assembly_source)
    if (
        species.status == "ready"
        and species.tsv_path
        and Path(species.tsv_path).exists()
        and species.metadata_status == "ready"
        and species.metadata_clean_path
        and Path(species.metadata_clean_path).exists()
    ):
        return species

    output_path = species_tsv_path(species.slug)
    updated_count = write_filtered_metadata_file(Path(genus.metadata_path), output_path, normalized)
    derived = derive_species_metadata_from_genus(species)
    if updated_count <= 0 or derived is None:
        raise ValueError("No matching genomes were found in the source genus metadata.")

    metadata_path, clean_path, source_taxon_id, clean_count = derived
    latest = load_species(species.id)
    latest.tsv_path = str(output_path)
    latest.status = "ready"
    latest.genome_count = clean_count
    latest.metadata_status = "ready"
    latest.metadata_path = metadata_path
    latest.metadata_clean_path = clean_path
    latest.metadata_source_taxon_id = source_taxon_id
    latest.metadata_last_built_at = utc_now()
    latest.metadata_error = None
    latest.refresh_requested = False
    latest.metadata_refresh_requested = False
    latest.claimed_by = None
    latest.claimed_at = None
    latest.metadata_claimed_by = None
    latest.metadata_claimed_at = None
    latest.updated_at = latest.metadata_last_built_at
    save_species(latest)
    return latest


def save_species_metadata_from_genus_rows(
    species: SpeciesRecord,
    genus: SpeciesRecord,
    filtered_rows: list[dict[str, Any]],
) -> SpeciesRecord:
    if not filtered_rows:
        raise ValueError("No matching genomes were found in the source genus metadata.")
    save_taxon_metadata_rows(species.id, filtered_rows, refreshed_at=utc_now())
    metadata_path, clean_path, clean_count = write_taxon_metadata_outputs(
        species.slug,
        filtered_rows,
        dataset_version_id=species.staging_dataset_version_id,
    )
    latest = load_species(species.id)
    latest.tsv_path = metadata_path
    latest.status = "ready"
    latest.genome_count = clean_count
    latest.metadata_status = "ready"
    latest.metadata_path = metadata_path
    latest.metadata_clean_path = clean_path
    latest.metadata_source_taxon_id = genus.id
    latest.metadata_last_built_at = utc_now()
    latest.metadata_error = None
    latest.refresh_requested = False
    latest.metadata_refresh_requested = False
    latest.claimed_by = None
    latest.claimed_at = None
    latest.metadata_claimed_by = None
    latest.metadata_claimed_at = None
    latest.updated_at = latest.metadata_last_built_at
    save_species(latest)
    return latest


def expand_species_catalog_from_genus_metadata(
    limit: int | None = None,
    *,
    staging_dataset_version_id: str | None = None,
) -> dict[str, int]:
    with get_sqlite_connection() as db:
        if staging_dataset_version_id:
            rows = db.execute(
                """
                SELECT source_taxon_id, source_taxon_name, species_name, genome_count
                FROM metadata_species_search_staging
                WHERE dataset_version_id = ?
                ORDER BY source_taxon_name COLLATE NOCASE ASC, genome_count DESC, species_name COLLATE NOCASE ASC
                """,
                (staging_dataset_version_id,),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT source_taxon_id, source_taxon_name, species_name, genome_count
                FROM metadata_species_search
                ORDER BY source_taxon_name COLLATE NOCASE ASC, genome_count DESC, species_name COLLATE NOCASE ASC
                """
            ).fetchall()

    candidates: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows:
        source_taxon_id = int(row["source_taxon_id"])
        source_taxon_name = str(row["source_taxon_name"])
        canonical_name = canonical_species_from_organism_name(str(row["species_name"]), source_taxon_name)
        if not canonical_name:
            continue
        key = (source_taxon_id, canonical_name)
        entry = candidates.setdefault(
            key,
            {
                "source_taxon_id": source_taxon_id,
                "source_taxon_name": source_taxon_name,
                "species_name": canonical_name,
                "genome_count": 0,
            },
        )
        entry["genome_count"] += int(row["genome_count"] or 0)

    created = 0
    updated = 0
    skipped = 0
    failed = 0
    processed = 0
    ordered_candidates = sorted(
        candidates.values(),
        key=lambda item: (str(item["source_taxon_name"]).lower(), -int(item["genome_count"]), str(item["species_name"]).lower()),
    )
    candidates_by_genus: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for candidate in ordered_candidates:
        candidates_by_genus[int(candidate["source_taxon_id"])].append(candidate)

    for source_taxon_id, genus_candidates in sorted(
        candidates_by_genus.items(),
        key=lambda item: str(item[1][0]["source_taxon_name"]).lower() if item[1] else "",
    ):
        if limit is not None and processed >= limit:
            break
        try:
            genus = load_species(source_taxon_id)
            genus_rows = load_taxon_metadata_rows(genus.id)
            rows_by_canonical: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in genus_rows.values():
                canonical_name = canonical_species_from_organism_name(str(row.get("Organism Name") or ""), genus.species_name)
                if canonical_name:
                    rows_by_canonical[canonical_name].append(row)
        except Exception:
            failed += len(genus_candidates)
            logging.exception("Failed to load genus metadata for species expansion: %s.", source_taxon_id)
            continue

        for candidate in genus_candidates:
            if limit is not None and processed >= limit:
                break
            species_name = str(candidate["species_name"])
            existing = get_taxon_by_name(species_name, "species")
            if (
                existing is not None
                and existing.status == "ready"
                and existing.metadata_status == "ready"
                and existing.metadata_clean_path
                and Path(existing.metadata_clean_path).exists()
            ):
                skipped += 1
                continue
            filtered_rows = rows_by_canonical.get(species_name, [])
            if not filtered_rows:
                failed += 1
                continue
            try:
                was_existing = existing is not None
                species = create_species(
                    species_name,
                    taxon_rank="species",
                    assembly_source=genus.assembly_source,
                    staging_dataset_version_id=staging_dataset_version_id,
                )
                if staging_dataset_version_id:
                    species.staging_dataset_version_id = staging_dataset_version_id
                    if not species.live_tsv_path:
                        species.is_live = False
                    species = save_species(species)
                save_species_metadata_from_genus_rows(species, genus, filtered_rows)
                if was_existing:
                    updated += 1
                else:
                    created += 1
                processed += 1
            except Exception:
                failed += 1
                logging.exception("Failed to derive species catalog entry for %s.", species_name)
    return {
        "candidate_total": len(ordered_candidates),
        "processed": processed,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
    }


def write_taxon_metadata_outputs(
    slug: str,
    rows: list[dict[str, Any]],
    *,
    normalize_rows: bool = True,
    dataset_version_id: str | None = None,
) -> tuple[str, str, int]:
    if not rows:
        raise RuntimeError("No metadata rows are available to write.")
    taxon_dir = metadata_taxon_dir(slug, dataset_version_id)
    shutil.rmtree(taxon_dir, ignore_errors=True)
    metadata_output_dir = taxon_dir / "metadata_output"
    metadata_output_dir.mkdir(parents=True, exist_ok=True)
    if normalize_rows:
        rows = [ensure_managed_metadata_schema(row) for row in rows]
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
        derived_country = clean_df["Geographic Location"].apply(extract_country)
        existing_country = clean_df["Country"] if "Country" in clean_df.columns else pd.Series([""] * len(clean_df), index=clean_df.index)
        clean_df["Country"] = existing_country.where(derived_country.isna(), derived_country)
    if "Country" in clean_df.columns:
        country_values = clean_df["Country"].fillna("").astype(str).str.strip()
        valid_country_mask = country_values.map(
            lambda value: (
                value in COUNTRY_MAPPING
                and normalize_standardization_lookup(value) not in NON_COUNTRY_OUTPUT_TERMS
            )
            or value.lower() in {"", "absent", "unknown"}
        )
        clean_df.loc[~valid_country_mask, "Country"] = "unknown"
        clean_df["Continent"] = clean_df["Country"].map(lambda value: COUNTRY_MAPPING.get(str(value), {}).get("Continent", "unknown"))
        clean_df["Subcontinent"] = clean_df["Country"].map(lambda value: COUNTRY_MAPPING.get(str(value), {}).get("Subcontinent", "unknown"))
    clean_df = enforce_clean_sample_type_columns(clean_df)
    clean_path = metadata_output_dir / "ncbi_clean.csv"
    output_columns = comprehensive_columns + ["Country", "Continent", "Subcontinent"]
    save_clean_data(clean_df, [column for column in output_columns if column in clean_df.columns], str(clean_path))
    clean_count = len(clean_df.index)
    return str(updated_path), str(clean_path), clean_count


def load_filtered_taxon_tsv_rows(species: SpeciesRecord) -> list[dict[str, Any]]:
    if not species.tsv_path:
        raise RuntimeError("Managed TSV path is missing for metadata build.")
    df = load_data(species.tsv_path)
    df, _filter_summary = filter_data(df, None, ["all"])
    if "Assembly Accession" not in df.columns:
        raise RuntimeError("Managed TSV is missing Assembly Accession.")
    df = df.drop_duplicates(subset=["Assembly Accession"], keep="first").copy()
    if "Assembly BioSample Accession" not in df.columns:
        df["Assembly BioSample Accession"] = pd.NA
    return df.to_dict(orient="records")


def run_taxon_metadata_pipeline(species: SpeciesRecord) -> tuple[str, str, int | None, int]:
    tsv_rows = load_filtered_taxon_tsv_rows(species)

    stored_rows = load_taxon_metadata_rows(species.id)
    tsv_accessions = {
        str(row.get("Assembly Accession") or "").strip()
        for row in tsv_rows
        if str(row.get("Assembly Accession") or "").strip()
    }
    resumed_row_total = count_taxon_metadata_rows_for_accessions(species.id, tsv_accessions)
    ordered_rows: list[tuple[str, dict[str, Any]]] = []
    new_tsv_rows: list[dict[str, Any]] = []

    for row in tsv_rows:
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
    cached_biosample_records, _stale_or_missing_biosamples = load_cached_biosample_records(biosample_ids)
    biosample_records = dict(cached_biosample_records)
    total_rows = len(ordered_rows)
    completed_rows = max(0, min(resumed_row_total, total_rows))
    update_taxon_metadata_progress(species.id, total=total_rows, completed=completed_rows)
    current_rows: list[dict[str, Any]] = []
    refreshed_at = utc_now()
    newly_persisted_rows = 0
    last_logged_checkpoint = 0

    def log_metadata_checkpoint(force: bool = False) -> None:
        nonlocal last_logged_checkpoint
        if not newly_persisted_rows:
            return
        checkpoint = newly_persisted_rows // 100
        if force or checkpoint > last_logged_checkpoint:
            last_logged_checkpoint = checkpoint
            logging.info(
                "Checkpointed %s new metadata rows for %s (%s/%s rows assembled).",
                newly_persisted_rows,
                species.species_name,
                completed_rows,
                total_rows,
            )

    def persist_new_rows(rows: list[dict[str, Any]]) -> None:
        nonlocal completed_rows, newly_persisted_rows
        if not rows:
            return

        accessions = [metadata_row_accession(row) for row in rows]
        recently_stored_rows = load_taxon_metadata_rows_for_accessions(species.id, accessions)
        rows_to_fetch: list[dict[str, Any]] = []
        stored_hit_count = 0
        for row in rows:
            accession = metadata_row_accession(row)
            stored_row = recently_stored_rows.get(accession)
            if stored_row is not None:
                current_rows.append(merge_tsv_record_with_stored_metadata(row, stored_row))
                stored_hit_count += 1
            else:
                rows_to_fetch.append(row)
        if stored_hit_count:
            completed_rows = max(completed_rows, min(total_rows, completed_rows + stored_hit_count))
        if not rows_to_fetch:
            update_taxon_metadata_progress(species.id, total=total_rows, completed=completed_rows)
            return

        first_accession = metadata_row_accession(rows_to_fetch[0])
        update_taxon_metadata_progress(
            species.id,
            total=total_rows,
            completed=completed_rows,
            current_accession=first_accession or None,
        )

        missing_biosample_ids = [
            biosample_id
            for biosample_id in (metadata_row_biosample_accession(row) for row in rows_to_fetch)
            if biosample_id and biosample_id not in biosample_records
        ]
        fetched_records = fetch_uncached_biosample_records(missing_biosample_ids)
        if fetched_records:
            biosample_records.update(fetched_records)
            save_biosample_cache_records(
                {
                    biosample_id: (record, biosample_record_has_data(record))
                    for biosample_id, record in fetched_records.items()
                }
            )

        enriched_rows: list[dict[str, Any]] = []
        for row in rows_to_fetch:
            biosample_id = metadata_row_biosample_accession(row)
            enriched_row = enrich_tsv_row_with_biosample_metadata(row, biosample_records.get(biosample_id))
            current_rows.append(enriched_row)
            enriched_rows.append(enriched_row)

        saved_count = upsert_taxon_metadata_rows(species.id, enriched_rows, refreshed_at=refreshed_at)
        completed_rows += saved_count
        newly_persisted_rows += saved_count
        update_taxon_metadata_progress(species.id, total=total_rows, completed=completed_rows)
        log_metadata_checkpoint()

    pending_new_rows: list[dict[str, Any]] = []
    for row_type, row in ordered_rows:
        if row_type == "stored":
            persist_new_rows(pending_new_rows)
            pending_new_rows = []
            current_rows.append(row)
            continue
        pending_new_rows.append(row)
        if METADATA_FETCH_WORKERS <= 1 or len(pending_new_rows) >= METADATA_FETCH_BATCH_SIZE:
            persist_new_rows(pending_new_rows)
            pending_new_rows = []
    persist_new_rows(pending_new_rows)
    if newly_persisted_rows and newly_persisted_rows % 100:
        log_metadata_checkpoint(force=True)

    upsert_taxon_metadata_rows(species.id, current_rows, refreshed_at=refreshed_at)
    stored_rows = load_taxon_metadata_rows(species.id)
    final_rows = [
        merge_tsv_record_with_stored_metadata(row, stored_rows[accession])
        for row in tsv_rows
        if (accession := metadata_row_accession(row)) and accession in stored_rows
    ]
    update_taxon_metadata_progress(species.id, total=total_rows, completed=len(final_rows))
    if len(final_rows) < total_rows:
        raise RuntimeError(f"Metadata build incomplete after helper merge: {len(final_rows)}/{total_rows} rows are stored.")
    updated_path, clean_path, clean_count = write_taxon_metadata_outputs(
        species.slug,
        final_rows,
        dataset_version_id=species.staging_dataset_version_id,
    )
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
        with maintain_worker_heartbeat(current.metadata_claimed_by or species.metadata_claimed_by):
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
            staging_version_id = active_pipeline_dataset_version_for_step("discovery", db)
            existing_rows = db.execute("SELECT species_name, slug FROM species").fetchall()
            existing_names = {normalize_species_name(str(row["species_name"] or "")).lower() for row in existing_rows}
            used_slugs = {str(row["slug"] or "") for row in existing_rows}
            now = utc_now()
            discovered_rows: list[tuple[Any, ...]] = []
            discovered_names: set[str] = set()
            for taxon_name, taxon_id in candidates:
                normalized_name = normalize_species_name(taxon_name)
                name_key = normalized_name.lower()
                if not normalized_name or name_key in existing_names or name_key in discovered_names:
                    continue
                base_slug = species_slug(normalized_name)
                slug = base_slug
                suffix = 2
                while slug in used_slugs:
                    slug = f"{base_slug}-{suffix}"
                    suffix += 1
                used_slugs.add(slug)
                discovered_names.add(name_key)
                discovered_rows.append(
                    (
                        normalized_name,
                        slug,
                        latest.target_rank,
                        normalize_assembly_source(latest.assembly_source),
                        "missing",
                        now,
                        now,
                        str(taxon_id) if taxon_id else normalized_name,
                        taxon_id,
                        0,
                        "missing",
                        0,
                        0,
                        0,
                        0 if staging_version_id else 1,
                        staging_version_id,
                    )
                )
            if discovered_rows:
                db.executemany(
                    """
                    INSERT OR IGNORE INTO species (
                        species_name, slug, taxon_rank, assembly_source, status,
                        created_at, updated_at, query_name, taxon_id, refresh_requested,
                        metadata_status, metadata_refresh_requested, metadata_claim_token,
                        metadata_attempt_count, is_live, staging_dataset_version_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    discovered_rows,
                )
                discovered = len(discovered_rows)
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


def job_claim_is_live(job: JobRecord) -> bool:
    if not job.claimed_by:
        return False
    return worker_heartbeat_is_live(job.claimed_by)


def mark_job_cancelled_after_stale_stop(
    job: JobRecord,
    *,
    reason: str = "Cancelled by user/admin; no active worker claim remains.",
    db: sqlite3.Connection | None = None,
) -> bool:
    now = utc_now()
    connection = db or get_db()
    cursor = connection.execute(
        """
        UPDATE jobs
        SET status = 'cancelled',
            pid = NULL,
            return_code = 1,
            error = ?,
            updated_at = ?,
            claimed_by = NULL,
            claimed_at = NULL
        WHERE id = ?
          AND status = 'running'
          AND cancel_requested = 1
        """,
        (reason, now, job.id),
    )
    if not cursor.rowcount:
        return False
    append_job_log(job, f"[{now}] {reason}\n")
    return True


def reconcile_cancelled_running_jobs(worker_name: str | None = None, db: sqlite3.Connection | None = None) -> int:
    """Mark cancelled running jobs as stopped when no live worker can still be executing them."""
    connection = db or get_db()
    rows = connection.execute(
        """
        SELECT *
        FROM jobs
        WHERE status = 'running'
          AND cancel_requested = 1
        """
    ).fetchall()
    reconciled = 0
    for row in rows:
        job = row_to_job(row)
        claimed_by = normalize_metadata_value(job.claimed_by)
        own_returned_claim = bool(worker_name and claimed_by == worker_name)
        dead_or_missing_claim = not claimed_by or not worker_heartbeat_is_live(claimed_by)
        if not own_returned_claim and not dead_or_missing_claim:
            continue
        if own_returned_claim:
            reason = (
                "Cancelled by user/admin; the claiming worker returned to the queue without "
                "finalizing the job, so the stale running state was reconciled."
            )
        else:
            reason = "Cancelled by user/admin; no live worker claim remains during reconciliation."
        if mark_job_cancelled_after_stale_stop(job, reason=reason, db=connection):
            reconciled += 1
    if reconciled:
        connection.commit()
    return reconciled


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


def build_quality_thresholds(source: Any) -> dict[str, Any]:
    def float_value(key: str, default: float | None) -> float | None:
        raw = source.get(key) if hasattr(source, "get") else None
        parsed = parse_optional_float(raw)
        return default if parsed is None else parsed

    def int_value(key: str, default: int | None) -> int | None:
        raw = source.get(key) if hasattr(source, "get") else None
        parsed = parse_optional_int(raw)
        return default if parsed is None else parsed

    return {
        "min_completeness": float_value("qc_min_completeness", SEQUENCE_QC_DEFAULTS["min_completeness"]),
        "max_contamination": float_value("qc_max_contamination", SEQUENCE_QC_DEFAULTS["max_contamination"]),
        "max_n_percent": float_value("qc_max_n_percent", SEQUENCE_QC_DEFAULTS["max_n_percent"]),
        "max_contigs": int_value("qc_max_contigs", SEQUENCE_QC_DEFAULTS["max_contigs"]),
        "min_n50": int_value("qc_min_n50", SEQUENCE_QC_DEFAULTS["min_n50"]),
        "min_total_bp": int_value("qc_min_total_bp", SEQUENCE_QC_DEFAULTS["min_total_bp"]),
        "max_total_bp": int_value("qc_max_total_bp", SEQUENCE_QC_DEFAULTS["max_total_bp"]),
        "min_gc_percent": float_value("qc_min_gc_percent", SEQUENCE_QC_DEFAULTS["min_gc_percent"]),
        "max_gc_percent": float_value("qc_max_gc_percent", SEQUENCE_QC_DEFAULTS["max_gc_percent"]),
    }


def build_quality_command(input_path: Path, output_dir: Path, thresholds: dict[str, Any], form: Any) -> tuple[list[str], dict[str, Any]]:
    command = ["integrated-sequence-quality-check", str(input_path), str(output_dir)]
    filters: dict[str, Any] = {}

    retries = (form.get("retries") or "3").strip() if hasattr(form, "get") else "3"
    retry_delay = (form.get("retry_delay") or "5").strip() if hasattr(form, "get") else "5"
    command.extend(["--retries", retries, "--retry-delay", retry_delay])
    filters["retries"] = retries
    filters["retry_delay"] = retry_delay

    for key, value in thresholds.items():
        if value is None:
            continue
        option = key.replace("_", "-")
        command.extend([f"--{option}", str(value)])
    filters["quality_thresholds"] = thresholds
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
    if job.mode == "qc":
        launch_sequence_quality_job(job)
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
    latest.claimed_by = None
    latest.claimed_at = None
    latest.return_code = return_code
    latest.updated_at = utc_now()
    if latest.cancel_requested and cancellation_honored:
        latest.status = "cancelled"
    else:
        latest.status = "completed" if return_code == 0 else "failed"
    if latest.status in {"completed", "cancelled"}:
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
    cancellation_honored = False

    try:
        with contextlib.redirect_stdout(log_handle), contextlib.redirect_stderr(log_handle):
            run_sequence_downloads(
                namespace,
                input_path=job.input_path,
                output_folder=job.output_dir,
                cancellation_requested=lambda: is_cancel_requested(job.id),
            )
    except SequenceDownloadCancelled:
        return_code = 1
        cancellation_honored = True
        append_job_log(
            job,
            f"[{utc_now()}] Cancellation honored. Stopped submitting new downloads, saved partial outputs, "
            "and wrote the current failed/missing accession report.\n",
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

    finalize_integrated_sequence_job(job, return_code=return_code, cancellation_honored=cancellation_honored)


def fasta_quality_stats(path: Path) -> dict[str, Any]:
    contig_lengths: list[int] = []
    total_bp = 0
    gc_count = 0
    n_count = 0
    current_length = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(">"):
                if current_length:
                    contig_lengths.append(current_length)
                    current_length = 0
                continue
            sequence = line.upper()
            length = len(sequence)
            total_bp += length
            current_length += length
            gc_count += sequence.count("G") + sequence.count("C")
            n_count += sequence.count("N")
    if current_length:
        contig_lengths.append(current_length)

    sorted_lengths = sorted(contig_lengths, reverse=True)
    half_total = total_bp / 2
    running = 0
    n50 = 0
    for length in sorted_lengths:
        running += length
        if running >= half_total:
            n50 = length
            break

    return {
        "sequence_file": path.name,
        "total_bp": total_bp,
        "contig_count": len(contig_lengths),
        "n50": n50,
        "gc_percent": round((gc_count / total_bp) * 100, 3) if total_bp else "",
        "ambiguous_n_percent": round((n_count / total_bp) * 100, 4) if total_bp else "",
    }


def quality_threshold_value(thresholds: dict[str, Any], key: str) -> float | int | None:
    value = thresholds.get(key)
    if value == "":
        return None
    return value


def apply_pass_fail_decision_mode(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "Sequence_QC_Status" not in frame.columns:
        return frame
    result = frame.copy()
    review_mask = result["Sequence_QC_Status"].astype(str).str.lower() == "review"
    if not review_mask.any():
        return result
    if "Sequence_QC_Failure_Reasons" not in result.columns:
        result["Sequence_QC_Failure_Reasons"] = ""
    if "Sequence_QC_Review_Reasons" not in result.columns:
        result["Sequence_QC_Review_Reasons"] = ""
    result.loc[review_mask, "Sequence_QC_Failure_Reasons"] = result.loc[review_mask].apply(
        lambda row: dedupe_reason_text(row.get("Sequence_QC_Failure_Reasons"), row.get("Sequence_QC_Review_Reasons")),
        axis=1,
    )
    result.loc[review_mask, "Sequence_QC_Review_Reasons"] = ""
    result.loc[review_mask, "Sequence_QC_Status"] = "fail"
    if "Sequence_QC_Pass" in result.columns:
        result.loc[review_mask, "Sequence_QC_Pass"] = False
    return result


def split_qc_decision_frames(enriched_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if enriched_frame.empty or "Sequence_QC_Status" not in enriched_frame.columns:
        return enriched_frame, enriched_frame, enriched_frame
    statuses = enriched_frame["Sequence_QC_Status"].astype(str).str.lower()
    pass_frame = enriched_frame[statuses == "pass"].copy()
    review_frame = enriched_frame[statuses == "review"].copy()
    fail_frame = enriched_frame[statuses == "fail"].copy()
    return pass_frame, review_frame, fail_frame


def find_fasta_for_accession(output_dir: Path, accession: str) -> Path | None:
    if not accession:
        return None
    matches = sorted(path for path in output_dir.glob(f"{accession}_*_genomic.fna") if path.is_file())
    return matches[0] if matches else None


def should_expose_output_file(relative_path: Path) -> bool:
    parts = relative_path.parts
    if any(part.startswith(".") for part in parts):
        return False
    hidden_prefixes = {
        ("external_tools", "quality_check", "nextflow_work"),
        ("external_tools", "quality_check", "local_samples"),
        ("external_tools", "quality_check", "pipeline_dag.dot"),
    }
    for prefix in hidden_prefixes:
        if parts[: len(prefix)] == prefix:
            return False
    return True


def first_nonempty_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = normalize_metadata_value(row.get(key))
        if value:
            return value
    return ""


def normalize_qc_decision_status(status: Any, fail_reasons: str = "", warning_reasons: str = "") -> str:
    normalized = normalize_metadata_value(status).lower()
    if normalized in {"pass", "passed", "ok", "success", "true"}:
        return "pass"
    if normalized in {"fail", "failed", "error", "false"}:
        return "fail"
    if normalized in {"warn", "warning", "review", "needs_review", "manual_review"}:
        return "review"
    if normalize_metadata_value(fail_reasons):
        return "fail"
    if normalize_metadata_value(warning_reasons):
        return "review"
    return "pass"


def dedupe_reason_text(*values: Any) -> str:
    reasons: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_metadata_value(value)
        if not text:
            continue
        for part in text.split(";"):
            reason = part.strip()
            if not reason:
                continue
            key = re.sub(r"\s+", " ", reason).lower()
            if key in seen:
                continue
            seen.add(key)
            reasons.append(reason)
    return "; ".join(reasons)


def remove_invalid_ani_warning_reasons(value: Any) -> str:
    text = normalize_metadata_value(value)
    if not text:
        return ""
    kept: list[str] = []
    for part in text.split(";"):
        reason = part.strip()
        if not reason:
            continue
        if re.fullmatch(r"ani_species_warning:0(?:\.0+)?<\d+(?:\.\d+)?", reason, re.IGNORECASE):
            continue
        kept.append(reason)
    return "; ".join(kept)


def latest_nextflow_qc_master_report(output_dir: Path) -> Path | None:
    root = output_dir / "nextflow_qc"
    if not root.exists():
        return None
    candidates = [path for path in root.rglob("qc_master_report.csv") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_nextflow_qc_report(output_dir: Path, *relative_parts: str) -> Path | None:
    root = output_dir / "nextflow_qc"
    if not root.exists() or not relative_parts:
        return None
    suffix = tuple(relative_parts)
    candidates = [path for path in root.rglob(relative_parts[-1]) if path.is_file() and path.parts[-len(suffix) :] == suffix]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def qc_sequence_lookup_keys(*values: Any) -> set[str]:
    keys: set[str] = set()
    for value in values:
        text = normalize_metadata_value(value)
        if not text:
            continue
        name = Path(text).name
        variants = {text, name}
        lowered = name.lower()
        for suffix in [".fna.gz", ".fa.gz", ".fasta.gz", ".fna", ".fa", ".fasta"]:
            if lowered.endswith(suffix):
                variants.add(name[: -len(suffix)])
                break
        for variant in variants:
            if variant:
                keys.add(variant)
    return keys


def import_nextflow_qc_outputs(input_path: Path, output_dir: Path, qc_dir: Path) -> dict[str, Any] | None:
    master_report = latest_nextflow_qc_master_report(output_dir)
    if master_report is None:
        return None

    source_frame = pd.read_csv(input_path, dtype=str).fillna("") if input_path.exists() else pd.DataFrame()
    external_frame = pd.read_csv(master_report, dtype=str).fillna("")
    if external_frame.empty:
        return None

    optional_report_sources = {
        "ani_summary": latest_nextflow_qc_report(output_dir, "ani", "analysis", "panr2_ani_summary.csv"),
        "ani_run_status": latest_nextflow_qc_report(output_dir, "ani", "analysis", "ani_run_status.tsv"),
        "ani_closest_genome": latest_nextflow_qc_report(output_dir, "ani", "analysis", "closest_genome.csv"),
        "mash_closest_neighbors": latest_nextflow_qc_report(output_dir, "mash", "analysis", "closest_mash_neighbor.csv"),
        "mash_distance_long": latest_nextflow_qc_report(output_dir, "mash", "analysis", "mash_distance_long.csv"),
        "gtdbtk_bac120_summary": latest_nextflow_qc_report(output_dir, "gtdbtk", "gtdbtk.bac120.summary.tsv"),
        "gtdbtk_ar53_summary": latest_nextflow_qc_report(output_dir, "gtdbtk", "gtdbtk.ar53.summary.tsv"),
        "gtdbtk_empty_summary": latest_nextflow_qc_report(output_dir, "gtdbtk", "gtdbtk.empty.summary.tsv"),
    }
    optional_report_targets = {
        "ani_summary": "external_ani_summary.csv",
        "ani_run_status": "external_ani_run_status.tsv",
        "ani_closest_genome": "external_ani_closest_genome.csv",
        "mash_closest_neighbors": "external_mash_closest_neighbors.csv",
        "mash_distance_long": "external_mash_distance_long.csv",
        "gtdbtk_bac120_summary": "external_gtdbtk_bac120_summary.tsv",
        "gtdbtk_ar53_summary": "external_gtdbtk_ar53_summary.tsv",
        "gtdbtk_empty_summary": "external_gtdbtk_empty_summary.tsv",
    }
    copied_optional_reports: dict[str, str] = {}
    for key, source_path in optional_report_sources.items():
        if source_path is None:
            continue
        target_path = qc_dir / optional_report_targets[key]
        shutil.copy2(source_path, target_path)
        copied_optional_reports[key] = str(target_path.relative_to(output_dir))

    mash_lookup: dict[str, dict[str, Any]] = {}
    mash_report = optional_report_sources.get("mash_closest_neighbors")
    if mash_report is not None:
        try:
            mash_frame = pd.read_csv(mash_report, dtype=str).fillna("")
            for _, mash_row in mash_frame.iterrows():
                row_dict = {str(key): value for key, value in mash_row.to_dict().items()}
                for lookup_key in qc_sequence_lookup_keys(row_dict.get("query")):
                    mash_lookup[lookup_key] = row_dict
        except (OSError, pd.errors.EmptyDataError, ValueError):
            mash_lookup = {}

    source_lookup: dict[str, dict[str, Any]] = {}
    for _, row in source_frame.iterrows():
        row_dict = {str(key): value for key, value in row.to_dict().items()}
        accession = normalize_metadata_value(row_dict.get("Assembly Accession"))
        sequence_file = normalize_metadata_value(row_dict.get("sequence_file"))
        if accession:
            source_lookup[f"accession:{accession}"] = row_dict
        if sequence_file:
            source_lookup[f"file:{sequence_file}"] = row_dict

    decision_rows: list[dict[str, Any]] = []
    enriched_rows: list[dict[str, Any]] = []
    stat_rows: list[dict[str, Any]] = []

    for _, row in external_frame.iterrows():
        external_row = {str(key): value for key, value in row.to_dict().items()}
        accession = first_nonempty_value(external_row, "Assembly Accession", "assembly_accession")
        sequence_file = first_nonempty_value(external_row, "sequence_file", "quast_assembly", "checkm2_name")
        source_row = source_lookup.get(f"accession:{accession}") or source_lookup.get(f"file:{sequence_file}") or {}
        merged = {**source_row}

        failure_reasons = dedupe_reason_text(
            first_nonempty_value(external_row, "qc_master_fail_reasons"),
            first_nonempty_value(external_row, "combined_qc_fail_reasons"),
            first_nonempty_value(external_row, "sequence_qc_fail_reasons"),
            first_nonempty_value(external_row, "checkm2_qc_fail_reasons"),
            first_nonempty_value(external_row, "quast_qc_fail_reasons"),
            first_nonempty_value(external_row, "ani_qc_fail_reasons"),
            first_nonempty_value(external_row, "mash_qc_fail_reasons"),
            first_nonempty_value(external_row, "gtdbtk_qc_fail_reasons"),
        )
        warning_reasons = dedupe_reason_text(
            first_nonempty_value(external_row, "qc_master_warning_reasons"),
            first_nonempty_value(external_row, "combined_qc_warning_reasons"),
            first_nonempty_value(external_row, "sequence_qc_warning_reasons"),
            first_nonempty_value(external_row, "checkm2_qc_warning_reasons"),
            first_nonempty_value(external_row, "ani_qc_warning_reasons"),
            first_nonempty_value(external_row, "mash_qc_warning_reasons"),
            first_nonempty_value(external_row, "gtdbtk_warnings"),
        )
        status = normalize_qc_decision_status(
            first_nonempty_value(external_row, "qc_master_status", "combined_qc_status", "sequence_qc_status"),
            failure_reasons,
            warning_reasons,
        )
        raw_ani_closest_genome = first_nonempty_value(external_row, "ani_closest_genome")
        raw_ani_closest_ani = first_nonempty_value(external_row, "ani_closest_ani")
        ani_closest_genome = nonnumeric_label(raw_ani_closest_genome)
        ani_closest_ani = positive_percent_text(raw_ani_closest_ani)
        ani_status = first_nonempty_value(external_row, "ani_species_consistency_status")
        if normalize_metadata_value(ani_status).upper() in {"WARN", "WARNING", "REVIEW"} and raw_ani_closest_ani and not ani_closest_ani:
            warning_reasons = dedupe_reason_text(remove_invalid_ani_warning_reasons(warning_reasons), "NO_VALID_ANI_RESULT")
            status = normalize_qc_decision_status(status, failure_reasons, warning_reasons)

        total_bp = first_nonempty_value(external_row, "sequence_total_length", "quast_total_length", "checkm2_genome_size")
        ambiguous_bases = parse_optional_float(first_nonempty_value(external_row, "sequence_ambiguous_bases"))
        total_bp_number = parse_optional_float(total_bp)
        ambiguous_n_percent = ""
        if ambiguous_bases is not None and total_bp_number:
            ambiguous_n_percent = round((ambiguous_bases / total_bp_number) * 100, 4)

        mash_row: dict[str, Any] = {}
        for lookup_key in qc_sequence_lookup_keys(accession, sequence_file, external_row.get("sequence_accession")):
            if lookup_key in mash_lookup:
                mash_row = mash_lookup[lookup_key]
                break

        decision = {
            "Assembly Accession": accession,
            "Assembly Name": first_nonempty_value(external_row, "Assembly Name", "assembly_name") or normalize_metadata_value(merged.get("Assembly Name")),
            "sequence_file": sequence_file,
            "Sequence_QC_Status": status,
            "Sequence_QC_Pass": status == "pass",
            "Sequence_QC_Failure_Reasons": failure_reasons,
            "Sequence_QC_Review_Reasons": warning_reasons,
            "CheckM completeness": first_nonempty_value(external_row, "checkm2_completeness", "CheckM completeness"),
            "CheckM contamination": first_nonempty_value(external_row, "checkm2_contamination", "CheckM contamination"),
            "QC_total_bp": total_bp,
            "QC_contig_count": first_nonempty_value(external_row, "sequence_num_contigs", "quast_num_contigs"),
            "QC_n50": first_nonempty_value(external_row, "sequence_n50", "quast_n50"),
            "QC_gc_percent": first_nonempty_value(external_row, "sequence_gc_percent", "quast_gc_percent"),
            "QC_ambiguous_n_percent": ambiguous_n_percent,
            "External_QC_Source": "nextflow",
            "External_QC_Master_Report": str(master_report.relative_to(output_dir)),
            "CheckM2_Model": first_nonempty_value(external_row, "checkm2_model"),
            "CheckM2_Coding_Density": first_nonempty_value(external_row, "checkm2_coding_density"),
            "QUAST_Largest_Contig": first_nonempty_value(external_row, "quast_largest_contig"),
            "QUAST_Ns_Per_100kbp": first_nonempty_value(external_row, "quast_ns_per_100kbp"),
            "ANI_Closest_Genome": ani_closest_genome,
            "ANI_Closest_ANI": ani_closest_ani,
            "ANI_Species_Consistency_Status": ani_status,
            "ANI_Cluster": first_nonempty_value(external_row, "ani_cluster"),
            "ANI_Cluster_Representative": first_nonempty_value(external_row, "ani_cluster_representative"),
            "ANI_Cluster_Size": first_nonempty_value(external_row, "ani_cluster_size"),
            "Mash_Closest_Genome": first_nonempty_value(mash_row, "reference"),
            "Mash_Distance": first_nonempty_value(mash_row, "mash_distance"),
            "Mash_P_Value": first_nonempty_value(mash_row, "p_value"),
            "Mash_Matching_Hashes": first_nonempty_value(mash_row, "matching_hashes"),
            "GTDBTK_QC_Status": first_nonempty_value(external_row, "gtdbtk_qc_status"),
            "GTDBTK_QC_Fail_Reasons": first_nonempty_value(external_row, "gtdbtk_qc_fail_reasons"),
            "GTDBTK_Match_Rank": first_nonempty_value(external_row, "gtdbtk_match_rank"),
            "GTDBTK_Classification": first_nonempty_value(external_row, "gtdbtk_classification"),
            "GTDBTK_Genus": first_nonempty_value(external_row, "gtdbtk_genus"),
            "GTDBTK_Species": first_nonempty_value(external_row, "gtdbtk_species"),
            "GTDBTK_FastANI": first_nonempty_value(external_row, "gtdbtk_fastani_ani"),
        }
        decision_rows.append(decision)
        enriched_rows.append({**merged, **external_row, **decision})
        stat_rows.append(
            {
                "Assembly Accession": accession,
                "Assembly Name": decision["Assembly Name"],
                "sequence_file": sequence_file,
                "total_bp": decision["QC_total_bp"],
                "contig_count": decision["QC_contig_count"],
                "n50": decision["QC_n50"],
                "gc_percent": decision["QC_gc_percent"],
                "ambiguous_n_percent": decision["QC_ambiguous_n_percent"],
            }
        )

    decision_frame = pd.DataFrame(decision_rows)
    enriched_frame = pd.DataFrame(enriched_rows)
    stats_frame = pd.DataFrame(stat_rows)
    pass_frame = enriched_frame[enriched_frame["Sequence_QC_Status"] == "pass"].copy()
    review_frame = enriched_frame[enriched_frame["Sequence_QC_Status"] == "review"].copy()
    fail_frame = enriched_frame[enriched_frame["Sequence_QC_Status"] == "fail"].copy()

    shutil.copy2(master_report, qc_dir / "external_qc_master_report.csv")
    decision_frame.to_csv(qc_dir / "qc_decisions.csv", index=False)
    enriched_frame.to_csv(qc_dir / "qc_enriched_metadata.csv", index=False)
    enriched_frame.to_csv(qc_dir / "qc_all_metadata.csv", index=False)
    pass_frame.to_csv(qc_dir / "qc_pass_metadata.csv", index=False)
    review_frame.to_csv(qc_dir / "qc_review_metadata.csv", index=False)
    fail_frame.to_csv(qc_dir / "qc_failed_metadata.csv", index=False)
    stats_frame.to_csv(qc_dir / "assembly_stats.csv", index=False)

    return {
        "source": "nextflow",
        "master_report": str(master_report.relative_to(output_dir)),
        "total": int(len(enriched_frame)),
        "pass": int(len(pass_frame)),
        "review": int(len(review_frame)),
        "fail": int(len(fail_frame)),
        "decision_frame": decision_frame,
        "enriched_frame": enriched_frame,
        "stats_frame": stats_frame,
        "pass_frame": pass_frame,
        "review_frame": review_frame,
        "fail_frame": fail_frame,
        "optional_reports": copied_optional_reports,
    }


def run_sequence_quality_checks(
    job: JobRecord,
    thresholds: dict[str, Any],
    quality_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    input_path = Path(job.input_path)
    output_dir = Path(job.output_dir)
    qc_dir = output_dir / "sequence_qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    quality_config = quality_config or {
        "profile": QUALITY_PROFILES["quick"],
        "run_mode": "quick",
        "selected_modules": ["quick_fasta"],
        "thresholds": thresholds,
        "external_modules": [],
    }
    handoff_manifest = build_quality_handoff(job.id, input_path, output_dir, quality_config)

    frame = pd.read_csv(input_path).fillna("")
    decision_rows: list[dict[str, Any]] = []
    stat_rows: list[dict[str, Any]] = []
    enriched_rows: list[dict[str, Any]] = []

    for _, row in frame.iterrows():
        row_dict = {str(key): value for key, value in row.to_dict().items()}
        accession = normalize_metadata_value(row_dict.get("Assembly Accession"))
        assembly_name = normalize_metadata_value(row_dict.get("Assembly Name"))
        fasta_path = find_fasta_for_accession(output_dir, accession)
        failures: list[str] = []
        reviews: list[str] = []
        stats: dict[str, Any] = {
            "sequence_file": "",
            "total_bp": "",
            "contig_count": "",
            "n50": "",
            "gc_percent": "",
            "ambiguous_n_percent": "",
        }

        if fasta_path is None:
            failures.append("FASTA file missing")
        else:
            try:
                stats = fasta_quality_stats(fasta_path)
                if not stats["total_bp"]:
                    failures.append("FASTA file contains no sequence bases")
            except Exception as exc:
                failures.append(f"FASTA parse failed: {type(exc).__name__}")

        completeness = parse_optional_float(row_dict.get("CheckM completeness"))
        min_completeness = quality_threshold_value(thresholds, "min_completeness")
        if min_completeness is not None:
            if completeness is None:
                reviews.append("CheckM completeness missing")
            elif completeness < float(min_completeness):
                failures.append(f"CheckM completeness {completeness:g} < {float(min_completeness):g}")

        contamination = parse_optional_float(row_dict.get("CheckM contamination"))
        max_contamination = quality_threshold_value(thresholds, "max_contamination")
        if max_contamination is not None:
            if contamination is None:
                reviews.append("CheckM contamination missing")
            elif contamination > float(max_contamination):
                failures.append(f"CheckM contamination {contamination:g} > {float(max_contamination):g}")

        max_n_percent = quality_threshold_value(thresholds, "max_n_percent")
        ambiguous_n_percent = parse_optional_float(stats.get("ambiguous_n_percent"))
        if max_n_percent is not None and ambiguous_n_percent is not None and ambiguous_n_percent > float(max_n_percent):
            failures.append(f"Ambiguous N percent {ambiguous_n_percent:g} > {float(max_n_percent):g}")

        total_bp = parse_optional_int(stats.get("total_bp"))
        min_total_bp = quality_threshold_value(thresholds, "min_total_bp")
        if min_total_bp is not None:
            if total_bp is None:
                reviews.append("Genome size missing")
            elif total_bp < int(min_total_bp):
                failures.append(f"Genome size {total_bp} < {int(min_total_bp)}")

        max_total_bp = quality_threshold_value(thresholds, "max_total_bp")
        if max_total_bp is not None:
            if total_bp is None:
                reviews.append("Genome size missing")
            elif total_bp > int(max_total_bp):
                failures.append(f"Genome size {total_bp} > {int(max_total_bp)}")

        gc_percent = parse_optional_float(stats.get("gc_percent"))
        min_gc_percent = quality_threshold_value(thresholds, "min_gc_percent")
        if min_gc_percent is not None:
            if gc_percent is None:
                reviews.append("GC percent missing")
            elif gc_percent < float(min_gc_percent):
                failures.append(f"GC percent {gc_percent:g} < {float(min_gc_percent):g}")

        max_gc_percent = quality_threshold_value(thresholds, "max_gc_percent")
        if max_gc_percent is not None:
            if gc_percent is None:
                reviews.append("GC percent missing")
            elif gc_percent > float(max_gc_percent):
                failures.append(f"GC percent {gc_percent:g} > {float(max_gc_percent):g}")

        max_contigs = quality_threshold_value(thresholds, "max_contigs")
        contig_count = parse_optional_int(stats.get("contig_count")) or parse_optional_int(row_dict.get("Assembly Stats Number of Contigs"))
        if max_contigs is not None:
            if contig_count is None:
                reviews.append("Contig count missing")
            elif contig_count > int(max_contigs):
                failures.append(f"Contig count {contig_count} > {int(max_contigs)}")

        min_n50 = quality_threshold_value(thresholds, "min_n50")
        n50 = parse_optional_int(stats.get("n50"))
        if min_n50 is not None:
            if n50 is None:
                reviews.append("N50 missing")
            elif n50 < int(min_n50):
                failures.append(f"N50 {n50} < {int(min_n50)}")

        status = "fail" if failures else "review" if reviews else "pass"
        decision = {
            "Assembly Accession": accession,
            "Assembly Name": assembly_name,
            "sequence_file": stats.get("sequence_file", ""),
            "Sequence_QC_Status": status,
            "Sequence_QC_Pass": status == "pass",
            "Sequence_QC_Failure_Reasons": "; ".join(failures),
            "Sequence_QC_Review_Reasons": "; ".join(reviews),
            "CheckM completeness": completeness if completeness is not None else "",
            "CheckM contamination": contamination if contamination is not None else "",
            "QC_total_bp": stats.get("total_bp", ""),
            "QC_contig_count": stats.get("contig_count", ""),
            "QC_n50": stats.get("n50", ""),
            "QC_gc_percent": stats.get("gc_percent", ""),
            "QC_ambiguous_n_percent": stats.get("ambiguous_n_percent", ""),
        }
        decision_rows.append(decision)
        stat_rows.append(
            {
                "Assembly Accession": accession,
                "Assembly Name": assembly_name,
                **stats,
            }
        )
        enriched_rows.append({**row_dict, **decision})

    decision_frame = pd.DataFrame(decision_rows)
    stats_frame = pd.DataFrame(stat_rows)
    enriched_frame = pd.DataFrame(enriched_rows)
    if quality_config.get("decision_mode") == "pass_fail":
        decision_frame = apply_pass_fail_decision_mode(decision_frame)
        enriched_frame = apply_pass_fail_decision_mode(enriched_frame)
    pass_frame, review_frame, fail_frame = split_qc_decision_frames(enriched_frame)

    stats_frame.to_csv(qc_dir / "assembly_stats.csv", index=False)
    decision_frame.to_csv(qc_dir / "qc_decisions.csv", index=False)
    enriched_frame.to_csv(qc_dir / "qc_enriched_metadata.csv", index=False)
    enriched_frame.to_csv(qc_dir / "qc_all_metadata.csv", index=False)
    pass_frame.to_csv(qc_dir / "qc_pass_metadata.csv", index=False)
    review_frame.to_csv(qc_dir / "qc_review_metadata.csv", index=False)
    fail_frame.to_csv(qc_dir / "qc_failed_metadata.csv", index=False)

    nextflow_return_code: int | None = None
    nextflow_log_path = ""
    external_qc_import: dict[str, Any] | None = None
    if quality_config.get("run_mode") == "nextflow" and quality_config.get("external_modules"):
        tool_status = handoff_manifest.get("tool_status") or {}
        if not tool_status.get("nextflow_enabled") or not tool_status.get("tools", {}).get("nextflow"):
            raise RuntimeError(
                "Nextflow quality execution was requested, but Nextflow execution is not configured. "
                "The external-tool handoff manifest was written under outputs/external_tools/quality_check."
            )
        handoff_dir = Path(handoff_manifest["handoff_dir"])
        nextflow_log = handoff_dir / "nextflow_execution.log"
        nextflow_log_path = str(nextflow_log.relative_to(output_dir))
        nextflow_env = os.environ.copy()
        nextflow_env.setdefault("NXF_SYNTAX_PARSER", "v1")
        workflow_path = normalize_metadata_value(tool_status.get("nextflow_workflow"))
        nextflow_cwd = Path(workflow_path) if workflow_path.startswith("/") and Path(workflow_path).exists() else handoff_dir
        append_job_log(job, f"[{utc_now()}] Starting external Nextflow QC: {shlex.join(handoff_manifest['nextflow_command'])}\n")
        with nextflow_log.open("w", encoding="utf-8") as nextflow_handle:
            result = subprocess.run(
                handoff_manifest["nextflow_command"],
                cwd=nextflow_cwd,
                env=nextflow_env,
                stdout=nextflow_handle,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        nextflow_return_code = int(result.returncode)
        append_job_log(job, f"[{utc_now()}] External Nextflow QC finished with return code {nextflow_return_code}.\n")
        if nextflow_return_code != 0:
            diagnostic = ""
            try:
                nextflow_tail = nextflow_log.read_text(encoding="utf-8", errors="ignore").splitlines()[-80:]
            except OSError:
                nextflow_tail = []
            for line in reversed(nextflow_tail):
                if "Process requirement exceeds available memory" in line:
                    diagnostic = line.strip()
                    break
            if diagnostic:
                raise RuntimeError(
                    f"External Nextflow QC failed with return code {nextflow_return_code}: {diagnostic}. "
                    f"See {nextflow_log_path}."
                )
            raise RuntimeError(f"External Nextflow QC failed with return code {nextflow_return_code}. See {nextflow_log_path}.")
        external_qc_import = import_nextflow_qc_outputs(input_path, output_dir, qc_dir)
        if external_qc_import:
            decision_frame = external_qc_import["decision_frame"]
            enriched_frame = external_qc_import["enriched_frame"]
            stats_frame = external_qc_import["stats_frame"]
            if quality_config.get("decision_mode") == "pass_fail":
                decision_frame = apply_pass_fail_decision_mode(decision_frame)
                enriched_frame = apply_pass_fail_decision_mode(enriched_frame)
            pass_frame, review_frame, fail_frame = split_qc_decision_frames(enriched_frame)
            decision_frame.to_csv(qc_dir / "qc_decisions.csv", index=False)
            enriched_frame.to_csv(qc_dir / "qc_enriched_metadata.csv", index=False)
            enriched_frame.to_csv(qc_dir / "qc_all_metadata.csv", index=False)
            pass_frame.to_csv(qc_dir / "qc_pass_metadata.csv", index=False)
            review_frame.to_csv(qc_dir / "qc_review_metadata.csv", index=False)
            fail_frame.to_csv(qc_dir / "qc_failed_metadata.csv", index=False)
            append_job_log(
                job,
                f"[{utc_now()}] Imported external QC master report "
                f"({len(pass_frame)} pass, {len(review_frame)} review, "
                f"{len(fail_frame)} fail).\n",
            )
        else:
            append_job_log(job, f"[{utc_now()}] External QC completed, but no qc_master_report.csv was found to import.\n")

    summary = {
        "total": int(len(enriched_frame)),
        "pass": int(len(pass_frame)),
        "review": int(len(review_frame)),
        "fail": int(len(fail_frame)),
        "thresholds": thresholds,
        "quality_profile": (quality_config.get("profile") or {}).get("label", "Quick QC"),
        "decision_mode": quality_config.get("decision_mode", "pass_fail"),
        "run_mode": quality_config.get("run_mode", "quick"),
        "selected_modules": quality_config.get("selected_modules", ["quick_fasta"]),
        "external_modules": quality_config.get("external_modules", []),
        "external_handoff": "external_tools/quality_check/quality_check_manifest.json",
        "external_execution_enabled": handoff_manifest.get("nextflow_execution_enabled", False),
        "nextflow_return_code": nextflow_return_code,
        "nextflow_log": nextflow_log_path,
        "qc_decision_source": "nextflow" if external_qc_import else "built_in",
        "external_qc_imported": bool(external_qc_import),
        "external_qc_master_report": external_qc_import["master_report"] if external_qc_import else "",
        "external_optional_reports": external_qc_import.get("optional_reports", {}) if external_qc_import else {},
    }
    report_lines = [
        "# FetchM Web Sequence Quality Check",
        "",
        f"Job ID: {job.id}",
        f"Input: {job.input_name}",
        f"Quality profile: {summary['quality_profile']}",
        f"Run mode: {summary['run_mode']}",
        f"QC filtering policy: {quality_config.get('qc_filter_mode', 'review_all')}",
        f"GTDB-Tk taxonomy match rank: {quality_config.get('taxonomy_match_rank', 'genus')}",
        f"QC decision source: {summary['qc_decision_source']}",
        f"Total genomes checked: {summary['total']}",
        f"Passed: {summary['pass']}",
        f"Review: {summary['review']}",
        f"Failed: {summary['fail']}",
        "",
        "## Selected Modules",
        "",
    ]
    for module in handoff_manifest.get("module_manifest", []):
        report_lines.append(
            f"- {module['label']} (`{module['key']}`): "
            f"{'built-in' if not module['requires_external_tool'] else 'external'}; "
            f"tool available: {'yes' if module['tool_available'] else 'no'}"
        )
    report_lines.extend(
        [
            "",
            "## External Tool Handoff",
            "",
            f"- Nextflow execution enabled: {'yes' if summary['external_execution_enabled'] else 'no'}",
            f"- External QC imported: {'yes' if summary['external_qc_imported'] else 'no'}",
            f"- External QC master report: `{summary['external_qc_master_report'] or 'not imported'}`",
            "- Manifest: `external_tools/quality_check/quality_check_manifest.json`",
            "- Command script: `external_tools/quality_check/nextflow_command.sh`",
            f"- Execution log: `{nextflow_log_path or 'not executed'}`",
            f"- Return code: `{nextflow_return_code if nextflow_return_code is not None else 'not executed'}`",
            "",
        ]
    )
    optional_reports = summary.get("external_optional_reports") or {}
    if optional_reports:
        report_lines.extend(["## External Comparative QC Reports", ""])
        for key, relative_path in optional_reports.items():
            report_lines.append(f"- `{key}`: `{relative_path}`")
        report_lines.append("")
    report_lines.extend(
        [
            "## Thresholds",
            "",
        ]
    )
    for key, value in thresholds.items():
        report_lines.append(f"- `{key}`: {value if value is not None else 'not applied'}")
    report_lines.extend(
        [
            "",
            "## Outputs",
            "",
            "- `sequence_qc/qc_decisions.csv`: one-row-per-assembly quality decision table.",
            "- `sequence_qc/qc_enriched_metadata.csv`: original filtered metadata with QC columns appended.",
            "- `sequence_qc/qc_all_metadata.csv`: all checked genomes with QC columns appended.",
            "- `sequence_qc/qc_pass_metadata.csv`: metadata subset passing all applied quality checks.",
            "- `sequence_qc/qc_review_metadata.csv`: rows needing review because a required metric was missing.",
            "- `sequence_qc/qc_failed_metadata.csv`: rows failing at least one applied quality check.",
            "- `sequence_qc/external_qc_master_report.csv`: imported external QC master report when Nextflow execution produced one.",
            "- `external_tools/quality_check/`: Nextflow handoff and execution logs.",
        ]
    )
    (qc_dir / "quality_check_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    (qc_dir / "quality_check_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (qc_dir / "qc_thresholds_used.json").write_text(json.dumps(thresholds, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_quality_check_html_summary(qc_dir, job=job, summary=summary, decisions=decision_frame.fillna(""))

    bundle_path = output_dir / "quality_check_bundle.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for root_dir in [qc_dir, output_dir / "external_tools" / "quality_check"]:
            if not root_dir.exists():
                continue
            for path in sorted(root_dir.rglob("*")):
                if not path.is_file():
                    continue
                relative_path = path.relative_to(output_dir)
                if not should_expose_output_file(relative_path):
                    continue
                archive.write(path, arcname=str(relative_path))

    return summary


def finalize_quality_job(
    job: JobRecord,
    *,
    return_code: int,
    cancellation_honored: bool = False,
    error_message: str | None = None,
) -> None:
    latest = load_job(job.id)
    latest.pid = None
    latest.claimed_by = None
    latest.claimed_at = None
    latest.return_code = return_code
    latest.updated_at = utc_now()
    if error_message:
        latest.error = error_message
    if latest.cancel_requested and cancellation_honored:
        latest.status = "cancelled"
    else:
        latest.status = "completed" if return_code == 0 else "failed"
    save_job(latest)
    if latest.status == "completed":
        notify_job_event(latest, "finished")
    elif latest.status == "failed":
        notify_job_event(latest, "failed")


def launch_sequence_quality_job(job: JobRecord) -> None:
    append_job_log(job, f"[{utc_now()}] Launching sequence quality check job {job.id}\n")
    append_job_log(job, "Quality step 1/5: Input loaded and selected metadata stored.\n")

    if is_cancel_requested(job.id):
        append_job_log(job, f"[{utc_now()}] Cancellation requested before quality check started.\n")
        finalize_quality_job(job, return_code=1, cancellation_honored=True)
        return

    job.pid = None
    job.status = "running"
    job.updated_at = utc_now()
    save_job(job)

    quality_config = (job.filters or {}).get("quality_config") or {
        "profile": QUALITY_PROFILES["quick"],
        "run_mode": "quick",
        "selected_modules": ["quick_fasta"],
        "thresholds": (job.filters or {}).get("quality_thresholds") or dict(SEQUENCE_QC_DEFAULTS),
        "external_modules": [],
    }
    thresholds = quality_config.get("thresholds") or (job.filters or {}).get("quality_thresholds") or dict(SEQUENCE_QC_DEFAULTS)
    append_job_log(
        job,
        f"[{utc_now()}] Quality profile: {(quality_config.get('profile') or {}).get('label', 'Quick QC')}; "
        f"run mode: {quality_config.get('run_mode', 'quick')}; "
        f"filtering: {quality_config.get('qc_filter_mode', 'review_all')}; "
        f"taxonomy match: {quality_config.get('taxonomy_match_rank', 'genus')}; "
        f"modules: {', '.join(quality_config.get('selected_modules') or ['quick_fasta'])}.\n",
    )
    namespace = build_integrated_sequence_namespace(job)
    namespace.check_only = False
    log_handle = open(job.log_path, "a", encoding="utf-8")
    root_logger = logging.getLogger()
    file_handler = logging.StreamHandler(log_handle)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    previous_level = root_logger.level
    root_logger.addHandler(file_handler)
    root_logger.setLevel(min(previous_level, logging.INFO) if previous_level else logging.INFO)
    return_code = 0
    cancellation_honored = False
    error_message: str | None = None

    try:
        with contextlib.redirect_stdout(log_handle), contextlib.redirect_stderr(log_handle):
            run_sequence_downloads(
                namespace,
                input_path=job.input_path,
                output_folder=job.output_dir,
                cancellation_requested=lambda: is_cancel_requested(job.id),
            )
            append_job_log(job, f"[{utc_now()}] Quality step 2/5: Sequence download step finished.\n")
            append_job_log(job, f"[{utc_now()}] Quality step 3/5: Computing FASTA assembly statistics.\n")
            summary = run_sequence_quality_checks(job, thresholds, quality_config)
            append_job_log(job, f"[{utc_now()}] Quality step 4/5: QC decisions written for {summary['total']} genomes.\n")
            append_job_log(
                job,
                f"[{utc_now()}] Quality step 5/5: QC pass subset ready "
                f"({summary['pass']} pass, {summary['review']} review, {summary['fail']} fail).\n",
            )
    except SequenceDownloadCancelled:
        return_code = 1
        cancellation_honored = True
        append_job_log(job, f"[{utc_now()}] Cancellation honored during quality check.\n")
    except Exception as exc:
        return_code = 1
        error_message = str(exc)
        append_job_log(job, f"[{utc_now()}] Sequence quality check failed: {exc}\n")
    finally:
        root_logger.removeHandler(file_handler)
        root_logger.setLevel(previous_level)
        file_handler.flush()
        file_handler.close()
        log_handle.write(f"\n[{utc_now()}] Job finished with return code {return_code}\n")
        log_handle.close()

    finalize_quality_job(job, return_code=return_code, cancellation_honored=cancellation_honored, error_message=error_message)


def request_job_cancellation(job: JobRecord) -> JobRecord:
    latest = load_job(job.id)
    latest.cancel_requested = True
    latest.updated_at = utc_now()
    if latest.status == "queued":
        latest.status = "cancelled"
        latest.pid = None
        latest.claimed_by = None
        latest.claimed_at = None
    save_job(latest)
    if latest.status == "running" and not job_claim_is_live(latest):
        with get_sqlite_connection() as db:
            mark_job_cancelled_after_stale_stop(
                latest,
                reason="Cancelled by user/admin; no live worker claim was found at cancellation time.",
                db=db,
            )
            db.commit()
        return load_job(job.id)
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
            with get_sqlite_connection() as db:
                reconcile_cancelled_running_jobs(worker_name, db)
            if WORKER_MODE in {"all", "sync", "metadata", "global-insights"}:
                schedule_due_dataset_pipeline_run()
                advance_dataset_update_pipeline_runs()
                pipeline_step = claim_next_dataset_pipeline_step(worker_name)
                if pipeline_step is not None:
                    with maintain_worker_heartbeat(worker_name):
                        process_dataset_pipeline_step(pipeline_step)
                    continue
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
                ensure_metadata_chunks_for_active_builds()
                metadata_species = claim_next_species_metadata_build(worker_name)
                if metadata_species is not None:
                    with maintain_worker_heartbeat(worker_name):
                        build_species_metadata_record(metadata_species)
                    continue
                metadata_chunk = claim_next_metadata_chunk(worker_name)
                if metadata_chunk is not None:
                    with maintain_worker_heartbeat(worker_name):
                        process_metadata_chunk(metadata_chunk)
                    continue
                if metadata_species is None:
                    with get_sqlite_connection() as db:
                        metadata_build_schedule_hours = metadata_build_hours(db)
                        metadata_refresh_schedule_hours = metadata_refresh_hours(db)
                    schedule_due_metadata_builds(metadata_build_schedule_hours, metadata_refresh_schedule_hours)
                    ensure_metadata_chunks_for_active_builds()
                    metadata_species = claim_next_species_metadata_build(worker_name)
                    if metadata_species is not None:
                        with maintain_worker_heartbeat(worker_name):
                            build_species_metadata_record(metadata_species)
                        continue
                    metadata_chunk = claim_next_metadata_chunk(worker_name)
                    if metadata_chunk is not None:
                        with maintain_worker_heartbeat(worker_name):
                            process_metadata_chunk(metadata_chunk)
                        continue

            if WORKER_MODE == "assembly-backfill":
                backfill_species = claim_next_assembly_feature_backfill(worker_name)
                if backfill_species is not None:
                    with maintain_worker_heartbeat(worker_name):
                        backfill_species_assembly_features(backfill_species)
                    continue

            if WORKER_MODE in {"all", "standardization"}:
                standardization_chunk = claim_next_standardization_refresh_chunk(worker_name)
                if standardization_chunk is not None:
                    with maintain_worker_heartbeat(worker_name):
                        process_standardization_refresh_chunk(standardization_chunk, worker_name)
                    continue
                standardization_task = claim_next_standardization_refresh_task(worker_name)
                if standardization_task is not None:
                    with maintain_worker_heartbeat(worker_name):
                        apply_current_standardization_to_taxon(standardization_task)
                    continue

            if WORKER_MODE in {"all", "metadata", "global-insights"}:
                global_insight_task = claim_next_global_insight_task(worker_name)
                if global_insight_task is not None:
                    with maintain_worker_heartbeat(worker_name):
                        process_global_insight_task(global_insight_task)
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
            relative_path = path.relative_to(root)
            if should_expose_output_file(relative_path):
                files.append(str(relative_path))
    return files


def prepare_sequence_download_artifacts(job: JobRecord) -> None:
    if job.mode != "seq":
        return

    output_root = Path(job.output_dir)
    if not output_root.exists():
        return

    input_path = Path(job.input_path)
    filtered_metadata_path = output_root / "filtered_metadata.csv"
    if input_path.exists():
        shutil.copy2(input_path, filtered_metadata_path)

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

    filters = job.filters or {}
    summary_path = output_root / "sequence_job_summary.txt"
    summary_lines = [
        f"Job ID: {job.id}",
        f"Input: {job.input_name}",
        f"Status: {job.status}",
        f"Created: {job.created_at}",
        f"Updated: {job.updated_at}",
        f"Matched genomes: {filters.get('matched_row_total', 'unknown')}",
        f"Filter logic: {filters.get('filter_logic', 'and')}",
        f"Filters: {filters.get('sequence_filter_sentence') or 'No filters applied'}",
        f"Downloaded FASTA files: {len(fasta_files)}",
        f"Combined FASTA: {'yes' if fasta_files else 'no'}",
        f"Filtered metadata CSV: {'yes' if filtered_metadata_path.exists() else 'no'}",
    ]
    summary_path.write_text("\n".join(str(line) for line in summary_lines) + "\n", encoding="utf-8")

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


def summarize_quality_check_assets(job: JobRecord, output_files: list[str]) -> dict[str, Any] | None:
    if job.mode != "qc":
        return None
    bundle_path = next((path for path in output_files if path == "quality_check_bundle.zip"), None)
    report_path = next((path for path in output_files if path == "sequence_qc/quality_check_report.md"), None)
    html_summary_path = next((path for path in output_files if path == "sequence_qc/quality_check_summary.html"), None)
    summary_path = next((path for path in output_files if path == "sequence_qc/quality_check_summary.json"), None)
    decisions_path = next((path for path in output_files if path == "sequence_qc/qc_decisions.csv"), None)
    all_metadata_path = next((path for path in output_files if path == "sequence_qc/qc_all_metadata.csv"), None)
    pass_metadata_path = next((path for path in output_files if path == "sequence_qc/qc_pass_metadata.csv"), None)
    review_metadata_path = next((path for path in output_files if path == "sequence_qc/qc_review_metadata.csv"), None)
    failed_metadata_path = next((path for path in output_files if path == "sequence_qc/qc_failed_metadata.csv"), None)
    enriched_metadata_path = next((path for path in output_files if path == "sequence_qc/qc_enriched_metadata.csv"), None)
    thresholds_path = next((path for path in output_files if path == "sequence_qc/qc_thresholds_used.json"), None)
    external_master_path = next((path for path in output_files if path == "sequence_qc/external_qc_master_report.csv"), None)
    ani_summary_path = next((path for path in output_files if path == "sequence_qc/external_ani_summary.csv"), None)
    ani_run_status_path = next((path for path in output_files if path == "sequence_qc/external_ani_run_status.tsv"), None)
    mash_closest_path = next((path for path in output_files if path == "sequence_qc/external_mash_closest_neighbors.csv"), None)
    mash_distance_path = next((path for path in output_files if path == "sequence_qc/external_mash_distance_long.csv"), None)
    gtdbtk_bac120_path = next((path for path in output_files if path == "sequence_qc/external_gtdbtk_bac120_summary.tsv"), None)
    gtdbtk_ar53_path = next((path for path in output_files if path == "sequence_qc/external_gtdbtk_ar53_summary.tsv"), None)
    gtdbtk_empty_path = next((path for path in output_files if path == "sequence_qc/external_gtdbtk_empty_summary.tsv"), None)
    external_manifest_path = next((path for path in output_files if path == "external_tools/quality_check/quality_check_manifest.json"), None)
    nextflow_command_path = next((path for path in output_files if path == "external_tools/quality_check/nextflow_command.sh"), None)
    nextflow_log_path = next((path for path in output_files if path == "external_tools/quality_check/nextflow_execution.log"), None)
    if not any([bundle_path, report_path, decisions_path, pass_metadata_path, enriched_metadata_path, external_manifest_path]):
        return None
    summary: dict[str, Any] = {}
    if summary_path:
        try:
            summary = json.loads((Path(job.output_dir) / summary_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            summary = {}
    ani_note = ""
    if ani_run_status_path:
        try:
            ani_status = pd.read_csv(Path(job.output_dir) / ani_run_status_path, sep="\t", dtype=str).fillna("")
            if not ani_status.empty:
                status_row = {str(key): value for key, value in ani_status.iloc[0].to_dict().items()}
                status_value = normalize_metadata_value(status_row.get("status"))
                message_value = normalize_metadata_value(status_row.get("message"))
                if status_value.upper() == "SKIPPED":
                    ani_note = message_value or "ANI skipped"
        except (OSError, pd.errors.EmptyDataError, ValueError):
            ani_note = ""
    mash_note = ""
    if mash_closest_path:
        try:
            mash_frame = pd.read_csv(Path(job.output_dir) / mash_closest_path, dtype=str).fillna("")
            if mash_frame.empty:
                mash_note = "No pairwise Mash hits"
        except (OSError, pd.errors.EmptyDataError, ValueError):
            mash_note = ""
    decision_preview: list[dict[str, Any]] = []
    reason_counts: list[dict[str, Any]] = []
    if decisions_path:
        try:
            decisions = pd.read_csv(Path(job.output_dir) / decisions_path, dtype=str).fillna("")
            decision_preview = build_qc_decision_preview(decisions, limit=25)
            reason_counts = qc_reason_counts(decisions)
            if not html_summary_path and summary:
                try:
                    write_quality_check_html_summary(Path(job.output_dir) / "sequence_qc", job=job, summary=summary, decisions=decisions)
                    html_summary_path = "sequence_qc/quality_check_summary.html"
                except OSError:
                    html_summary_path = None
        except (OSError, pd.errors.EmptyDataError, ValueError):
            decision_preview = []
            reason_counts = []
    return {
        "bundle_path": bundle_path,
        "report_path": report_path,
        "html_summary_path": html_summary_path,
        "summary_path": summary_path,
        "decisions_path": decisions_path,
        "all_metadata_path": all_metadata_path,
        "pass_metadata_path": pass_metadata_path,
        "review_metadata_path": review_metadata_path,
        "failed_metadata_path": failed_metadata_path,
        "enriched_metadata_path": enriched_metadata_path,
        "thresholds_path": thresholds_path,
        "external_master_path": external_master_path,
        "ani_summary_path": ani_summary_path,
        "ani_run_status_path": ani_run_status_path,
        "mash_closest_path": mash_closest_path,
        "mash_distance_path": mash_distance_path,
        "gtdbtk_bac120_path": gtdbtk_bac120_path,
        "gtdbtk_ar53_path": gtdbtk_ar53_path,
        "gtdbtk_empty_path": gtdbtk_empty_path,
        "external_manifest_path": external_manifest_path,
        "nextflow_command_path": nextflow_command_path,
        "nextflow_log_path": nextflow_log_path,
        "summary": summary,
        "decision_preview": decision_preview,
        "reason_counts": reason_counts,
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


def parse_optional_utc(value: Any) -> datetime | None:
    text = normalize_metadata_value(value)
    if not text:
        return None
    try:
        return parse_utc(text)
    except ValueError:
        return None


def compact_datetime_label(value: Any) -> str:
    parsed = parse_optional_utc(value)
    if parsed is None:
        return normalize_metadata_value(value) or "unknown"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def job_runtime_summary(job: JobRecord) -> dict[str, Any]:
    created = parse_optional_utc(job.created_at)
    updated = parse_optional_utc(job.updated_at)
    elapsed = None
    if created is not None and updated is not None:
        elapsed = max(0, int((updated - created).total_seconds()))
    return {
        "created_label": compact_datetime_label(job.created_at),
        "updated_label": compact_datetime_label(job.updated_at),
        "date_label": compact_datetime_label(job.created_at).split(" ", 1)[0],
        "duration_seconds": elapsed,
        "duration_label": format_elapsed_brief(elapsed),
    }


def row_first(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = normalize_metadata_value(row.get(key))
        if value:
            return value
    return ""


def clean_number_label(value: Any, suffix: str = "", decimals: int | None = None) -> str:
    text = normalize_metadata_value(value)
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return text
    if decimals is None:
        if number.is_integer():
            label = f"{int(number):,}"
        else:
            label = f"{number:g}"
    else:
        label = f"{number:,.{decimals}f}".rstrip("0").rstrip(".")
    return f"{label}{suffix}"


def nonnumeric_label(value: Any) -> str:
    text = normalize_metadata_value(value)
    if not text:
        return ""
    try:
        float(text)
    except ValueError:
        return text
    return ""


def clean_positive_percent_label(value: Any, suffix: str = "", decimals: int | None = None) -> str:
    text = positive_percent_text(value)
    if not text:
        return ""
    return clean_number_label(text, suffix, decimals)


def build_qc_decision_preview(decisions: pd.DataFrame, limit: int = 25) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for _, row in decisions.head(limit).iterrows():
        row_dict = {str(key): value for key, value in row.to_dict().items()}
        status = row_first(row_dict, "Sequence_QC_Status", "qc_status") or "unknown"
        gtdbtk_status = row_first(row_dict, "GTDBTK_QC_Status", "gtdbtk_qc_status")
        preview.append(
            {
                "accession": row_first(row_dict, "Assembly Accession", "assembly_accession"),
                "assembly_name": row_first(row_dict, "Assembly Name", "assembly_name"),
                "status": status.lower(),
                "sequence_file": row_first(row_dict, "sequence_file", "Sequence_File"),
                "genome_size": clean_number_label(row_first(row_dict, "QC_total_bp", "total_bp")),
                "contigs": clean_number_label(row_first(row_dict, "QC_contig_count", "contig_count")),
                "n50": clean_number_label(row_first(row_dict, "QC_n50", "n50")),
                "gc": clean_number_label(row_first(row_dict, "QC_gc_percent", "gc_percent"), "%", 2),
                "n_percent": clean_number_label(row_first(row_dict, "QC_ambiguous_n_percent", "ambiguous_n_percent"), "%", 4),
                "completeness": clean_number_label(row_first(row_dict, "CheckM completeness", "Completeness"), "%", 2),
                "contamination": clean_number_label(row_first(row_dict, "CheckM contamination", "Contamination"), "%", 2),
                "checkm2_model": row_first(row_dict, "CheckM2_Model"),
                "coding_density": clean_number_label(row_first(row_dict, "CheckM2_Coding_Density"), "", 3),
                "quast_largest_contig": clean_number_label(row_first(row_dict, "QUAST_Largest_Contig")),
                "quast_ns": clean_number_label(row_first(row_dict, "QUAST_Ns_Per_100kbp"), "", 3),
                "ani_closest": nonnumeric_label(row_first(row_dict, "ANI_Closest_Genome")),
                "ani": clean_positive_percent_label(row_first(row_dict, "ANI_Closest_ANI"), "", 2),
                "ani_status": row_first(row_dict, "ANI_Species_Consistency_Status"),
                "ani_cluster": row_first(row_dict, "ANI_Cluster"),
                "mash_closest": row_first(row_dict, "Mash_Closest_Genome"),
                "mash": clean_number_label(row_first(row_dict, "Mash_Distance"), "", 6),
                "mash_p": clean_number_label(row_first(row_dict, "Mash_P_Value"), "", 3),
                "mash_hashes": row_first(row_dict, "Mash_Matching_Hashes"),
                "gtdbtk": gtdbtk_status,
                "gtdbtk_rank": row_first(row_dict, "GTDBTK_Match_Rank"),
                "gtdbtk_classification": row_first(row_dict, "GTDBTK_Classification"),
                "gtdbtk_genus": row_first(row_dict, "GTDBTK_Genus", "gtdbtk_genus"),
                "gtdbtk_species": row_first(row_dict, "GTDBTK_Species", "gtdbtk_species"),
                "gtdbtk_fastani": clean_number_label(row_first(row_dict, "GTDBTK_FastANI"), "", 2),
                "gtdbtk_reasons": dedupe_reason_text(row_dict.get("GTDBTK_QC_Fail_Reasons")),
                "reasons": dedupe_reason_text(
                    row_dict.get("Sequence_QC_Failure_Reasons"),
                    remove_invalid_ani_warning_reasons(row_dict.get("Sequence_QC_Review_Reasons")),
                    "NO_VALID_ANI_RESULT"
                    if row_first(row_dict, "ANI_Species_Consistency_Status").upper() in {"WARN", "WARNING", "REVIEW"}
                    and row_first(row_dict, "ANI_Closest_ANI")
                    and not positive_percent_text(row_first(row_dict, "ANI_Closest_ANI"))
                    else "",
                    row_dict.get("GTDBTK_QC_Fail_Reasons"),
                ),
            }
        )
    return preview


def qc_reason_counts(decisions: pd.DataFrame, limit: int = 8) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for _, row in decisions.iterrows():
        text = dedupe_reason_text(
            row.get("Sequence_QC_Failure_Reasons"),
            row.get("Sequence_QC_Review_Reasons"),
            row.get("GTDBTK_QC_Fail_Reasons"),
        )
        for reason in [part.strip() for part in text.split(";") if part.strip()]:
            counter[reason] += 1
    return [{"reason": reason, "count": count} for reason, count in counter.most_common(limit)]


def qc_value_list(source: Any, key: str) -> list[str]:
    if hasattr(source, "getlist"):
        return [normalize_metadata_value(value) for value in source.getlist(key)]
    value = source.get(key) if hasattr(source, "get") else None
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [normalize_metadata_value(item) for item in value]
    return [normalize_metadata_value(value)]


def load_quality_filter_frame(quality_job: JobRecord) -> pd.DataFrame:
    enriched_metadata = Path(quality_job.output_dir) / "sequence_qc" / "qc_enriched_metadata.csv"
    if not enriched_metadata.exists():
        enriched_metadata = Path(quality_job.output_dir) / "sequence_qc" / "qc_decisions.csv"
    if not enriched_metadata.exists():
        raise FileNotFoundError("QC metadata is not available yet.")
    return pd.read_csv(enriched_metadata, dtype=str).fillna("")


def apply_quality_post_filters(frame: pd.DataFrame, source: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    statuses = [value.lower() for value in qc_value_list(source, "qc_status")]
    statuses = [value for value in statuses if value in {"pass", "review", "fail"}]
    if not statuses:
        statuses = ["pass"]

    filtered = frame.copy()
    if "Sequence_QC_Status" in filtered.columns:
        filtered = filtered[filtered["Sequence_QC_Status"].astype(str).str.lower().isin(statuses)].copy()

    applied_filters: dict[str, Any] = {"qc_status": statuses}
    numeric_filters = [
        ("min_completeness", "CheckM completeness", ">="),
        ("max_contamination", "CheckM contamination", "<="),
        ("min_n50", "QC_n50", ">="),
        ("max_contigs", "QC_contig_count", "<="),
        ("min_genome_size", "QC_total_bp", ">="),
        ("max_genome_size", "QC_total_bp", "<="),
        ("min_gc_percent", "QC_gc_percent", ">="),
        ("max_gc_percent", "QC_gc_percent", "<="),
        ("max_n_percent", "QC_ambiguous_n_percent", "<="),
        ("min_ani_percent", "ANI_Closest_ANI", ">="),
        ("max_mash_distance", "Mash_Distance", "<="),
    ]
    for form_key, column, operator in numeric_filters:
        threshold = parse_optional_float(source.get(form_key) if hasattr(source, "get") else None)
        if threshold is None or column not in filtered.columns:
            continue
        values = pd.to_numeric(filtered[column], errors="coerce")
        if operator == ">=":
            filtered = filtered[values >= threshold].copy()
        else:
            filtered = filtered[values <= threshold].copy()
        applied_filters[form_key] = threshold

    ani_statuses = [value.upper() for value in qc_value_list(source, "ani_status")]
    ani_statuses = [value for value in ani_statuses if value in {"PASS", "WARN", "FAIL", "SKIP", "NOT_RUN"}]
    if ani_statuses and "ANI_Species_Consistency_Status" in filtered.columns:
        filtered = filtered[filtered["ANI_Species_Consistency_Status"].astype(str).str.upper().isin(ani_statuses)].copy()
        applied_filters["ani_status"] = ani_statuses

    gtdbtk_statuses = [value.upper() for value in qc_value_list(source, "gtdbtk_status")]
    gtdbtk_statuses = [value for value in gtdbtk_statuses if value in {"PASS", "WARN", "FAIL"}]
    if gtdbtk_statuses and "GTDBTK_QC_Status" in filtered.columns:
        filtered = filtered[filtered["GTDBTK_QC_Status"].astype(str).str.upper().isin(gtdbtk_statuses)].copy()
        applied_filters["gtdbtk_status"] = gtdbtk_statuses

    gtdbtk_ranks = [value.lower() for value in qc_value_list(source, "gtdbtk_rank")]
    gtdbtk_ranks = [value for value in gtdbtk_ranks if value in {"genus", "species", "none", "mismatch"}]
    if gtdbtk_ranks and "GTDBTK_Match_Rank" in filtered.columns:
        normalized_rank = filtered["GTDBTK_Match_Rank"].astype(str).str.lower().replace({"": "none"})
        filtered = filtered[normalized_rank.isin(gtdbtk_ranks)].copy()
        applied_filters["gtdbtk_rank"] = gtdbtk_ranks

    return filtered, applied_filters


def summarize_post_qc_filter_preview(quality_job: JobRecord, source: Any) -> dict[str, Any] | None:
    if quality_job.mode != "qc" or not source.get("qc_filter_preview"):
        return None
    try:
        frame = load_quality_filter_frame(quality_job)
    except (OSError, pd.errors.EmptyDataError, ValueError):
        return {
            "available": False,
            "error": "QC metadata is not available yet.",
            "matched": 0,
            "total": 0,
            "applied_filters": {},
            "preview": [],
        }
    filtered, applied_filters = apply_quality_post_filters(frame, source)
    return {
        "available": True,
        "error": "",
        "matched": int(len(filtered)),
        "total": int(len(frame)),
        "applied_filters": applied_filters,
        "preview": build_qc_decision_preview(filtered.fillna(""), limit=12),
        "selected_statuses": applied_filters.get("qc_status", ["pass"]),
        "selected_gtdbtk_statuses": applied_filters.get("gtdbtk_status", []),
        "selected_ani_statuses": applied_filters.get("ani_status", []),
        "selected_gtdbtk_ranks": applied_filters.get("gtdbtk_rank", []),
        "query_args": [(key, value) for key in source.keys() for value in qc_value_list(source, key)],
    }


def numeric_histogram(values: pd.Series, bins: int = 8) -> list[dict[str, Any]]:
    numbers = pd.to_numeric(values, errors="coerce").dropna()
    if numbers.empty:
        return []
    counts, edges = np.histogram(numbers, bins=min(bins, max(1, int(numbers.nunique()))))
    max_count = max(int(count) for count in counts) if len(counts) else 1
    rows = []
    for index, count in enumerate(counts):
        start = edges[index]
        end = edges[index + 1]
        rows.append(
            {
                "label": f"{start:g}-{end:g}",
                "count": int(count),
                "width": round((int(count) / max_count) * 100, 2) if max_count else 0,
            }
        )
    return rows


def load_qc_runtime_tasks(output_dir: Path) -> dict[str, dict[str, Any]]:
    runtime_path = output_dir / "nextflow_qc" / "pipeline_runtime_tasks.tsv"
    if not runtime_path.exists():
        return {}
    try:
        runtime = pd.read_csv(runtime_path, sep="\t", dtype=str).fillna("")
    except (OSError, pd.errors.EmptyDataError, ValueError):
        return {}
    tasks: dict[str, dict[str, Any]] = {}
    for _, row in runtime.iterrows():
        process = normalize_metadata_value(row.get("process")).upper()
        if not process:
            continue
        seconds = None
        try:
            seconds = float(normalize_metadata_value(row.get("realtime_seconds")) or "0")
        except ValueError:
            seconds = None
        tasks[process] = {
            "status": normalize_metadata_value(row.get("status")),
            "duration_seconds": seconds,
            "duration_label": format_elapsed_brief(seconds),
            "memory_label": clean_number_label(row.get("peak_rss_gib"), " GiB", 1),
            "cpu_label": clean_number_label(row.get("cpu_percent"), "%", 1),
        }
    return tasks


def extract_log_event_times(log_text: str) -> dict[str, datetime]:
    events: dict[str, datetime] = {}
    patterns = [
        ("worker", "Launching sequence quality check job"),
        ("sequence_done", "Quality step 2/5"),
        ("stats_started", "Quality step 3/5"),
        ("external_started", "Starting external Nextflow QC"),
        ("external_done", "External Nextflow QC finished"),
        ("decisions", "Quality step 4/5"),
        ("pass_subset", "Quality step 5/5"),
        ("finished", "Job finished with return code"),
    ]
    for line in [line for line in log_text.splitlines() if line.strip()]:
        timestamp = parse_log_timestamp(line)
        if not timestamp:
            continue
        try:
            parsed = parse_utc(timestamp)
        except ValueError:
            continue
        for key, pattern in patterns:
            if key not in events and pattern in line:
                events[key] = parsed
    return events


def compact_qc_progress(job: JobRecord, log_text: str, quality_check_assets: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    if job.mode != "qc":
        return []
    output_dir = Path(job.output_dir)
    tasks = load_qc_runtime_tasks(output_dir)
    events = extract_log_event_times(log_text)
    created = parse_optional_utc(job.created_at)
    updated = parse_optional_utc(job.updated_at)

    def elapsed_between(start: datetime | None, end: datetime | None) -> str:
        if start is None or end is None:
            return ""
        return format_elapsed_brief((end - start).total_seconds())

    selected_modules = set((quality_check_assets or {}).get("summary", {}).get("selected_modules") or [])
    module_rows = [
        ("checkm2", "Comprehensive QC 1", "CheckM2 completeness/contamination", "CHECKM2_QC"),
        ("quast", "Comprehensive QC 2", "QUAST assembly metrics", "QUAST_QC"),
        ("ani", "Comprehensive QC 3", "ANI species consistency", "ANI_ANALYSIS"),
        ("mash", "Comprehensive QC 4", "Mash nearest-neighbor screen", "MASH_PRESCREEN"),
        ("gtdbtk", "Comprehensive QC 5", "GTDB-Tk taxonomy check", "GTDBTK_QC"),
        ("combined", "Comprehensive QC 6", "Combined pass/review/fail decision", "COMBINED_QC"),
    ]

    rows = [
        {
            "label": "Queued",
            "state": "done" if created else "pending",
            "detail": f"Started {compact_datetime_label(job.created_at)}",
            "duration": "",
        },
        {
            "label": "Sequence Collected",
            "state": "done" if events.get("sequence_done") or job.status == "completed" else "pending",
            "detail": f"{(quality_check_assets or {}).get('summary', {}).get('total', 'Selected')} genome FASTA files prepared",
            "duration": elapsed_between(created, events.get("sequence_done")),
        },
        {
            "label": "Assembly Statistics",
            "state": "done" if events.get("stats_started") or job.status == "completed" else "pending",
            "detail": "Genome size, contigs, N50, GC%, and ambiguous bases calculated",
            "duration": elapsed_between(events.get("sequence_done"), events.get("external_started")),
        },
    ]
    for module_key, stage, label, process in module_rows:
        if module_key != "combined" and selected_modules and module_key not in selected_modules:
            continue
        task = tasks.get(process, {})
        state = "done" if task or job.status == "completed" else "pending"
        detail = label
        if task.get("memory_label"):
            detail = f"{detail}; peak memory {task['memory_label']}"
        rows.append(
            {
                "label": stage,
                "state": state,
                "detail": detail,
                "duration": task.get("duration_label") or "",
            }
        )
    rows.append(
        {
            "label": "Finished",
            "state": "done" if job.status == "completed" else ("current" if job.status == "running" else job.status),
            "detail": f"{(quality_check_assets or {}).get('summary', {}).get('pass', '—')} pass, {(quality_check_assets or {}).get('summary', {}).get('review', '—')} review, {(quality_check_assets or {}).get('summary', {}).get('fail', '—')} fail",
            "duration": elapsed_between(created, updated),
        }
    )
    return rows


def write_quality_check_html_summary(
    qc_dir: Path,
    *,
    job: JobRecord,
    summary: dict[str, Any],
    decisions: pd.DataFrame,
) -> None:
    output_root = Path(job.output_dir)
    filters = job.filters or {}
    thresholds = summary.get("thresholds") or {}
    status_counts = {
        "pass": int(summary.get("pass") or 0),
        "review": int(summary.get("review") or 0),
        "fail": int(summary.get("fail") or 0),
    }
    display_total = int(summary.get("total") or sum(status_counts.values()) or len(decisions) or 0)
    total = max(display_total, 1)
    runtime = job_runtime_summary(job)
    reason_rows = qc_reason_counts(decisions, limit=16)
    preview_rows = build_qc_decision_preview(decisions, limit=50)

    def column_counter(column: str, *, uppercase: bool = False, fallback: str = "Not reported") -> Counter[str]:
        if column not in decisions.columns:
            return Counter()
        values = []
        for value in decisions[column].fillna(""):
            label = normalize_metadata_value(value) or fallback
            values.append(label.upper() if uppercase else label)
        return Counter(values)

    gtdbtk_counts = column_counter("GTDBTK_QC_Status", uppercase=True)
    gtdbtk_rank_counts = column_counter("GTDBTK_Match_Rank")
    gtdbtk_species_counts = column_counter("GTDBTK_Species")
    ani_status_counts = column_counter("ANI_Species_Consistency_Status", uppercase=True)
    mash_neighbor_counts = column_counter("Mash_Closest_Genome")
    histograms = {
        "Completeness": numeric_histogram(decisions.get("CheckM completeness", pd.Series(dtype=str))),
        "Contamination": numeric_histogram(decisions.get("CheckM contamination", pd.Series(dtype=str))),
        "Genome size": numeric_histogram(decisions.get("QC_total_bp", pd.Series(dtype=str))),
        "Contigs": numeric_histogram(decisions.get("QC_contig_count", pd.Series(dtype=str))),
        "N50": numeric_histogram(decisions.get("QC_n50", pd.Series(dtype=str))),
        "GC percent": numeric_histogram(decisions.get("QC_gc_percent", pd.Series(dtype=str))),
        "Ambiguous N percent": numeric_histogram(decisions.get("QC_ambiguous_n_percent", pd.Series(dtype=str))),
        "ANI nearest percent": numeric_histogram(decisions.get("ANI_Closest_ANI", pd.Series(dtype=str))),
        "Mash distance": numeric_histogram(decisions.get("Mash_Distance", pd.Series(dtype=str))),
    }

    def esc(value: Any) -> str:
        return html.escape(normalize_metadata_value(value) or "—")

    def url(relative_path: str) -> str:
        return f"/jobs/{esc(job.id)}/files/{html.escape(relative_path, quote=True)}"

    def available_link(relative_path: str, label: str, detail: str) -> str:
        if not (output_root / relative_path).exists():
            return ""
        return (
            f'<a class="file-card" href="{url(relative_path)}">'
            f"<strong>{esc(label)}</strong><span>{esc(detail)}</span></a>"
        )

    def status_bar() -> str:
        colors = {"pass": "#1b6b39", "review": "#b8844d", "fail": "#8e2f1d"}
        segments = []
        for status, count in status_counts.items():
            width = (count / total) * 100
            segments.append(
                f'<span style="width:{width:.2f}%;background:{colors[status]}" title="{status}: {count}"></span>'
            )
        return "".join(segments)

    selected_modules = [normalize_metadata_value(module) for module in summary.get("selected_modules", [])]
    selected_modules = [module for module in selected_modules if module]
    selected_modules_html = "".join(f"<span>{esc(module)}</span>" for module in selected_modules) or "<span>quick_fasta</span>"

    threshold_items = []
    for key, value in thresholds.items():
        if value is None or normalize_metadata_value(value) == "":
            continue
        threshold_items.append(f"<li><strong>{esc(key.replace('_', ' '))}</strong>: {esc(value)}</li>")
    threshold_html = "".join(threshold_items) or "<li>No hard threshold was applied for this profile.</li>"

    table_rows = []
    for row in preview_rows:
        table_rows.append(
            "<tr>"
            f"<td>{esc(row.get('accession'))}</td>"
            f"<td><strong>{esc(row.get('status'))}</strong></td>"
            f"<td>{esc(row.get('completeness'))}</td>"
            f"<td>{esc(row.get('contamination'))}</td>"
            f"<td>{esc(row.get('genome_size'))}</td>"
            f"<td>{esc(row.get('contigs'))}</td>"
            f"<td>{esc(row.get('n50'))}</td>"
            f"<td>{esc(row.get('gc'))}</td>"
            f"<td>{esc(row.get('ani'))}</td>"
            f"<td>{esc(row.get('mash'))}</td>"
            f"<td>{esc(row.get('gtdbtk'))}</td>"
            f"<td>{esc(row.get('gtdbtk_species') or row.get('gtdbtk_genus'))}</td>"
            f"<td>{esc(row.get('gtdbtk_fastani'))}</td>"
            f"<td>{esc(row.get('reasons'))}</td>"
            "</tr>"
        )

    reason_html = "".join(
        f"<li><strong>{count}</strong> {esc(reason)}</li>"
        for reason, count in [(item["reason"], item["count"]) for item in reason_rows]
    ) or "<li>No failure or review reasons reported.</li>"

    def count_bars(counter: Counter[str], limit: int = 8) -> str:
        if not counter:
            return "<p>No values reported.</p>"
        max_count = max(counter.values()) or 1
        rows = []
        for label, count in counter.most_common(limit):
            width = (count / max_count) * 100
            rows.append(
                f'<div class="bar-row"><span>{esc(label)}</span><strong>{count}</strong>'
                f'<i style="width:{width:.2f}%"></i></div>'
            )
        return "".join(rows)

    def histogram_html(title: str, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return f'<div class="mini-chart"><h3>{esc(title)}</h3><p>No numeric values reported.</p></div>'
        bars = "".join(
            f'<div class="hist-row"><span>{esc(row["label"])}</span><i style="width:{row["width"]}%"></i><strong>{row["count"]}</strong></div>'
            for row in rows
        )
        return f'<div class="mini-chart"><h3>{esc(title)}</h3>{bars}</div>'

    histogram_section = "".join(histogram_html(title, rows) for title, rows in histograms.items())
    important_links = "".join(
        [
            available_link("quality_check_bundle.zip", "Download QC ZIP", "Complete QC bundle with reports and key tables."),
            available_link("sequence_qc/qc_decisions.csv", "Download decisions CSV", "Pass/fail decision and reasons per assembly."),
            available_link("sequence_qc/qc_all_metadata.csv", "All QC metadata", "Every checked genome with QC columns appended."),
            available_link("sequence_qc/qc_pass_metadata.csv", "Passed metadata", "Genome metadata passing the applied QC filters."),
            available_link("sequence_qc/qc_review_metadata.csv", "Review metadata", "Rows that need review, when review mode is used."),
            available_link("sequence_qc/qc_failed_metadata.csv", "Failed metadata", "Rows failing at least one applied threshold."),
            available_link("sequence_qc/external_qc_master_report.csv", "External QC master report", "Imported CheckM2/QUAST/ANI/Mash/GTDB-Tk report."),
            available_link("sequence_qc/quality_check_report.md", "Markdown report", "Plain-text report with thresholds and outputs."),
        ]
    )
    if not important_links:
        important_links = "<p>No downloadable QC files were found yet.</p>"
    filter_sentence = normalize_metadata_value(filters.get("sequence_filter_sentence"))
    profile = normalize_metadata_value(summary.get("quality_profile")) or "Quality Check"
    decision_source = normalize_metadata_value(summary.get("qc_decision_source")) or "built_in"
    run_mode = normalize_metadata_value(summary.get("run_mode")) or "qc"

    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>FetchM Web QC Summary - {esc(job.id)}</title>
  <style>
    :root {{ --ink: #17332e; --accent: #165c4e; --paper: #fffaf0; --line: rgba(22, 92, 78, 0.16); --muted: #67746d; }}
    body {{ margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--ink); background: #f7f2e8; }}
    main {{ max-width: 1240px; margin: 0 auto; padding: 32px 22px; }}
    .hero, .card {{ border: 1px solid rgba(22, 92, 78, 0.16); border-radius: 24px; background: rgba(255,255,255,0.82); box-shadow: 0 18px 50px rgba(28, 45, 40, 0.08); }}
    .hero {{ padding: 28px; background: radial-gradient(circle at top right, rgba(22,92,78,.14), transparent 35%), rgba(255,255,255,.86); }}
    .eyebrow {{ margin: 0 0 8px; text-transform: uppercase; letter-spacing: .08em; font-weight: 800; color: #165c4e; font-size: 12px; }}
    h1 {{ margin: 0 0 8px; font-size: clamp(30px, 5vw, 54px); line-height: .95; }}
    h2 {{ margin-top: 0; }}
    code {{ background: #fff2d7; border-radius: 8px; padding: 2px 6px; }}
    .lede {{ max-width: 820px; color: var(--muted); }}
    .pill-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 0; }}
    .pill-row span, .pill {{ display: inline-flex; padding: 7px 10px; border-radius: 999px; background: #e8f2ed; color: var(--accent); font-weight: 800; font-size: 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }}
    .metric {{ padding: 16px; border-radius: 18px; background: #fff; border: 1px solid rgba(22,92,78,.12); }}
    .metric strong {{ display: block; font-size: 30px; }}
    .metric span {{ color: var(--muted); }}
    .bar {{ display: flex; height: 16px; overflow: hidden; border-radius: 999px; background: #eadfca; }}
    .bar span {{ display: block; }}
    .card {{ padding: 22px; margin-top: 18px; }}
    .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; }}
    .mini-chart {{ padding: 16px; border: 1px solid rgba(22,92,78,.12); border-radius: 18px; background: #fff; }}
    .mini-chart h3 {{ margin: 0 0 12px; }}
    .hist-row, .bar-row {{ display: grid; grid-template-columns: 96px minmax(20px, 1fr) 42px; gap: 8px; align-items: center; margin: 8px 0; font-size: 12px; }}
    .hist-row i, .bar-row i {{ display: block; height: 10px; min-width: 3px; border-radius: 999px; background: #165c4e; }}
    .bar-row {{ grid-template-columns: minmax(90px, 1fr) 42px minmax(20px, 1.2fr); }}
    .file-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .file-card {{ display: grid; gap: 4px; padding: 14px; border-radius: 16px; border: 1px solid rgba(22,92,78,.14); background: #fff; text-decoration: none; }}
    .file-card span {{ color: var(--muted); font-weight: 500; font-size: 13px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1180px; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid rgba(22,92,78,.12); border-radius: 18px; background: #fff; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #eee4d2; text-align: left; font-size: 13px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #fff8ea; color: #165c4e; }}
    li {{ margin: 8px 0; }}
    a {{ color: #165c4e; font-weight: 800; }}
    .two-col {{ display: grid; grid-template-columns: minmax(0, .9fr) minmax(0, 1.1fr); gap: 18px; }}
    @media (max-width: 860px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
<main>
  <section class="hero">
    <p class="eyebrow">FetchM Web Quality Summary</p>
    <h1>QC results for job {esc(job.id)}</h1>
    <p class="lede">{esc(profile)} using {esc(decision_source)} decisions. {esc(filter_sentence) if filter_sentence else 'This report summarizes the selected genome set, QC thresholds, distributions, taxonomy checks, and important output files.'}</p>
    <div class="pill-row">
      <span>{esc(run_mode)}</span>
      <span>{esc(summary.get('decision_mode', 'pass_fail'))}</span>
      <span>Created {esc(runtime.get('created_label'))}</span>
      <span>Runtime {esc(runtime.get('duration_label'))}</span>
    </div>
    <div class="grid">
      <div class="metric"><strong>{status_counts['pass']}</strong><span>Pass</span></div>
      <div class="metric"><strong>{status_counts['review']}</strong><span>Review</span></div>
      <div class="metric"><strong>{status_counts['fail']}</strong><span>Fail</span></div>
      <div class="metric"><strong>{display_total}</strong><span>Total checked</span></div>
    </div>
    <div class="bar">{status_bar()}</div>
  </section>
  <section class="card two-col">
    <div>
      <h2>Run Configuration</h2>
      <p class="eyebrow">Selected modules</p>
      <div class="pill-row">{selected_modules_html}</div>
    </div>
    <div>
      <h2>Thresholds Used</h2>
      <ul>{threshold_html}</ul>
    </div>
  </section>
  <section class="card">
    <h2>Top Review/Failure Reasons</h2>
    <ul>{reason_html}</ul>
  </section>
  <section class="card">
    <h2>QC Figures</h2>
    <div class="chart-grid">{histogram_section}</div>
  </section>
  <section class="card">
    <h2>ANI, Mash, and GTDB-Tk Summary</h2>
    <div class="chart-grid">
      <div class="mini-chart"><h3>ANI status</h3>{count_bars(ani_status_counts)}</div>
      <div class="mini-chart"><h3>Nearest Mash neighbors</h3>{count_bars(mash_neighbor_counts)}</div>
      <div class="mini-chart"><h3>GTDB-Tk match rank</h3>{count_bars(gtdbtk_rank_counts)}</div>
    </div>
  </section>
  <section class="card">
    <h2>GTDB-Tk Taxonomy Summary</h2>
    <div class="chart-grid">
      <div class="mini-chart"><h3>GTDB-Tk status</h3>{count_bars(gtdbtk_counts)}</div>
      <div class="mini-chart"><h3>Top GTDB species</h3>{count_bars(gtdbtk_species_counts)}</div>
    </div>
  </section>
  <section class="card">
    <h2>Important Files</h2>
    <p>Use the job detail page to launch sequence downloads from passed or currently filtered genomes. These links expose the compact QC tables and reports.</p>
    <div class="file-grid">{important_links}</div>
    <p><a href="/jobs/{esc(job.id)}">Back to QC job page</a></p>
  </section>
  <section class="card">
    <h2>Comprehensive QC Table Preview</h2>
    <p>The full table is available in <code>qc_decisions.csv</code>. This preview includes the first 50 assemblies.</p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Assembly</th><th>Status</th><th>Completeness</th><th>Contamination</th><th>Genome size</th><th>Contigs</th><th>N50</th><th>GC</th><th>ANI</th><th>Mash</th><th>GTDB-Tk</th><th>GTDB species/genus</th><th>GTDB reference ANI</th><th>Reason</th></tr></thead>
        <tbody>{''.join(table_rows)}</tbody>
      </table>
    </div>
  </section>
</main>
</body>
</html>
"""
    (qc_dir / "quality_check_summary.html").write_text(html_text, encoding="utf-8")


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
    if job.mode in {"seq", "qc"}:
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
    if mode == "qc":
        return [
            {"key": "queued", "label": "Queued", "matches": []},
            {"key": "worker", "label": "Worker started", "matches": ["Launching sequence quality check job"]},
            {"key": "input", "label": "Input loaded", "matches": ["Quality step 1/5"]},
            {"key": "downloads", "label": "Sequence download", "matches": ["Quality step 2/5", "Downloaded genome FASTA", "Genome FASTA already exists"]},
            {"key": "stats", "label": "Assembly statistics", "matches": ["Quality step 3/5"]},
            {"key": "external_qc", "label": "External QC", "matches": ["Starting external Nextflow QC", "External Nextflow QC finished", "Imported external QC master report"]},
            {"key": "decisions", "label": "QC decisions", "matches": ["Quality step 4/5"]},
            {"key": "pass_subset", "label": "Pass subset ready", "matches": ["Quality step 5/5"]},
            {"key": "finished", "label": "Finished", "matches": ["Job finished with return code 0"]},
        ]

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
    if job.mode in {"seq", "qc"}:
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


def remove_job_directory(job_id: str) -> tuple[bool, str | None]:
    job_root = job_dir(job_id)
    if not job_root.exists():
        return True, None
    try:
        shutil.rmtree(job_root)
        return True, None
    except FileNotFoundError:
        return True, None
    except OSError as exc:
        logging.warning("Could not remove job directory %s: %s", job_root, exc)
        return False, str(exc)


def cleanup_old_jobs(*, older_than_days: int) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    removable_statuses = {"completed", "failed", "cancelled"}
    removed = 0
    skipped = 0
    errors: list[dict[str, str]] = []
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
        removed_files, error = remove_job_directory(str(row["id"]))
        if not removed_files:
            skipped += 1
            errors.append({"job_id": str(row["id"]), "error": error or "unknown file-removal error"})
            continue
        db.execute("DELETE FROM jobs WHERE id = ?", (row["id"],))
        removed += 1
    db.commit()
    return {"removed": removed, "skipped": skipped, "errors": errors}


def list_metadata_claims(limit: int = 12) -> list[dict[str, Any]]:
    rows = get_db().execute(
        """
        SELECT id, species_name, taxon_rank, genome_count, metadata_status,
               metadata_claimed_by, metadata_claimed_at, metadata_error,
               metadata_progress_total, metadata_progress_completed,
               metadata_progress_current_accession
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
                "progress": summarize_metadata_build_progress(row["metadata_progress_total"], row["metadata_progress_completed"]),
                "current_accession": str(row["metadata_progress_current_accession"]) if row["metadata_progress_current_accession"] else None,
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
def enforce_csrf() -> None:
    if request.method == "POST":
        validate_csrf_token()


@app.before_request
def enforce_auth() -> Any:
    return require_auth()


@app.after_request
def audit_admin_post(response: Any) -> Any:
    endpoint = request.endpoint or ""
    if request.method == "POST" and endpoint.startswith("admin_") and response.status_code < 400:
        record_audit_event(
            f"admin.{endpoint}",
            metadata={"status_code": response.status_code},
        )
    return response


@app.route("/login", methods=["GET", "POST"])
def login() -> Any:
    if g.current_user is not None:
        return redirect(url_for("index"))

    if request.method == "POST":
        if auth_rate_limited("login"):
            record_audit_event("auth.login_rate_limited")
            flash("Too many sign-in attempts. Try again later.", "error")
            return render_template("login.html"), 429
        username = normalize_username(request.form.get("login_identifier") or request.form.get("username") or "")
        password = request.form.get("password") or ""
        user = get_user_by_username(username)
        if user is not None and check_password_hash(user["password_hash"], password):
            login_user(user)
            record_audit_event("auth.login_success", target_type="user", target_id=str(user["id"]))
            flash("Signed in.", "success")
            target = request.args.get("next") or url_for("index")
            return redirect(target)
        record_audit_event("auth.login_failure", metadata={"username": username})
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
        if auth_rate_limited("password_reset"):
            record_audit_event("auth.password_reset_rate_limited")
            flash("Too many password reset attempts. Try again later.", "error")
            return render_template("forgot_password.html", mail_configured=mail_is_configured()), 429
        identifier = (request.form.get("identifier") or "").strip()
        user = get_user_by_email(identifier) or get_user_by_username(identifier)
        if user is None:
            record_audit_event("auth.password_reset_unknown_identifier")
            flash("No account matched that username or email.", "error")
        else:
            token = create_reset_token(int(user["id"]))
            reset_url = url_for("reset_password", token=token, _external=True)
            try:
                send_reset_email(str(user["email"]), str(user["username"]), reset_url)
                record_audit_event("auth.password_reset_requested", target_type="user", target_id=str(user["id"]))
                flash("Password reset email sent.", "success")
            except Exception as exc:
                record_audit_event("auth.password_reset_send_failed", target_type="user", target_id=str(user["id"]))
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
    if g.current_user is not None:
        record_audit_event("auth.logout", target_type="user", target_id=str(g.current_user["id"]))
    logout_user()
    flash("Logged out.", "success")
    return redirect(url_for("login"))


@app.route("/")
def index() -> str:
    user = g.current_user
    jobs = list_jobs_for_user(int(user["id"])) if user is not None else []
    return render_template(
        "index_dashboard.html",
        jobs=jobs,
        modes=MODES,
        home_metrics=build_public_home_metrics(),
        taxon_recent_hours=TAXON_RECENT_HOURS,
        taxon_very_old_hours=TAXON_VERY_OLD_HOURS,
    )


@app.route("/healthz")
def healthz() -> Any:
    return {"commit": APP_COMMIT, "status": "ok", "version": APP_VERSION}


@app.route("/global-insights")
def global_insights() -> str:
    summary, snapshot_dir = load_latest_global_insights_summary()
    simulator_filters = {
        "taxon": (request.args.get("taxon") or "").strip(),
        "country": (request.args.get("country") or "").strip(),
        "host": (request.args.get("host") or "").strip(),
        "year_from": (request.args.get("year_from") or "").strip(),
        "year_to": (request.args.get("year_to") or "").strip(),
        "assembly_level": (request.args.get("assembly_level") or "").strip(),
    }
    simulator_result = None
    if summary and snapshot_dir and any(simulator_filters.values()):
        simulator_relative = ((summary.get("downloads") or {}).get("simulator_records") or "").strip()
        if simulator_relative:
            simulator_result = run_standardization_simulator(snapshot_dir / simulator_relative, simulator_filters)
    return render_template(
        "global_insights.html",
        summary=summary,
        snapshot_dir=snapshot_dir,
        latest_task=latest_global_insight_task(),
        simulator_filters=simulator_filters,
        simulator_result=simulator_result,
    )


@app.route("/global-insights/download/<path:relative_path>")
def download_global_insights_file(relative_path: str) -> Any:
    summary, snapshot_dir = load_latest_global_insights_summary()
    if not summary or not snapshot_dir:
        abort(404)
    allowed_files = {str(value) for value in (summary.get("downloads") or {}).values() if value}
    allowed_files.add("summary.json")
    normalized = Path(relative_path)
    if normalized.is_absolute() or ".." in normalized.parts:
        abort(404)
    relative = normalized.as_posix()
    if relative not in allowed_files:
        abort(404)
    path = snapshot_dir / relative
    if not path.exists() or not path.is_file():
        abort(404)
    record_audit_event("download.global_insights", target_type="global_insights", target_id=str(summary.get("snapshot_id") or "latest"), metadata={"file": relative})
    return send_from_directory(snapshot_dir, relative, as_attachment=True)


@app.route("/api/taxa/search")
def api_taxa_search() -> Any:
    query = normalize_species_name(request.args.get("q") or "")
    if len(query) < 2:
        return app.response_class(json.dumps({"results": []}), mimetype="application/json")

    search = species_search_name(query)
    like_value = f"%{search}%"
    starts_value = f"{search}%"
    rows = get_db().execute(
        """
        SELECT id, species_name, taxon_rank, COALESCE(live_genome_count, genome_count) AS genome_count, assembly_source
        FROM species
        WHERE is_live = 1
          AND live_status = 'ready'
          AND live_tsv_path IS NOT NULL
          AND lower(species_name) LIKE ?
        ORDER BY
            CASE WHEN lower(species_name) LIKE ? THEN 0 ELSE 1 END,
            COALESCE(genome_count, 0) DESC,
            species_name COLLATE NOCASE ASC
        LIMIT 8
        """,
        (like_value, starts_value),
    ).fetchall()
    results = [
        {
            "id": int(row["id"]),
            "species_name": str(row["species_name"]),
            "taxon_rank": str(row["taxon_rank"]),
            "genome_count": int(row["genome_count"] or 0),
            "assembly_source": str(row["assembly_source"] or "all"),
            "source": "catalog",
        }
        for row in rows
    ]
    seen_names = {species_search_name(item["species_name"]) for item in results}

    if g.current_user is not None:
        metadata_rows = get_db().execute(
            """
            SELECT m.source_taxon_id, m.source_taxon_name, m.species_name, m.genome_count
            FROM metadata_species_search m
            JOIN species s ON s.id = m.source_taxon_id
            WHERE m.search_name LIKE ?
              AND s.is_live = 1
              AND s.live_metadata_status = 'ready'
              AND s.live_metadata_clean_path IS NOT NULL
            ORDER BY
                CASE WHEN m.search_name LIKE ? THEN 0 ELSE 1 END,
                m.genome_count DESC,
                m.species_name COLLATE NOCASE ASC
            LIMIT 8
            """,
            (like_value, starts_value),
        ).fetchall()
        for row in metadata_rows:
            name = str(row["species_name"])
            key = species_search_name(name)
            if key in seen_names:
                continue
            results.append(
                {
                    "id": None,
                    "species_name": name,
                    "taxon_rank": "species",
                    "genome_count": int(row["genome_count"] or 0),
                    "assembly_source": "all",
                    "source": "genus_metadata",
                    "source_taxon_id": int(row["source_taxon_id"]),
                    "source_taxon_name": str(row["source_taxon_name"]),
                    "requires_prepare": True,
                }
            )
            seen_names.add(key)
            if len(results) >= 8:
                break

    return app.response_class(json.dumps({"results": results}), mimetype="application/json")


@app.route("/api/taxa/prepare-metadata-species", methods=["POST"])
def api_prepare_metadata_species() -> Any:
    user = g.current_user
    assert user is not None
    species_name = request.form.get("species_name") or ""
    source_taxon_raw = request.form.get("source_taxon_id") or ""
    try:
        source_taxon_id = int(source_taxon_raw)
        species = ensure_species_from_genus_metadata(species_name, source_taxon_id)
    except Exception as exc:
        return app.response_class(
            json.dumps({"error": str(exc)}),
            status=400,
            mimetype="application/json",
        )
    return app.response_class(
        json.dumps(
            {
                "id": species.id,
                "species_name": species.species_name,
                "taxon_rank": species.taxon_rank,
                "genome_count": species.genome_count or 0,
                "assembly_source": species.assembly_source,
                "source": "catalog",
            }
        ),
        mimetype="application/json",
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
        dataset_pipeline=build_dataset_pipeline_dashboard(),
        observability=build_observability_dashboard(),
        security_posture=build_security_posture(),
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
        taxa=list_recent_metadata_taxa(100),
        metadata_dashboard=build_metadata_dashboard(),
        **admin_common_context("metadata"),
    )


@app.route("/admin/refinement")
def admin_refinement() -> str:
    require_admin()
    limit = parse_optional_int(request.args.get("limit"))
    filters = refinement_filters_from_mapping(request.args)
    dashboard = build_refinement_dashboard(limit or 50, filters=filters)
    return render_template(
        "admin_refinement.html",
        refinement=dashboard,
        **admin_common_context("refinement"),
    )


@app.route("/admin/refinement/approve", methods=["POST"])
def admin_refinement_approve() -> Any:
    user = require_admin()
    limit = parse_optional_int(request.form.get("limit")) or 50
    filters = refinement_filters_from_mapping(request.form)
    try:
        save_approved_standardization_rule(
            source_column=(request.form.get("source_column") or "").strip(),
            original_value=(request.form.get("original_value") or "").strip(),
            category=(request.form.get("category") or "").strip(),
            destination=(request.form.get("destination") or "").strip(),
            proposed_value=(request.form.get("proposed_value") or "").strip(),
            ontology_id=(request.form.get("ontology_id") or "").strip(),
            method=(request.form.get("method") or "manual_review").strip(),
            confidence=(request.form.get("item_confidence") or request.form.get("confidence") or "medium").strip(),
            note=(request.form.get("note") or "").strip(),
            approved_by=str(user["username"]),
        )
    except ValueError as exc:
        flash(str(exc), "error")
    else:
        flash("Standardization rule approved. Future metadata standardization can use this mapping.", "success")
        filters["status"] = "all"
    return redirect(
        url_for(
            "admin_refinement",
            limit=limit,
            source=filters["source"],
            status=filters["status"],
            confidence=filters["confidence"],
            min_count=filters["min_count"],
        )
    )


@app.route("/admin/refinement/bulk-approve", methods=["POST"])
def admin_refinement_bulk_approve() -> Any:
    user = require_admin()
    limit = parse_optional_int(request.form.get("limit")) or 50
    filters = refinement_filters_from_mapping(request.form)
    mode = (request.form.get("approval_mode") or "selected").strip()
    if mode == "high_confidence_visible":
        payloads = visible_high_confidence_refinement_payloads(filters, limit)
    else:
        payloads = request.form.getlist("rule")
    summary = approve_refinement_rule_payloads(payloads, str(user["username"]))
    if summary["approved"]:
        flash(
            f"Approved {summary['approved']} standardization rules. "
            f"Skipped {summary['skipped']}; failed {summary['failed']}.",
            "success" if summary["failed"] == 0 else "error",
        )
        filters["status"] = "all"
    else:
        flash("No eligible standardization rules were selected or visible.", "error")
    return redirect(
        url_for(
            "admin_refinement",
            limit=limit,
            source=filters["source"],
            status=filters["status"],
            confidence=filters["confidence"],
            min_count=filters["min_count"],
        )
    )


@app.route("/admin/refinement/export.csv")
def admin_refinement_export() -> Any:
    require_admin()
    limit = parse_optional_int(request.args.get("limit"))
    filters = refinement_filters_from_mapping(request.args)
    dashboard = build_refinement_dashboard(limit or 200, sample_rows=500000, filters=filters)
    rows: list[dict[str, Any]] = []
    for section in dashboard["sections"]:
        rows.extend(section["items"])
    output = StringIO()
    fieldnames = [
        "source_column",
        "value",
        "count",
        "category",
        "destination",
        "proposed_value",
        "ontology_id",
        "method",
        "confidence",
        "suggestion_score",
        "action",
        "note",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=fetchm_refinement_review.csv"},
    )


@app.route("/admin/host-curation")
def admin_host_curation() -> str:
    require_admin()
    dashboard = build_host_curation_dashboard(request.args)
    return render_template(
        "admin_host_curation.html",
        host_curation=dashboard,
        **admin_common_context("standardization"),
    )


@app.route("/admin/host-curation/approve", methods=["POST"])
def admin_host_curation_approve() -> Any:
    user = require_admin()
    filters = host_curation_filters_from_mapping(request.form)
    row = {
        "raw_host": request.form.get("raw_host") or "",
        "proposed_host": request.form.get("proposed_host") or "",
        "taxid": request.form.get("taxid") or "",
        "note": request.form.get("note") or "",
    }
    action = normalize_standardization_lookup(request.form.get("action") or "")
    if action == "ignore":
        flash("Host value left for later review.", "success")
    else:
        try:
            save_host_curation_decision(row, action, str(user["username"]))
        except ValueError as exc:
            flash(str(exc), "error")
        else:
            flash("Host curation rule approved.", "success")
            filters["status"] = "all"
    return redirect(
        url_for(
            "admin_host_curation",
            decision=filters["decision"],
            status=filters["status"],
            q=filters["q"],
            limit=filters["limit"],
        )
    )


@app.route("/admin/host-curation/bulk-approve", methods=["POST"])
def admin_host_curation_bulk_approve() -> Any:
    user = require_admin()
    action = normalize_standardization_lookup(request.form.get("bulk_action") or "")
    filters = host_curation_filters_from_mapping(request.form)
    if action not in {"non_host_source", "missing"}:
        flash("Bulk approval is only allowed for non-host source and missing decisions.", "error")
        return redirect(
            url_for(
                "admin_host_curation",
                decision=filters["decision"],
                status=filters["status"],
                q=filters["q"],
                limit=filters["limit"],
            )
        )
    dashboard = build_host_curation_dashboard({**filters, "decision": action, "status": "unapproved"})
    approved = 0
    failed = 0
    for row in dashboard["rows"]:
        if row.get("decision") != action:
            continue
        try:
            save_host_curation_decision(row, action, str(user["username"]))
        except ValueError:
            failed += 1
            continue
        approved += 1
    flash(
        f"Bulk approved {approved} {action.replace('_', ' ')} host rules. Failed {failed}.",
        "success" if failed == 0 else "error",
    )
    return redirect(url_for("admin_host_curation", decision=action, status="all", limit=filters["limit"]))


@app.route("/admin/host-curation/export.csv")
def admin_host_curation_export() -> Any:
    require_admin()
    filters = host_curation_filters_from_mapping(request.args)
    dashboard = build_host_curation_dashboard(filters)
    output = StringIO()
    fieldnames = ["raw_host", "count", "decision", "proposed_host", "taxid", "confidence", "note", "is_approved"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in dashboard["rows"]:
        writer.writerow(row)
    label = filters["decision"] if filters["decision"] != "all" else "host_curation"
    return app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={label}_review.csv"},
    )


@app.route("/admin/host-curation/apply", methods=["POST"])
def admin_host_curation_apply() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit"))
    if limit is not None:
        limit = max(1, min(limit, 10000))
    rank_scope = request.form.get("rank_scope") or "genus"
    summary = queue_standardization_refresh_for_ready_taxa(limit=limit, dry_run=False, rank_scope=rank_scope)
    flash(
        f"Queued standardization refresh after host curation ({summary['rank_scope']}): "
        f"{summary['queued']} taxa queued, {summary['running']} already running, {summary['skipped']} skipped.",
        "success",
    )
    return redirect(url_for("admin_host_curation"))


@app.route("/admin/geography-curation")
def admin_geography_curation() -> str:
    require_admin()
    return render_template(
        "admin_geography_curation.html",
        geography_curation=build_geography_curation_dashboard(request.args),
        **admin_common_context("standardization"),
    )


@app.route("/admin/geography-curation/approve", methods=["POST"])
def admin_geography_curation_approve() -> Any:
    require_admin()
    try:
        save_geography_curation_rule(
            request.form.get("source_value") or "",
            request.form.get("suggested_value") or "",
            request.form.get("note") or "Reviewed from admin geography curation.",
        )
    except ValueError as exc:
        flash(str(exc), "error")
    else:
        flash("Geography curation rule approved.", "success")
    return redirect(url_for("admin_geography_curation", status=request.form.get("status") or "pending"))


@app.route("/admin/geography-curation/export.csv")
def admin_geography_curation_export() -> Any:
    require_admin()
    dashboard = build_geography_curation_dashboard(request.args)
    output = StringIO()
    fieldnames = ["count", "source_column", "source_value", "suggested_value", "live_value", "status", "decision", "note"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in dashboard["rows"]:
        writer.writerow(row)
    return app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=geography_curation_review.csv"},
    )


@app.route("/admin/geography-curation/apply", methods=["POST"])
def admin_geography_curation_apply() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit"))
    if limit is not None:
        limit = max(1, min(limit, 10000))
    summary = queue_standardization_refresh_for_ready_taxa(limit=limit, dry_run=False, rank_scope="genus")
    flash(
        f"Queued geography-aware standardization refresh: {summary['queued']} taxa queued, "
        f"{summary['running']} already running, {summary['skipped']} skipped.",
        "success",
    )
    return redirect(url_for("admin_geography_curation"))


@app.route("/admin/collection-date-curation")
def admin_collection_date_curation() -> str:
    require_admin()
    return render_template(
        "admin_collection_date_curation.html",
        collection_date_curation=build_collection_date_curation_dashboard(request.args),
        **admin_common_context("standardization"),
    )


@app.route("/admin/collection-date-curation/approve", methods=["POST"])
def admin_collection_date_curation_approve() -> Any:
    require_admin()
    try:
        save_collection_date_curation_rule(
            request.form.get("source_value") or "",
            request.form.get("suggested_value") or "",
            request.form.get("note") or "Reviewed from admin collection-date curation.",
        )
    except ValueError as exc:
        flash(str(exc), "error")
    else:
        flash("Collection-date curation rule approved.", "success")
    return redirect(url_for("admin_collection_date_curation", status=request.form.get("status") or "pending"))


@app.route("/admin/collection-date-curation/export.csv")
def admin_collection_date_curation_export() -> Any:
    require_admin()
    dashboard = build_collection_date_curation_dashboard(request.args)
    output = StringIO()
    fieldnames = ["count", "source_column", "source_value", "suggested_value", "live_value", "status", "decision", "note"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in dashboard["rows"]:
        writer.writerow(row)
    return app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=collection_date_curation_review.csv"},
    )


@app.route("/admin/collection-date-curation/apply", methods=["POST"])
def admin_collection_date_curation_apply() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit"))
    if limit is not None:
        limit = max(1, min(limit, 10000))
    summary = queue_standardization_refresh_for_ready_taxa(limit=limit, dry_run=False, rank_scope="genus")
    flash(
        f"Queued collection-date standardization refresh: {summary['queued']} taxa queued, "
        f"{summary['running']} already running, {summary['skipped']} skipped.",
        "success",
    )
    return redirect(url_for("admin_collection_date_curation"))


@app.route("/admin/jobs")
def admin_jobs() -> str:
    require_admin()
    query = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "all").strip()
    all_jobs = list_all_jobs()
    filtered_jobs = filter_admin_jobs(all_jobs, query, status_filter)
    return render_template(
        "admin_jobs.html",
        jobs=filtered_jobs,
        job_analytics=build_admin_job_analytics(all_jobs),
        filtered_job_analytics=build_admin_job_analytics(filtered_jobs),
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


@app.route("/admin/audit-log")
def admin_audit_log() -> str:
    require_admin()
    limit = parse_optional_int(request.args.get("limit")) or 200
    return render_template(
        "admin_audit_log.html",
        audit_events=list_audit_log(limit),
        audit_limit=max(1, min(limit, 1000)),
        **admin_common_context("audit"),
    )


@app.route("/admin/audit-log/bundle.zip")
def admin_audit_bundle() -> Any:
    require_admin()
    bundle = BytesIO()
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        audit_rows = [dict(row) for row in list_audit_log(1000)]
        audit_csv = StringIO()
        audit_fieldnames = [
            "id",
            "actor_user_id",
            "actor_username",
            "action",
            "target_type",
            "target_id",
            "request_path",
            "request_method",
            "ip_address",
            "user_agent",
            "metadata_json",
            "created_at",
        ]
        writer = csv.DictWriter(audit_csv, fieldnames=audit_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(audit_rows)
        archive.writestr("audit_log_latest_1000.csv", audit_csv.getvalue())

        jobs = list_all_jobs()
        analytics = build_admin_job_analytics(jobs)
        archive.writestr("job_analytics.json", json.dumps(analytics, indent=2, sort_keys=True))
        included_audit_files: list[str] = []
        for path, arcname in iter_admin_audit_bundle_files():
            archive.write(path, arcname)
            included_audit_files.append(arcname)

        summary = [
            "# FetchM Web Admin Audit Bundle",
            "",
            f"Generated at: {utc_now()}",
            f"App version: {APP_VERSION}",
            f"Audit events included: {len(audit_rows)}",
            f"Jobs represented: {analytics['total']}",
            f"Active jobs: {analytics['active']}",
            f"Completed jobs: {analytics['completed']}",
            f"Failed jobs: {analytics['failed']}",
            f"Standardization audit files included: {len(included_audit_files)}",
            "",
            "## Included standardization artifacts",
            "",
        ]
        if included_audit_files:
            summary.extend(f"- `{arcname}`" for arcname in included_audit_files)
        else:
            summary.append("- No standardization audit artifacts were found.")
        archive.writestr("README.md", "\n".join(summary) + "\n")
    bundle.seek(0)
    record_audit_event("admin.audit_bundle_download")
    return app.response_class(
        bundle.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=fetchm_admin_audit_bundle.zip"},
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


@app.route("/admin/dataset-pipeline/settings", methods=["POST"])
def admin_set_dataset_pipeline_settings() -> Any:
    require_admin()
    enabled = "1" if request.form.get("dataset_pipeline_discovery_schedule_enabled") == "1" else "0"
    auto_publish = "1" if request.form.get("dataset_pipeline_auto_publish_insights") == "1" else "0"
    try:
        schedule_hour = max(0, min(23, int(request.form.get("dataset_pipeline_schedule_hour_utc") or "18")))
    except ValueError:
        schedule_hour = 18
    try:
        interval_days = max(1, min(365, int(request.form.get("dataset_pipeline_discovery_interval_days") or "7")))
    except ValueError:
        interval_days = 7
    scope = (request.form.get("dataset_pipeline_scope") or "2").strip() or "2"
    set_setting("dataset_pipeline_enabled", enabled)
    set_setting("dataset_pipeline_discovery_schedule_enabled", enabled)
    set_setting("dataset_pipeline_discovery_interval_days", str(interval_days))
    set_setting("dataset_pipeline_auto_publish_insights", auto_publish)
    set_setting("dataset_pipeline_schedule_hour_utc", str(schedule_hour))
    set_setting("dataset_pipeline_scope", scope)
    for step_key, setting_key in DATASET_PIPELINE_SEQUENTIAL_KEYS.items():
        set_setting(setting_key, "1" if request.form.get(setting_key) == "1" else "0")
    record_audit_event(
        "admin.dataset_pipeline_settings",
        target_type="dataset_pipeline",
        target_id="settings",
        metadata={
            "discovery_schedule_enabled": enabled == "1",
            "discovery_interval_days": interval_days,
            "schedule_hour_utc": schedule_hour,
            "scope": scope,
        },
    )
    flash("Dataset update pipeline settings updated.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dataset-pipeline/run", methods=["POST"])
def admin_run_dataset_pipeline() -> Any:
    require_admin()
    user_id = int(g.current_user["id"]) if g.current_user else None
    start_step = normalize_dataset_pipeline_step(request.form.get("start_step"))
    if start_step == "replace":
        flash("Replacement cannot be started as a standalone shortcut. Enable it as the sequential step after standardization so live data only switches after the full update run completes.", "error")
        return redirect(url_for("admin_dashboard"))
    run_id, error = queue_dataset_update_pipeline_run("manual", user_id, start_step=start_step)
    if error:
        flash(error, "error")
    else:
        record_audit_event(
            "admin.dataset_pipeline_run",
            target_type="dataset_pipeline",
            target_id=run_id,
            metadata={"start_step": start_step},
        )
        flash(f"{dataset_pipeline_step_label(start_step)} queued: {run_id}. Sequentially enabled downstream steps will follow.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dataset-pipeline/restart-standardization", methods=["POST"])
def admin_restart_dataset_standardization() -> Any:
    require_admin()
    rank_scope = request.form.get("rank_scope") or "all"
    limit = parse_optional_int(request.form.get("limit"))
    if limit is not None:
        limit = max(1, min(limit, 100000))
    summary = queue_standardization_refresh_for_ready_taxa(
        limit=limit,
        dry_run=False,
        rank_scope=rank_scope,
        changed_since=None,
    )
    record_audit_event(
        "admin.dataset_pipeline_restart_standardization",
        target_type="dataset_pipeline",
        target_id="standardization",
        metadata=summary,
    )
    flash(
        f"Restarted metadata standardization ({summary['rank_scope']}): "
        f"{summary['queued']} queued, {summary['running']} already running, {summary['skipped']} skipped.",
        "success",
    )
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dataset-pipeline/cancel", methods=["POST"])
def admin_cancel_dataset_pipeline() -> Any:
    require_admin()
    run_id = (request.form.get("run_id") or "").strip()
    if not run_id:
        flash("No pipeline run selected.", "error")
        return redirect(url_for("admin_dashboard"))
    with get_sqlite_connection() as db:
        row = db.execute("SELECT * FROM dataset_update_pipeline_runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            flash("Pipeline run was not found.", "error")
            return redirect(url_for("admin_dashboard"))
        db.execute("UPDATE dataset_update_pipeline_runs SET status = 'cancelled', completed_at = ?, error = 'Cancelled by admin.' WHERE run_id = ?", (utc_now(), run_id))
        db.execute("UPDATE dataset_update_pipeline_steps SET status = 'skipped', completed_at = ?, blockers_json = '[]' WHERE run_id = ? AND status IN ('pending', 'waiting', 'running')", (utc_now(), run_id))
        db.execute("UPDATE dataset_versions SET status = 'failed', error = 'Staging update cancelled by admin.' WHERE version_id = ?", (row["dataset_version_id"],))
        db.commit()
    record_audit_event("admin.dataset_pipeline_cancel", target_type="dataset_pipeline", target_id=run_id)
    flash(f"Cancelled staging dataset update {run_id}. Public data was unchanged.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dataset-pipeline/promote", methods=["POST"])
def admin_promote_dataset_pipeline() -> Any:
    require_admin()
    version_id = (request.form.get("version_id") or "").strip()
    user_id = int(g.current_user["id"]) if g.current_user else None
    with get_sqlite_connection() as db:
        version = db.execute("SELECT * FROM dataset_versions WHERE version_id = ?", (version_id,)).fetchone()
        if version is None or str(version["status"]) != "verified":
            flash("Only verified staging versions can be promoted.", "error")
            return redirect(url_for("admin_dashboard"))
        previous = get_active_dataset_version_id(db)
        now = utc_now()
        snapshot_live_species_state(db, previous)
        snapshot_live_metadata_species_search(db, previous)
        promote_staged_species_state(db, version_id)
        promote_staged_metadata_species_search(db, version_id)
        db.execute("UPDATE dataset_versions SET status = 'archived', archived_at = ? WHERE version_id = ? AND status = 'live'", (now, previous))
        db.execute("UPDATE dataset_versions SET status = 'live', promoted_at = ?, promoted_by = ? WHERE version_id = ?", (now, user_id, version_id))
        set_setting("previous_dataset_version_id", previous, db)
        set_setting("active_dataset_version_id", version_id, db)
        if get_setting("dataset_pipeline_auto_publish_insights", "1", db) == "1":
            staging_latest_path = dataset_version_root(version_id) / "global_insights" / "latest.json"
            if staging_latest_path.exists():
                staging_latest = json.loads(staging_latest_path.read_text(encoding="utf-8"))
                staging_summary_path = Path(str(staging_latest.get("summary_path") or ""))
                staging_snapshot_dir = staging_summary_path.parent
                public_snapshot_dir = global_insights_root() / "snapshots" / staging_snapshot_dir.name
                if staging_snapshot_dir.exists():
                    public_snapshot_dir.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(staging_snapshot_dir, public_snapshot_dir, dirs_exist_ok=True)
                    public_latest = dict(staging_latest)
                    public_latest["summary_path"] = str(public_snapshot_dir / "summary.json")
                    global_insights_root().mkdir(parents=True, exist_ok=True)
                    global_insights_latest_path().write_text(json.dumps(public_latest, indent=2), encoding="utf-8")
        run = db.execute("SELECT run_id FROM dataset_update_pipeline_runs WHERE dataset_version_id = ? ORDER BY id DESC LIMIT 1", (version_id,)).fetchone()
        if run is not None:
            db.execute("UPDATE dataset_update_pipeline_runs SET status = 'promoted', completed_at = ? WHERE run_id = ?", (now, run["run_id"]))
            db.execute("UPDATE dataset_update_pipeline_steps SET status = 'completed', completed_at = ?, blockers_json = '[]' WHERE run_id = ? AND step_key = 'replace'", (now, run["run_id"]))
        db.commit()
    record_audit_event("admin.dataset_pipeline_promote", target_type="dataset_version", target_id=version_id)
    flash(f"Promoted dataset version {version_id}. Public dataset pointers now use the verified staging data.", "success")
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dataset-pipeline/rollback", methods=["POST"])
def admin_rollback_dataset_pipeline() -> Any:
    require_admin()
    with get_sqlite_connection() as db:
        previous = str(get_setting("previous_dataset_version_id", "", db) or "")
        current = get_active_dataset_version_id(db)
        if not previous:
            flash("No previous dataset version is available for rollback.", "error")
            return redirect(url_for("admin_dashboard"))
        now = utc_now()
        restore_live_species_snapshot(db, previous)
        restore_live_metadata_species_search_snapshot(db, previous)
        db.execute("UPDATE dataset_versions SET status = 'archived', archived_at = ? WHERE version_id = ?", (now, current))
        db.execute("UPDATE dataset_versions SET status = 'live', promoted_at = ? WHERE version_id = ?", (now, previous))
        set_setting("active_dataset_version_id", previous, db)
        set_setting("previous_dataset_version_id", current, db)
        db.commit()
    record_audit_event("admin.dataset_pipeline_rollback", target_type="dataset_version", target_id=previous)
    flash(f"Rolled back active dataset version to {previous}.", "success")
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


@app.route("/admin/metadata/refine-host-standardization", methods=["POST"])
def admin_refine_host_standardization() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit"))
    if limit is None:
        limit = 25
    limit = max(1, min(limit, 500))
    summary = refine_ready_host_standardization(limit)
    flash(
        f"Host refinement complete for {summary['refined']} taxa. "
        f"Skipped {summary['skipped']} already-refined taxa; {summary['failed']} failed.",
        "success" if summary["failed"] == 0 else "error",
    )
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/standardization-refresh", methods=["POST"])
def admin_queue_standardization_refresh() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit"))
    if limit is not None:
        limit = max(1, min(limit, 10000))
    dry_run = request.form.get("dry_run") == "1"
    rank_scope = request.form.get("rank_scope") or "genus"
    summary = queue_standardization_refresh_for_ready_taxa(limit=limit, dry_run=dry_run, rank_scope=rank_scope)
    if dry_run:
        flash(
            f"Current standardization dry run ({summary['rank_scope']}): "
            f"{summary['eligible']} ready taxa eligible, about {summary['estimated_rows']} rows in scope, "
            f"{summary['skipped']} skipped because files are missing.",
            "success",
        )
    else:
        flash(
            f"Queued current standardization refresh ({summary['rank_scope']}): "
            f"{summary['queued']} taxa queued, {summary['running']} already running, "
            f"{summary['skipped']} skipped because files are missing.",
            "success",
        )
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/global-insights/generate", methods=["POST"])
def admin_generate_global_insights() -> Any:
    require_admin()
    demo = request.form.get("demo") == "1"
    user_id = int(g.current_user["id"]) if g.current_user else None
    snapshot_id, blockers = queue_global_insights_generation(user_id, demo=demo)
    if blockers:
        flash("Global Insights was not queued because " + "; ".join(blockers) + ".", "error")
    else:
        flash(
            f"Queued {'demo ' if demo else ''}Global Insights snapshot {snapshot_id}.",
            "success",
        )
        record_audit_event(
            "admin.global_insights_generate",
            target_type="global_insights",
            target_id=snapshot_id,
            metadata={"demo": demo},
        )
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/backfill-assembly-features", methods=["POST"])
def admin_queue_assembly_feature_backfill() -> Any:
    require_admin()
    queued = 0
    skipped = 0
    for species in list_all_species():
        if species.status != "ready" or species.metadata_status != "ready":
            skipped += 1
            continue
        if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
            skipped += 1
            continue
        if taxon_has_assembly_feature_columns(species):
            skipped += 1
            continue
        request_assembly_feature_backfill(species)
        queued += 1
    flash(f"Queued assembly-feature backfill for {queued} taxa. Skipped {skipped} taxa already complete or not ready.", "success")
    return redirect(url_for("admin_metadata"))


@app.route("/admin/metadata/expand-species-catalog", methods=["POST"])
def admin_expand_species_catalog() -> Any:
    require_admin()
    limit = parse_optional_int(request.form.get("limit") if hasattr(request, "form") else None)
    summary = expand_species_catalog_from_genus_metadata(limit=limit)
    flash(
        "Expanded species catalog from genus metadata: "
        f"{summary['created']} created, {summary['updated']} updated, "
        f"{summary['skipped']} already ready, {summary['failed']} failed "
        f"from {summary['candidate_total']} canonical candidates.",
        "success" if summary["failed"] == 0 else "error",
    )
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
                species = require_public_species(int(taxon_id_raw))
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
            safe_name = safe_upload_name(uploaded.filename, expected_extension)
            input_path = uploads_dir / safe_name
            save_validated_upload(uploaded, input_path)
            filters["input_source"] = "upload"
        else:
            refresh_before_run = (request.form.get("refresh_before_run") or "") == "1"
            taxon_id_raw = (request.form.get("taxon_id") or "").strip()
            if not taxon_id_raw:
                flash("Select a taxon from the managed catalog or switch to TSV upload.", "error")
                return redirect(url_for("index"))
            try:
                species = require_public_species(int(taxon_id_raw))
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

        safe_name = safe_upload_name(uploaded.filename, expected_extension)
        input_path = uploads_dir / safe_name
        save_validated_upload(uploaded, input_path)
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=job_id,
        metadata={"mode": mode, "input_source": filters.get("input_source")},
    )
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
    parent_quality_job = None
    parent_quality_job_id = str(job.filters.get("parent_quality_job_id") or "").strip()
    if parent_quality_job_id:
        try:
            candidate_parent = load_job(parent_quality_job_id)
            if candidate_parent.owner_user_id == job.owner_user_id:
                parent_quality_job = candidate_parent
        except FileNotFoundError:
            parent_quality_job = None
    log_text = Path(job.log_path).read_text(encoding="utf-8") if Path(job.log_path).exists() else ""
    output_files = collect_output_files(Path(job.output_dir))
    grouped_output_summary = summarize_grouped_sequence_outputs(job, output_files)
    sequence_download_assets = summarize_sequence_download_assets(job, output_files)
    quality_check_assets = summarize_quality_check_assets(job, output_files)
    progress = summarize_job_progress(job, log_text)
    runtime_summary = job_runtime_summary(job)
    compact_qc_steps = compact_qc_progress(job, log_text, quality_check_assets)
    post_qc_filter_preview = summarize_post_qc_filter_preview(job, request.args)
    return render_template(
        "job_detail.html",
        job=job,
        log_text=log_text[-40000:],
        output_files=output_files,
        grouped_output_summary=grouped_output_summary,
        sequence_download_assets=sequence_download_assets,
        quality_check_assets=quality_check_assets,
        progress=progress,
        runtime_summary=runtime_summary,
        compact_qc_steps=compact_qc_steps,
        post_qc_filter_preview=post_qc_filter_preview,
        post_qc_filter_query=request.query_string.decode("utf-8"),
        quality_tool_status=quality_tool_status(),
        parent_quality_job=parent_quality_job,
    )


def render_taxon_metadata_section(species_id: int, section: str) -> str:
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
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
    record_audit_event(
        "metadata.view",
        target_type="species",
        target_id=str(species_id),
        metadata={
            "section": section,
            "taxon_name": species.species_name,
            "taxon_rank": species.taxon_rank,
            "authenticated": bool(g.current_user),
        },
    )
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
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Metadata download is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_metadata", species_id=species_id))
    clean_path = Path(species.metadata_clean_path)
    record_audit_event("download.metadata_csv", target_type="species", target_id=str(species_id))
    return send_from_directory(clean_path.parent, clean_path.name, as_attachment=True)


@app.route("/taxa/<int:species_id>/metadata/download-bundle")
def download_taxon_metadata_bundle(species_id: int):
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Metadata bundle is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_metadata", species_id=species_id))
    analysis = load_taxon_metadata_analysis(species)
    payload = build_analysis_bundle(species, analysis)
    record_audit_event("download.metadata_bundle", target_type="species", target_id=str(species_id))
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
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
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


@app.route("/taxa/<int:species_id>/quality-check")
def taxon_quality_check(species_id: int) -> str:
    user = g.current_user
    assert user is not None
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
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
        "taxon_quality_check.html",
        species=species,
        sequence_dashboard=sequence_dashboard,
        preview_columns=sequence_dashboard["preview_columns"],
        preview_rows=sequence_dashboard["preview_rows"],
        quality_modules=list_quality_modules(),
        quality_profiles=QUALITY_PROFILES,
        quality_tool_status=quality_tool_status(),
        external_module_keys=external_module_keys(),
        filter_query_string=request.query_string.decode("utf-8"),
    )


@app.route("/taxa/<int:species_id>/sequences/metadata.csv", methods=["POST"])
def download_taxon_sequence_metadata_subset(species_id: int):
    user = g.current_user
    assert user is not None
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
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
    record_audit_event(
        "download.sequence_metadata_subset",
        target_type="species",
        target_id=str(species_id),
        metadata={"rows": int(len(filtered_frame))},
    )
    return app.response_class(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/taxa/<int:species_id>/sequences/jobs", methods=["POST"])
def create_taxon_sequence_job(species_id: int) -> Any:
    user = g.current_user
    assert user is not None
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
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
        "filter_logic": sequence_dashboard["filter_logic"],
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=job_id,
        metadata={
            "mode": "seq",
            "input_source": "taxon_sequences",
            "matched_row_total": sequence_dashboard["matched_row_total"],
        },
    )
    notify_job_event(record, "submitted")
    flash(f"Sequence job {job_id} submitted for {sequence_dashboard['matched_row_total']:,} genomes.", "success")
    return redirect(url_for("job_detail", job_id=job_id))


@app.route("/taxa/<int:species_id>/quality-check/jobs", methods=["POST"])
@app.route("/taxa/<int:species_id>/sequences/quality-jobs", methods=["POST"])
def create_taxon_sequence_quality_job(species_id: int) -> Any:
    user = g.current_user
    assert user is not None
    try:
        species = require_public_species(species_id)
    except FileNotFoundError:
        flash("That taxon is not ready in the managed catalog yet.", "error")
        return redirect(url_for("index"))
    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("taxon_quality_check", species_id=species_id, **request.form))
    if not species.metadata_clean_path or not Path(species.metadata_clean_path).exists():
        flash("Combined metadata is not ready for that taxon yet.", "error")
        return redirect(url_for("taxon_quality_check", species_id=species_id))

    sequence_dashboard = build_taxon_sequence_dashboard(species, request.form)
    filtered_frame = sequence_dashboard["filtered_frame"]
    if filtered_frame.empty:
        flash("The current filters do not match any genomes.", "error")
        return redirect(url_for("taxon_quality_check", species_id=species_id, **request.form))

    job_id = uuid.uuid4().hex[:12]
    root = job_dir(job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    input_name = f"{species.species_name} quality-check metadata.csv"
    input_path = uploads_dir / f"{species.slug}_quality_subset.csv"
    filtered_frame.to_csv(input_path, index=False)

    quality_config = build_quality_config(request.form)
    if (quality_config.get("profile") or {}).get("key") not in {"quick", "standard"}:
        flash("Choose Quick QC or Standard QC before submitting the first quality-check stage.", "error")
        return redirect(url_for("taxon_quality_check", species_id=species_id, **request.form))
    resource_blockers = quality_submission_blockers()
    for blocker in resource_blockers:
        flash(blocker, "error")
    if resource_blockers:
        return redirect(url_for("taxon_quality_check", species_id=species_id, **request.form))
    tool_status = quality_tool_status()
    runtime_errors = validate_quality_runtime(quality_config, tool_status)
    if runtime_errors:
        for error in runtime_errors:
            flash(error, "error")
        return redirect(url_for("taxon_quality_check", species_id=species_id, **request.form))
    thresholds = quality_config["thresholds"]
    command = build_quality_display_command(input_path, outputs_dir, quality_config)
    retries = (request.form.get("retries") or "3").strip()
    retry_delay = (request.form.get("retry_delay") or "5").strip()
    filters = {
        "input_source": "taxon_quality_check",
        "taxon_id": species.id,
        "taxon_name": species.species_name,
        "taxon_rank": species.taxon_rank,
        "matched_row_total": sequence_dashboard["matched_row_total"],
        "match_percent": sequence_dashboard["match_percent"],
        "filter_logic": sequence_dashboard["filter_logic"],
        "sequence_filter_sentence": sequence_dashboard["filter_sentence"],
        "selected_filters": sequence_dashboard["filters"],
        "quality_config": quality_config,
        "quality_thresholds": thresholds,
        "retries": retries,
        "retry_delay": retry_delay,
    }
    command.extend(["--retries", retries, "--retry-delay", retry_delay])
    record = JobRecord(
        id=job_id,
        mode="qc",
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=job_id,
        metadata={
            "mode": "qc",
            "input_source": "taxon_quality_check",
            "matched_row_total": sequence_dashboard["matched_row_total"],
            "quality_profile": quality_config.get("profile", {}).get("label"),
            "quality_run_mode": quality_config.get("run_mode"),
            "qc_filter_mode": quality_config.get("qc_filter_mode"),
            "taxonomy_match_rank": quality_config.get("taxonomy_match_rank"),
            "selected_modules": quality_config.get("selected_modules"),
            "quality_thresholds": thresholds,
        },
    )
    notify_job_event(record, "submitted")
    flash(f"Quality-check job {job_id} submitted for {sequence_dashboard['matched_row_total']:,} genomes.", "success")
    return redirect(url_for("job_detail", job_id=job_id))


@app.route("/jobs/<job_id>/quality-passed-sequence-job", methods=["POST"])
def create_quality_passed_sequence_job(job_id: str) -> Any:
    user = g.current_user
    assert user is not None
    quality_job = require_job_owner(job_id)
    if quality_job.mode != "qc":
        abort(404)
    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    pass_metadata = Path(quality_job.output_dir) / "sequence_qc" / "qc_pass_metadata.csv"
    if not pass_metadata.exists():
        flash("QC-passed metadata is not available yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    try:
        pass_frame = pd.read_csv(pass_metadata).fillna("")
    except Exception as exc:
        flash(f"Could not read QC-passed metadata: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    if pass_frame.empty:
        flash("No genomes passed the selected quality checks.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    new_job_id = uuid.uuid4().hex[:12]
    root = job_dir(new_job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    input_name = f"{quality_job.input_name} QC-passed metadata.csv"
    input_path = uploads_dir / "qc_pass_metadata.csv"
    pass_frame.to_csv(input_path, index=False)

    class DefaultSequenceForm:
        def get(self, key: str, default: Any = None) -> Any:
            defaults = {"retries": "3", "retry_delay": "5"}
            return defaults.get(key, default)

        def getlist(self, key: str) -> list[Any]:
            return []

    command, command_filters = build_command("seq", input_path, outputs_dir, DefaultSequenceForm())
    filters = {
        "input_source": "quality_passed_metadata",
        "parent_quality_job_id": quality_job.id,
        "taxon_id": (quality_job.filters or {}).get("taxon_id"),
        "taxon_name": (quality_job.filters or {}).get("taxon_name"),
        "taxon_rank": (quality_job.filters or {}).get("taxon_rank"),
        "matched_row_total": int(len(pass_frame)),
        "sequence_filter_sentence": f"QC-passed genomes from quality job {quality_job.id}",
    }
    filters.update(command_filters)
    record = JobRecord(
        id=new_job_id,
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=new_job_id,
        metadata={
            "mode": "seq",
            "input_source": "quality_passed_metadata",
            "parent_quality_job_id": quality_job.id,
            "matched_row_total": int(len(pass_frame)),
        },
    )
    notify_job_event(record, "submitted")
    flash(f"Sequence job {new_job_id} submitted for {len(pass_frame):,} QC-passed genomes.", "success")
    return redirect(url_for("job_detail", job_id=new_job_id))


@app.route("/jobs/<job_id>/quality-filtered-sequence-job", methods=["POST"])
def create_quality_filtered_sequence_job(job_id: str) -> Any:
    user = g.current_user
    assert user is not None
    quality_job = require_job_owner(job_id)
    if quality_job.mode != "qc":
        abort(404)
    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    try:
        frame = load_quality_filter_frame(quality_job)
    except FileNotFoundError:
        flash("QC metadata is not available yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    except Exception as exc:
        flash(f"Could not read QC metadata: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    if frame.empty:
        flash("QC metadata is empty.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    filtered, applied_filters = apply_quality_post_filters(frame, request.form)

    if filtered.empty:
        flash("No genomes match the selected post-QC filters.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    new_job_id = uuid.uuid4().hex[:12]
    root = job_dir(new_job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    input_name = f"{quality_job.input_name} QC-filtered metadata.csv"
    input_path = uploads_dir / "qc_filtered_metadata.csv"
    filtered.to_csv(input_path, index=False)

    class DefaultSequenceForm:
        def get(self, key: str, default: Any = None) -> Any:
            defaults = {"retries": "3", "retry_delay": "5"}
            return defaults.get(key, default)

        def getlist(self, key: str) -> list[Any]:
            return []

    command, command_filters = build_command("seq", input_path, outputs_dir, DefaultSequenceForm())
    filters = {
        "input_source": "quality_filtered_metadata",
        "parent_quality_job_id": quality_job.id,
        "taxon_id": (quality_job.filters or {}).get("taxon_id"),
        "taxon_name": (quality_job.filters or {}).get("taxon_name"),
        "taxon_rank": (quality_job.filters or {}).get("taxon_rank"),
        "matched_row_total": int(len(filtered)),
        "sequence_filter_sentence": f"Post-QC filtered genomes from quality job {quality_job.id}",
        "post_qc_filters": applied_filters,
    }
    filters.update(command_filters)
    record = JobRecord(
        id=new_job_id,
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=new_job_id,
        metadata={
            "mode": "seq",
            "input_source": "quality_filtered_metadata",
            "parent_quality_job_id": quality_job.id,
            "matched_row_total": int(len(filtered)),
            "post_qc_filters": applied_filters,
        },
    )
    notify_job_event(record, "submitted")
    flash(f"Sequence job {new_job_id} submitted for {len(filtered):,} post-QC filtered genomes.", "success")
    return redirect(url_for("job_detail", job_id=new_job_id))


@app.route("/jobs/<job_id>/advanced-quality-job", methods=["POST"])
def create_advanced_quality_job(job_id: str) -> Any:
    user = g.current_user
    assert user is not None
    quality_job = require_job_owner(job_id)
    if quality_job.mode != "qc":
        abort(404)
    if count_active_jobs_for_user(int(user["id"])) >= 1:
        flash("You already have an active job. Wait for it to finish before submitting another.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    try:
        frame = load_quality_filter_frame(quality_job)
    except FileNotFoundError:
        flash("QC metadata is not available yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    except Exception as exc:
        flash(f"Could not read QC metadata: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    if frame.empty:
        flash("QC metadata is empty.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    filtered, applied_filters = apply_quality_post_filters(frame, request.form)
    if filtered.empty:
        flash("No genomes match the selected post-QC filters.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    requested_modules = {"quick_fasta", "ani"}
    if request.form.get("advanced_module_mash"):
        requested_modules.add("mash")
    if request.form.get("advanced_module_gtdbtk"):
        requested_modules.add("gtdbtk")

    class AdvancedQualityForm:
        values = {
            "quality_profile": "advanced",
            "quality_run_mode": "nextflow",
            "qc_min_ani_percent": request.form.get("advanced_min_ani_percent") or "95",
            "qc_max_mash_distance": request.form.get("advanced_max_mash_distance") or "",
            "taxonomy_match_rank": request.form.get("taxonomy_match_rank") or "genus",
        }

        def get(self, key: str, default: Any = None) -> Any:
            return self.values.get(key, default)

        def getlist(self, key: str) -> list[Any]:
            if key == "quality_module":
                return sorted(requested_modules)
            return []

    quality_config = build_quality_config(AdvancedQualityForm())
    quality_config["advanced_qc"] = True
    quality_config["parent_quality_job_id"] = quality_job.id
    quality_config["decision_mode"] = "pass_fail"
    resource_blockers = quality_submission_blockers()
    for blocker in resource_blockers:
        flash(blocker, "error")
    if resource_blockers:
        return redirect(url_for("job_detail", job_id=job_id))
    tool_status = quality_tool_status()
    runtime_errors = validate_quality_runtime(quality_config, tool_status)
    if runtime_errors:
        for error in runtime_errors:
            flash(error, "error")
        return redirect(url_for("job_detail", job_id=job_id, **request.form))

    new_job_id = uuid.uuid4().hex[:12]
    root = job_dir(new_job_id)
    uploads_dir = root / UPLOADS_DIR_NAME
    outputs_dir = root / OUTPUTS_DIR_NAME
    uploads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    input_name = f"{quality_job.input_name} advanced-QC metadata.csv"
    input_path = uploads_dir / "advanced_qc_metadata.csv"
    filtered.to_csv(input_path, index=False)

    thresholds = quality_config["thresholds"]
    command = build_quality_display_command(input_path, outputs_dir, quality_config)
    retries = (request.form.get("retries") or "3").strip()
    retry_delay = (request.form.get("retry_delay") or "5").strip()
    command.extend(["--retries", retries, "--retry-delay", retry_delay])
    filters = {
        "input_source": "advanced_quality_filtered_metadata",
        "parent_quality_job_id": quality_job.id,
        "advanced_qc": True,
        "taxon_id": (quality_job.filters or {}).get("taxon_id"),
        "taxon_name": (quality_job.filters or {}).get("taxon_name"),
        "taxon_rank": (quality_job.filters or {}).get("taxon_rank"),
        "matched_row_total": int(len(filtered)),
        "sequence_filter_sentence": f"Advanced QC subset from quality job {quality_job.id}",
        "post_qc_filters": applied_filters,
        "quality_config": quality_config,
        "quality_thresholds": thresholds,
        "retries": retries,
        "retry_delay": retry_delay,
    }
    record = JobRecord(
        id=new_job_id,
        mode="qc",
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
    record_audit_event(
        "job.created",
        target_type="job",
        target_id=new_job_id,
        metadata={
            "mode": "qc",
            "input_source": "advanced_quality_filtered_metadata",
            "parent_quality_job_id": quality_job.id,
            "matched_row_total": int(len(filtered)),
            "selected_modules": quality_config.get("selected_modules"),
            "post_qc_filters": applied_filters,
        },
    )
    notify_job_event(record, "submitted")
    flash(f"Advanced QC job {new_job_id} submitted for {len(filtered):,} filtered genomes.", "success")
    return redirect(url_for("job_detail", job_id=new_job_id))


@app.route("/jobs/<job_id>/quality-filtered-metadata.csv")
def download_quality_filtered_metadata(job_id: str) -> Any:
    quality_job = require_job_owner(job_id)
    if quality_job.mode != "qc":
        abort(404)
    try:
        frame = load_quality_filter_frame(quality_job)
    except FileNotFoundError:
        flash("QC metadata is not available yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    except Exception as exc:
        flash(f"Could not read QC metadata: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    filtered, applied_filters = apply_quality_post_filters(frame, request.args)
    if filtered.empty:
        flash("No genomes match the selected post-QC filters.", "error")
        return redirect(url_for("job_detail", job_id=job_id, **request.args))
    record_audit_event(
        "download.quality_filtered_metadata",
        target_type="job",
        target_id=job_id,
        metadata={"rows": int(len(filtered)), "post_qc_filters": applied_filters},
    )
    payload = filtered.to_csv(index=False)
    return app.response_class(
        payload,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={job_id}_post_qc_filtered_metadata.csv"},
    )


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
    removed_files, error = remove_job_directory(job.id)
    if not removed_files:
        flash(f"Could not delete stored files for job {job.id}: {error}", "error")
        return redirect(url_for("admin_jobs"))
    get_db().execute("DELETE FROM jobs WHERE id = ?", (job.id,))
    get_db().commit()
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
    try:
        result = cleanup_old_jobs(older_than_days=older_than_days)
    except Exception as exc:
        logging.exception("Admin job cleanup failed")
        flash(f"Job cleanup failed before completion: {exc}", "error")
        return redirect(url_for("admin_jobs"))
    flash(f"Removed {result['removed']} old job records/directories older than {older_than_days} days.", "success")
    if result["skipped"]:
        flash(
            f"Skipped {result['skipped']} job directories because stored files could not be removed. "
            "Check filesystem ownership on the mounted data directory.",
            "error",
        )
    return redirect(url_for("admin_jobs"))


@app.route("/jobs/<job_id>/files/<path:relative_path>")
def download_output(job_id: str, relative_path: str) -> Any:
    job = require_job_owner(job_id)
    output_root = Path(job.output_dir).resolve()
    target = (output_root / relative_path).resolve()
    if output_root not in target.parents and target != output_root:
        abort(404)
    return send_from_directory(output_root, relative_path, as_attachment=True)


@app.route("/jobs/<job_id>/quality-summary")
def open_quality_summary(job_id: str) -> Any:
    job = require_job_owner(job_id)
    if job.mode != "qc":
        abort(404)
    output_root = Path(job.output_dir).resolve()
    relative_path = "sequence_qc/quality_check_summary.html"
    qc_dir = output_root / "sequence_qc"
    summary_path = qc_dir / "quality_check_summary.json"
    decisions_path = qc_dir / "qc_decisions.csv"
    if summary_path.exists() and decisions_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            decisions = pd.read_csv(decisions_path, dtype=str).fillna("")
            write_quality_check_html_summary(qc_dir, job=job, summary=summary, decisions=decisions)
        except (OSError, json.JSONDecodeError, pd.errors.EmptyDataError, ValueError):
            pass
    target = (output_root / relative_path).resolve()
    if not target.exists() or (output_root not in target.parents and target != output_root):
        flash("QC HTML summary is not available yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))
    return send_from_directory(output_root, relative_path, as_attachment=False)


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
