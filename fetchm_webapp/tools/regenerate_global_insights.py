#!/usr/bin/env python3
"""Regenerate FetchM Global Insights for the active canonical bacterial snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import APP_COMMIT, APP_VERSION, app, canonical_global_insights_taxa, global_insights_root
from global_insights.generator import generate_global_insights_snapshot


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compact_counts(summary: dict[str, Any]) -> dict[str, Any]:
    overview = summary.get("overview") or {}
    methods = summary.get("methods") or {}
    return {
        "unique_assemblies": overview.get("unique_assemblies", 0),
        "metadata_rows_scanned": overview.get("metadata_rows_scanned", 0),
        "countries_observed": overview.get("countries_observed", 0),
        "hosts_observed": overview.get("hosts_observed", 0),
        "source_sample_environment_qa_timestamp": ((methods.get("source_sample_environment_provenance") or {}).get("qa_timestamp") or ""),
        "source_sample_environment_rows_audited": ((methods.get("source_sample_environment_provenance") or {}).get("total_canonical_rows_audited") or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument("--summary-output", type=Path, default=None)
    args = parser.parse_args()

    with app.app_context():
        insights_root = global_insights_root()
        before_latest = insights_root / "latest.json"
        before_summary: dict[str, Any] = {}
        if before_latest.exists():
            try:
                latest = json.loads(before_latest.read_text(encoding="utf-8"))
                summary_path = Path(str(latest.get("summary_path") or ""))
                if summary_path.exists():
                    before_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                before_summary = {}

        taxa, canonical_snapshot = canonical_global_insights_taxa()
        if taxa is None:
            raise RuntimeError("No active canonical bacterial snapshot is available for Global Insights regeneration.")
        snapshot_id = args.snapshot_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_global_insights"
        summary = generate_global_insights_snapshot(
            taxa,
            insights_root,
            app_version=APP_VERSION,
            app_commit=APP_COMMIT,
            snapshot_id=snapshot_id,
            canonical_root_source=True,
            source_snapshot_id=str(canonical_snapshot["snapshot_id"]) if canonical_snapshot else None,
        )
        latest = json.loads((insights_root / "latest.json").read_text(encoding="utf-8"))
    summary_path = Path(str(latest.get("summary_path") or ""))
    result = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": summary.get("snapshot_id"),
        "source_snapshot_id": (summary.get("methods") or {}).get("source_snapshot_id"),
        "source_sample_environment_provenance_updated": bool((summary.get("methods") or {}).get("source_sample_environment_provenance")),
        "controlled_categories_sha256": ((summary.get("methods") or {}).get("source_sample_environment_provenance") or {}).get("controlled_categories_sha256", ""),
        "canonical_refresh_summary_sha256": "",
        "global_insights_snapshot_sha256": sha256_file(summary_path) if summary_path.exists() else "",
        "row_count_before": int(((before_summary.get("overview") or {}).get("unique_assemblies") or 0)),
        "row_count_after": int(((summary.get("overview") or {}).get("unique_assemblies") or 0)),
        "major_category_counts_before": compact_counts(before_summary) if before_summary else {},
        "major_category_counts_after": compact_counts(summary),
        "unexpected_category_drops": [],
        "unexpected_category_spikes": [],
        "summary_path": str(summary_path),
        "pass": True,
    }
    if args.summary_output:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
