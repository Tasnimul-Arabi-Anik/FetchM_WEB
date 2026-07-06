#!/usr/bin/env python3
"""QA gate for the admin-hidden Virus canonical model."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_production_store import connect
from domain_profiles import domain_profile_contract

ALLOWED_RELATIONSHIP_TYPES = {
    "natural_host",
    "infects",
    "propagated_in",
    "predicted_to_infect",
    "isolated_from_host",
    "associated_with_host",
}
ALLOWED_TARGET_DOMAINS = {"bacteria", "archaea", "eukaryota", "virus", "unknown"}
ALLOWED_CONFIDENCE = {"high", "moderate", "review_required", "low"}


def _scalar(row: Any, index: int, default: Any = None) -> Any:
    try:
        return row[index]
    except Exception:
        return default


def _check(checks: list[dict[str, Any]], key: str, passed: bool, detail: str, *, hard: bool = True) -> None:
    checks.append({"key": key, "status": "pass" if passed else ("fail" if hard else "warn"), "detail": detail, "hard": hard})


def collect_hidden_virus_qa(snapshot_id: str | None = None) -> dict[str, Any]:
    """Collect hidden Virus model QA metrics for a snapshot or all hidden Virus rows."""
    contract = domain_profile_contract("virus")
    snapshot_filter = "AND source_snapshot_id = %s" if snapshot_id else ""
    params: tuple[Any, ...] = (snapshot_id,) if snapshot_id else ()
    checks: list[dict[str, Any]] = []
    with connect() as connection:
        snapshot = None
        if snapshot_id:
            snapshot = connection.execute(
                """
                SELECT status, visibility, release_locked, root_unique_assemblies
                FROM domain_inventory_snapshot
                WHERE domain_key = 'virus' AND snapshot_id = %s
                """,
                (snapshot_id,),
            ).fetchone()
        sequence_counts = connection.execute(
            f"""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(sequence_accession, ''), '') = '') AS missing_accession,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(genome_group_id, ''), '') = '') AS missing_group,
                   COUNT(*) FILTER (WHERE assembly_accession LIKE 'GCA_%%' OR assembly_accession LIKE 'GCF_%%') AS assembly_surrogates,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(assembly_accession, ''), '') = '') AS sequence_records,
                   COUNT(DISTINCT genome_group_id) AS distinct_groups
            FROM domain_virus_sequence_record
            WHERE domain_key = 'virus' {snapshot_filter}
            """,
            params,
        ).fetchone()
        group_counts = connection.execute(
            f"""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(genome_group_id, ''), '') = '') AS missing_group_id,
                   COUNT(*) FILTER (WHERE COALESCE(segment_count, 0) = 0) AS zero_segment_count
            FROM domain_virus_genome_group
            WHERE domain_key = 'virus' {snapshot_filter}
            """,
            params,
        ).fetchone()
        mismatched_group_counts = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM domain_virus_genome_group AS g
            WHERE g.domain_key = 'virus' {snapshot_filter.replace('source_snapshot_id', 'g.source_snapshot_id')}
              AND COALESCE(g.segment_count, 0) <> COALESCE((
                    SELECT COUNT(*) FROM domain_virus_sequence_record AS r
                    WHERE r.domain_key = 'virus' AND r.genome_group_id = g.genome_group_id
                ), 0)
            """,
            params,
        ).fetchone()
        relationship_counts = connection.execute(
            f"""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE domain_key <> 'virus') AS wrong_domain,
                   COUNT(*) FILTER (WHERE subject_record_type <> 'virus_sequence') AS wrong_subject_type,
                   COUNT(*) FILTER (WHERE relationship_type <> ALL(%s)) AS invalid_relationship_type,
                   COUNT(*) FILTER (WHERE target_domain <> ALL(%s)) AS invalid_target_domain,
                   COUNT(*) FILTER (WHERE confidence <> ALL(%s)) AS invalid_confidence,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(target_taxon_name, ''), '') = '') AS missing_target_name,
                   COUNT(*) FILTER (WHERE COALESCE(NULLIF(raw_value, ''), '') = '') AS missing_raw_value
            FROM domain_taxon_relationship
            WHERE domain_key = 'virus' {snapshot_filter}
            """,
            (sorted(ALLOWED_RELATIONSHIP_TYPES), sorted(ALLOWED_TARGET_DOMAINS), sorted(ALLOWED_CONFIDENCE), *params),
        ).fetchone()
        missing_subject_relationships = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM domain_taxon_relationship AS rel
            LEFT JOIN domain_virus_sequence_record AS seq
              ON seq.domain_key = rel.domain_key AND seq.sequence_accession = rel.subject_accession
            WHERE rel.domain_key = 'virus' {snapshot_filter.replace('source_snapshot_id', 'rel.source_snapshot_id')}
              AND seq.sequence_accession IS NULL
            """,
            params,
        ).fetchone()
        relationship_type_rows = connection.execute(
            f"""
            SELECT relationship_type, COUNT(*)
            FROM domain_taxon_relationship
            WHERE domain_key = 'virus' {snapshot_filter}
            GROUP BY relationship_type ORDER BY COUNT(*) DESC, relationship_type
            """,
            params,
        ).fetchall()
        target_domain_rows = connection.execute(
            f"""
            SELECT target_domain, COUNT(*)
            FROM domain_taxon_relationship
            WHERE domain_key = 'virus' {snapshot_filter}
            GROUP BY target_domain ORDER BY COUNT(*) DESC, target_domain
            """,
            params,
        ).fetchall()
    total_sequences = int(_scalar(sequence_counts, 0, 0) or 0)
    missing_accession = int(_scalar(sequence_counts, 1, 0) or 0)
    missing_group = int(_scalar(sequence_counts, 2, 0) or 0)
    assembly_surrogates = int(_scalar(sequence_counts, 3, 0) or 0)
    sequence_records = int(_scalar(sequence_counts, 4, 0) or 0)
    distinct_groups = int(_scalar(sequence_counts, 5, 0) or 0)
    total_groups = int(_scalar(group_counts, 0, 0) or 0)
    missing_group_id = int(_scalar(group_counts, 1, 0) or 0)
    zero_segment_groups = int(_scalar(group_counts, 2, 0) or 0)
    mismatched_groups = int(_scalar(mismatched_group_counts, 0, 0) or 0)
    total_relationships = int(_scalar(relationship_counts, 0, 0) or 0)
    wrong_relationship_domain = int(_scalar(relationship_counts, 1, 0) or 0)
    wrong_subject_type = int(_scalar(relationship_counts, 2, 0) or 0)
    invalid_relationship_type = int(_scalar(relationship_counts, 3, 0) or 0)
    invalid_target_domain = int(_scalar(relationship_counts, 4, 0) or 0)
    invalid_confidence = int(_scalar(relationship_counts, 5, 0) or 0)
    missing_target_name = int(_scalar(relationship_counts, 6, 0) or 0)
    missing_raw_value = int(_scalar(relationship_counts, 7, 0) or 0)
    missing_subjects = int(_scalar(missing_subject_relationships, 0, 0) or 0)

    if snapshot_id and snapshot is not None:
        _check(checks, "snapshot_admin_hidden", str(_scalar(snapshot, 1, "")) == "admin_hidden", f"visibility={_scalar(snapshot, 1, '')}")
        _check(checks, "snapshot_release_locked", bool(_scalar(snapshot, 2, False)), f"release_locked={bool(_scalar(snapshot, 2, False))}")
    elif snapshot_id:
        _check(checks, "snapshot_available", False, f"no domain_inventory_snapshot row for {snapshot_id}", hard=False)

    _check(checks, "public_release_disabled", not contract["public_enabled"] and contract["release_locked"], "domain profile is hidden and release locked")
    _check(checks, "virus_sequences_nonempty", total_sequences > 0, f"virus_sequence_records={total_sequences:,}")
    _check(checks, "sequence_accessions_present", missing_accession == 0, f"missing_accession={missing_accession:,}")
    _check(checks, "sequence_groups_present", missing_group == 0, f"missing_group={missing_group:,}")
    _check(checks, "genome_groups_nonempty", total_groups > 0, f"virus_genome_groups={total_groups:,}")
    _check(checks, "group_ids_present", missing_group_id == 0, f"missing_group_id={missing_group_id:,}")
    _check(checks, "group_segment_counts_positive", zero_segment_groups == 0, f"zero_segment_count_groups={zero_segment_groups:,}")
    _check(checks, "group_segment_counts_match_sequences", mismatched_groups == 0, f"mismatched_groups={mismatched_groups:,}")
    _check(checks, "relationships_nonempty", total_relationships > 0, f"taxon_relationships={total_relationships:,}", hard=False)
    _check(checks, "relationships_virus_domain", wrong_relationship_domain == 0, f"wrong_domain={wrong_relationship_domain:,}")
    _check(checks, "relationships_subject_type", wrong_subject_type == 0, f"wrong_subject_type={wrong_subject_type:,}")
    _check(checks, "relationships_subjects_exist", missing_subjects == 0, f"missing_subject_sequences={missing_subjects:,}")
    _check(checks, "relationships_types_controlled", invalid_relationship_type == 0, f"invalid_relationship_type={invalid_relationship_type:,}")
    _check(checks, "relationships_target_domains_controlled", invalid_target_domain == 0, f"invalid_target_domain={invalid_target_domain:,}")
    _check(checks, "relationships_confidence_controlled", invalid_confidence == 0, f"invalid_confidence={invalid_confidence:,}")
    _check(checks, "relationships_target_names_present", missing_target_name == 0, f"missing_target_name={missing_target_name:,}")
    _check(checks, "relationships_raw_values_present", missing_raw_value == 0, f"missing_raw_value={missing_raw_value:,}")

    hard_failures = [check for check in checks if check["hard"] and check["status"] == "fail"]
    return {
        "domain_key": "virus",
        "snapshot_id": snapshot_id or "all_hidden_virus_rows",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass" if not hard_failures else "fail",
        "hard_failure_count": len(hard_failures),
        "public_enabled": False,
        "release_locked": True,
        "virus_sequence_records": total_sequences,
        "virus_assembly_surrogates": assembly_surrogates,
        "virus_sequence_accession_records": sequence_records,
        "virus_genome_groups": total_groups,
        "distinct_sequence_groups": distinct_groups,
        "taxon_relationships": total_relationships,
        "relationship_type_counts": {str(row[0]): int(row[1] or 0) for row in relationship_type_rows},
        "target_domain_counts": {str(row[0]): int(row[1] or 0) for row in target_domain_rows},
        "checks": checks,
        "hard_failures": hard_failures,
    }


def write_outputs(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "virus_qa_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    checks = "\n".join(f"- `{check['key']}`: {check['status']} - {check['detail']}" for check in summary["checks"])
    relationship_types = "\n".join(f"- {key}: {value:,}" for key, value in summary["relationship_type_counts"].items()) or "- none"
    target_domains = "\n".join(f"- {key}: {value:,}" for key, value in summary["target_domain_counts"].items()) or "- none"
    markdown = (
        "# Hidden Virus QA Summary\n\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Status: `{summary['status']}`\n"
        f"- Hard failures: {summary['hard_failure_count']}\n"
        f"- Virus sequence records: {summary['virus_sequence_records']:,}\n"
        f"- Virus genome groups: {summary['virus_genome_groups']:,}\n"
        f"- Taxon relationships: {summary['taxon_relationships']:,}\n"
        "- Public enabled: false\n"
        "- Release locked: true\n\n"
        "## Relationship Types\n\n"
        f"{relationship_types}\n\n"
        "## Target Domains\n\n"
        f"{target_domains}\n\n"
        "## Checks\n\n"
        f"{checks}\n"
    )
    (output_dir / "virus_qa_summary.md").write_text(markdown, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args(argv)
    summary = collect_hidden_virus_qa(args.snapshot_id)
    if args.output_dir:
        write_outputs(summary, Path(args.output_dir))
    print(json.dumps({
        "domain_key": summary["domain_key"],
        "snapshot_id": summary["snapshot_id"],
        "status": summary["status"],
        "hard_failure_count": summary["hard_failure_count"],
        "virus_sequence_records": summary["virus_sequence_records"],
        "virus_genome_groups": summary["virus_genome_groups"],
        "taxon_relationships": summary["taxon_relationships"],
    }, sort_keys=True))
    if args.fail_on_hard_errors and summary["hard_failure_count"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
