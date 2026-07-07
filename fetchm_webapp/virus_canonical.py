"""Hidden Virus canonical extraction helpers.

These pure helpers define the viral semantics before production ingestion. They
accept flexible NCBI-style report dictionaries and return a small canonical
entity bundle that can later be persisted into hidden Virus tables.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

from domain_profiles import domain_profile

MISSING_TOKENS = {"", "na", "n/a", "none", "null", "not provided", "unknown", "missing", "absent"}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return "; ".join(clean_text(item) for item in value if clean_text(item))
    if isinstance(value, dict):
        for key in ("name", "label", "scientific_name", "tax_name", "taxname", "value", "text"):
            text = clean_text(value.get(key))
            if text:
                return text
        return ""
    text = re.sub(r"\s+", " ", str(value).strip())
    return "" if text.casefold() in MISSING_TOKENS else text


def clean_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        for key in ("tax_id", "taxid", "id"):
            parsed = clean_int(value.get(key))
            if parsed is not None:
                return parsed
        return None
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def _walk_values(payload: Any, target_key: str) -> Iterable[Any]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = str(key).replace("-", "_").casefold()
            if normalized == target_key:
                yield value
            yield from _walk_values(value, target_key)
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk_values(item, target_key)


def first_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        normalized = key.replace("-", "_").casefold()
        for value in _walk_values(payload, normalized):
            text = clean_text(value)
            if text:
                return value
    return None


def first_text(payload: dict[str, Any], *keys: str) -> str:
    return clean_text(first_value(payload, *keys))


def first_int(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = first_value(payload, key)
        parsed = clean_int(value)
        if parsed is not None:
            return parsed
    return None


def accession_from_report(report: dict[str, Any]) -> str:
    return first_text(
        report,
        "nuccore_accession",
        "nucleotide_accession",
        "sequence_accession",
        "accession",
        "genbank_accession",
        "assembly_accession",
        "current_accession",
    )


def assembly_accession_from_report(report: dict[str, Any]) -> str:
    accession = first_text(report, "assembly_accession", "current_accession", "accession")
    return accession if accession.startswith(("GCA_", "GCF_")) else ""


def infer_target_domain(name: str, tax_id: int | None = None) -> str:
    text = name.casefold()
    if tax_id in {2} or any(token in text for token in ("escherichia", "klebsiella", "salmonella", "bacter", "bacteria")):
        return "bacteria"
    if tax_id in {2157} or "archaea" in text or "archaeon" in text or text.startswith(("methano", "haloarch", "sulfolobus")):
        return "archaea"
    if any(token in text for token in ("homo sapiens", "human", "mouse", "plant", "avian", "chicken")):
        return "eukaryota"
    return "unknown"


def relationship_id(subject_accession: str, relationship_type: str, raw_value: str, source_field: str) -> str:
    digest = hashlib.sha256(f"{subject_accession}\0{relationship_type}\0{source_field}\0{raw_value}".encode("utf-8")).hexdigest()
    return f"virusrel_{digest[:24]}"


def _relationship_from_value(
    *,
    subject_accession: str,
    relationship_type: str,
    source_field: str,
    value: Any,
    evidence_type: str,
    confidence: str = "review_required",
) -> dict[str, Any] | None:
    raw_value = clean_text(value)
    if not raw_value:
        return None
    target_taxon_id = clean_int(value)
    target_taxon_name = clean_text(value)
    if isinstance(value, dict):
        target_taxon_name = clean_text(
            value.get("name")
            or value.get("scientific_name")
            or value.get("tax_name")
            or value.get("taxname")
            or value.get("label")
            or raw_value
        )
    return {
        "relationship_id": relationship_id(subject_accession, relationship_type, raw_value, source_field),
        "subject_record_domain": "virus",
        "subject_accession": subject_accession,
        "relationship_type": relationship_type,
        "target_taxon_id": target_taxon_id,
        "target_taxon_name": target_taxon_name or raw_value,
        "target_domain": infer_target_domain(target_taxon_name or raw_value, target_taxon_id),
        "evidence_type": evidence_type,
        "confidence": confidence,
        "source_field": source_field,
        "raw_value": raw_value,
        "normalized_value": target_taxon_name or raw_value,
    }


def extract_host_relationships(report: dict[str, Any], subject_accession: str) -> list[dict[str, Any]]:
    relationships: list[dict[str, Any]] = []
    seen: set[str] = set()
    candidates = [
        ("host", "natural_host", "submitted_host", "moderate"),
        ("virus_host", "natural_host", "submitted_host", "moderate"),
        ("natural_host", "natural_host", "submitted_host", "moderate"),
        ("lab_host", "propagated_in", "submitted_lab_host", "moderate"),
        ("laboratory_host", "propagated_in", "submitted_lab_host", "moderate"),
        ("predicted_host", "predicted_to_infect", "computed_prediction", "review_required"),
        ("isolation_host", "isolated_from_host", "submitted_source", "moderate"),
    ]
    for key, relationship_type, evidence_type, confidence in candidates:
        for value in _walk_values(report, key):
            relationship = _relationship_from_value(
                subject_accession=subject_accession,
                relationship_type=relationship_type,
                source_field=key,
                value=value,
                evidence_type=evidence_type,
                confidence=confidence,
            )
            if relationship and relationship["relationship_id"] not in seen:
                relationships.append(relationship)
                seen.add(relationship["relationship_id"])
    return relationships


def virus_genome_group_id(report: dict[str, Any], primary_accession: str) -> str:
    explicit = first_text(report, "genome_group_id", "isolate_id", "virus_genome_id")
    if explicit:
        return explicit
    biosample = first_text(report, "biosample_accession", "biosample")
    tax_id = first_int(report, "tax_id", "taxid")
    isolate = first_text(report, "isolate", "isolate_name", "strain")
    parts = [str(tax_id or ""), biosample, isolate]
    if any(parts):
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
        return f"virus_group_{digest}"
    return f"virus_group_{primary_accession}"


def virus_canonical_entities(report: dict[str, Any], *, snapshot_id: str = "", profile: str | None = None) -> dict[str, Any]:
    profile_contract = domain_profile("virus")
    profile_name = profile or profile_contract.profile
    primary_accession = accession_from_report(report)
    assembly_accession = assembly_accession_from_report(report)
    if not primary_accession:
        raise ValueError("Virus report does not contain a usable sequence or assembly accession.")
    record_model = "virus_assembly_surrogate" if primary_accession.startswith(("GCA_", "GCF_")) else "virus_sequence"
    tax_id = first_int(report, "tax_id", "taxid")
    organism_name = first_text(report, "organism_name", "taxname", "tax_name", "virus_name", "name")
    biosample_accession = first_text(report, "biosample_accession", "biosample")
    group_id = virus_genome_group_id(report, primary_accession)
    relationships = extract_host_relationships(report, primary_accession)
    sequence = {
        "record_domain": "virus",
        "record_model": record_model,
        "profile": profile_name,
        "primary_accession": primary_accession,
        "sequence_accession": "" if record_model == "virus_assembly_surrogate" else primary_accession,
        "assembly_accession": assembly_accession,
        "genome_group_id": group_id,
        "organism_name": organism_name,
        "tax_id": tax_id,
        "biosample_accession": biosample_accession,
        "molecule_type": first_text(report, "molecule_type", "mol_type"),
        "segment": first_text(report, "segment", "segment_name"),
        "genome_completeness": first_text(report, "genome_completeness", "completeness"),
        "isolate": first_text(report, "isolate", "isolate_name", "strain"),
        "source_snapshot_id": snapshot_id,
        "relationship_count": len(relationships),
    }
    return {
        "common_record": {
            "record_domain": "virus",
            "primary_accession": primary_accession,
            "source_database": "genbank",
            "organism_taxon_id": tax_id,
            "organism_name": organism_name,
            "biosample_accession": biosample_accession,
            "source_snapshot_id": snapshot_id,
            "schema_version": profile_name,
            "raw_metadata_pointer": primary_accession,
        },
        "virus_sequence": sequence,
        "virus_genome_group": {
            "genome_group_id": group_id,
            "representative_accession": primary_accession,
            "tax_id": tax_id,
            "organism_name": organism_name,
            "biosample_accession": biosample_accession,
            "segment": sequence["segment"],
            "source_snapshot_id": snapshot_id,
        },
        "host_relationships": relationships,
        "raw_payload": report,
    }


def virus_standardization_row_fields(report: dict[str, Any]) -> dict[str, Any]:
    entities = virus_canonical_entities(report, profile="virus_hidden_v1")
    sequence = entities["virus_sequence"]
    relationships = entities["host_relationships"]
    return {
        "FetchM_Virus_Record_Model": sequence["record_model"],
        "Virus_Primary_Accession": sequence["primary_accession"],
        "Virus_Sequence_Accession": sequence["sequence_accession"],
        "Virus_Assembly_Accession": sequence["assembly_accession"],
        "Virus_Genome_Group_ID": sequence["genome_group_id"],
        "Virus_Molecule_Type": sequence["molecule_type"],
        "Virus_Segment": sequence["segment"],
        "Virus_Genome_Completeness": sequence["genome_completeness"],
        "Virus_Isolate": sequence["isolate"],
        "Virus_Host_Relationship_Count": len(relationships),
        "Virus_Host_Relationships_JSON": json.dumps(relationships, sort_keys=True),
    }
