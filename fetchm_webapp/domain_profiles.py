"""Versioned domain-profile contracts for FetchM canonical pipelines.

The public bacterial workflow remains the default. Hidden domains use these
contracts to keep shared infrastructure separate from biological semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DomainProfile:
    key: str
    label: str
    short_label: str
    root_taxon_id: int
    root_name: str
    profile: str
    source_database: str = "genbank"
    accession_namespace: str = "GCA"
    visibility: str = "admin_hidden"
    public_enabled: bool = False
    release_locked: bool = True
    canonical_entities: tuple[str, ...] = field(default_factory=tuple)
    primary_record_model: str = "assembly"
    source_adapters: tuple[str, ...] = field(default_factory=tuple)
    standardization_axes: tuple[str, ...] = field(default_factory=tuple)
    qa_gates: tuple[str, ...] = field(default_factory=tuple)
    ui_facets: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)

    def store_config(self) -> dict[str, Any]:
        return {
            "domain_key": self.key,
            "label": self.label,
            "root_taxon_id": self.root_taxon_id,
            "source_database": self.source_database,
            "canonical_accession_namespace": self.accession_namespace,
            "profile": self.profile,
            "release_status": "locked_admin_hidden" if self.release_locked else "release_candidate",
            "visibility": self.visibility,
            "record_model": self.primary_record_model,
            "canonical_entities": list(self.canonical_entities),
            "public_enabled": self.public_enabled,
            "release_locked": self.release_locked,
        }

    def public_config(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "short_label": self.short_label,
            "root_taxon_id": str(self.root_taxon_id),
            "root_name": self.root_name,
            "assembly_namespace": self.accession_namespace,
            "source_database": self.source_database.title(),
            "public_status": "active" if self.public_enabled else "hidden",
            "admin_status": "public" if self.public_enabled else "background_prep",
            "status_label": "Active" if self.public_enabled else "Hidden prep",
            "enabled": self.public_enabled,
            "public_enabled": self.public_enabled,
            "canonical_backend_ready": self.key == "bacteria",
            "inventory_backend_ready": self.key != "bacteria",
            "profile": self.profile,
            "record_model": self.primary_record_model,
        }


DOMAIN_PROFILES: dict[str, DomainProfile] = {
    "bacteria": DomainProfile(
        key="bacteria",
        label="Bacteria",
        short_label="Bacterial",
        root_taxon_id=2,
        root_name="Bacteria",
        profile="bacteria_public_v1",
        visibility="public",
        public_enabled=True,
        release_locked=False,
        canonical_entities=("prokaryote_assembly", "biosample", "standardized_metadata_payload"),
        primary_record_model="prokaryote_assembly",
        source_adapters=("ncbi_datasets_genome_report",),
        standardization_axes=("host", "source", "sample", "environment", "geography", "collection_date"),
        qa_gates=("host_qa", "source_environment_qa", "geography_date_qa", "global_insights_qa"),
        ui_facets=("assembly_level", "host", "source", "country", "collection_year"),
    ),
    "archaea": DomainProfile(
        key="archaea",
        label="Archaea",
        short_label="Archaeal",
        root_taxon_id=2157,
        root_name="Archaea",
        profile="archaea_hidden_v1",
        canonical_entities=("prokaryote_assembly", "biosample", "standardized_metadata_payload"),
        primary_record_model="prokaryote_assembly",
        source_adapters=("ncbi_datasets_genome_report",),
        standardization_axes=("prokaryote_source", "geography", "collection_date", "environment"),
        qa_gates=("hidden_domain_profile_qa", "metadata_coverage_qa", "release_lock_qa"),
        ui_facets=("assembly_level", "source", "environment", "country", "collection_year"),
        notes=("Uses shared prokaryote infrastructure with archaeal profile boundaries.",),
    ),
    "virus": DomainProfile(
        key="virus",
        label="Virus",
        short_label="Viral",
        root_taxon_id=10239,
        root_name="Viruses",
        profile="virus_hidden_v1",
        canonical_entities=(
            "common_record",
            "virus_sequence",
            "virus_genome_group",
            "biosample",
            "taxon_relationship",
            "standardized_metadata_payload",
        ),
        primary_record_model="virus_sequence_or_assembly_surrogate",
        source_adapters=("ncbi_datasets_genome_report", "ncbi_virus_sequence_report"),
        standardization_axes=(
            "virus_taxonomy",
            "sequence_accession",
            "genome_group",
            "segment",
            "molecule_type",
            "genome_completeness",
            "host_relationship",
            "lab_passage_context",
            "geography",
            "collection_date",
            "sample_source",
        ),
        qa_gates=(
            "hidden_domain_profile_qa",
            "virus_relationship_semantics_qa",
            "virus_segment_grouping_qa",
            "metadata_coverage_qa",
            "release_lock_qa",
        ),
        ui_facets=(
            "virus_taxonomy",
            "host_taxonomy",
            "host_domain",
            "molecule_type",
            "genome_completeness",
            "segment",
            "country",
            "collection_year",
        ),
        notes=(
            "Virus records remain viral even when host relationships target bacterial or archaeal taxa.",
            "Viral host, lab-host, predicted-host, and sampled-host relationships are modeled as relationships, not as record domains.",
            "Segmented genomes may require multiple sequence records grouped by virus_genome_group.",
        ),
    ),
}


def domain_profile(key: str | None) -> DomainProfile:
    normalized = str(key or "").strip().lower()
    try:
        return DOMAIN_PROFILES[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported FetchM domain profile: {key!r}") from exc


def hidden_domain_keys() -> tuple[str, ...]:
    return tuple(key for key, profile in DOMAIN_PROFILES.items() if not profile.public_enabled)


def hidden_domain_store_configs() -> dict[str, dict[str, Any]]:
    return {key: DOMAIN_PROFILES[key].store_config() for key in hidden_domain_keys()}


def domain_profile_contract(key: str | None) -> dict[str, Any]:
    profile = domain_profile(key)
    return {
        "key": profile.key,
        "label": profile.label,
        "root_taxon_id": profile.root_taxon_id,
        "profile": profile.profile,
        "visibility": profile.visibility,
        "public_enabled": profile.public_enabled,
        "release_locked": profile.release_locked,
        "canonical_entities": list(profile.canonical_entities),
        "primary_record_model": profile.primary_record_model,
        "source_adapters": list(profile.source_adapters),
        "standardization_axes": list(profile.standardization_axes),
        "qa_gates": list(profile.qa_gates),
        "ui_facets": list(profile.ui_facets),
        "notes": list(profile.notes),
    }
