from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import sys
import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import app as fetchm_app
from app import (
    apply_pass_fail_decision_mode,
    apply_quality_post_filters,
    apply_sequence_download_subset,
    apply_sequence_filters,
    sequence_download_subset_metadata,
    broad_standardization_category,
    build_qc_decision_preview,
    build_quality_config,
    ensure_managed_metadata_schema,
    extract_country,
    extract_year_from_collection_text,
    import_nextflow_qc_outputs,
    dedupe_reason_text,
    run_sequence_quality_checks,
    should_expose_output_file,
    standardize_collection_year_value,
    standardize_host_metadata,
)
from external_tools.quality_check.runner import validate_quality_runtime
from global_insights.generator import generate_demo_snapshot, generate_global_insights_snapshot, latest_geography_collection_date_provenance, latest_host_standardization_provenance, latest_source_sample_environment_provenance, run_standardization_simulator, taxonomy_label_metadata as global_taxonomy_label_metadata
import dataset_production_store as production_store
from dataset_production_store import canonical_partition_from_organism_name, parse_taxonkit_taxonomy_lineages
from tools import seed_canonical_metadata_from_sqlite as canonical_seed_tool
from tools import fetch_domain_missing_metadata as domain_fetch_tool
from tools import import_host_review_decisions as host_review_importer


@contextmanager
def isolated_initialized_app_client():
    old_paths = (
        fetchm_app.DATA_DIR,
        fetchm_app.JOBS_DIR,
        fetchm_app.LOCKS_DIR,
        fetchm_app.SPECIES_DIR,
        fetchm_app.METADATA_DIR,
        fetchm_app.CANONICAL_METADATA_REPORTS_DIR,
        fetchm_app.DB_PATH,
    )
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        fetchm_app.DATA_DIR = root / "data"
        fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
        fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
        fetchm_app.SPECIES_DIR = fetchm_app.DATA_DIR / "species"
        fetchm_app.METADATA_DIR = fetchm_app.DATA_DIR / "metadata"
        fetchm_app.CANONICAL_METADATA_REPORTS_DIR = fetchm_app.DATA_DIR / "canonical_metadata_reports"
        fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
        try:
            with fetchm_app.app.app_context():
                fetchm_app.ensure_directories()
                fetchm_app.init_db()
            yield fetchm_app.app.test_client()
        finally:
            (
                fetchm_app.DATA_DIR,
                fetchm_app.JOBS_DIR,
                fetchm_app.LOCKS_DIR,
                fetchm_app.SPECIES_DIR,
                fetchm_app.METADATA_DIR,
                fetchm_app.CANONICAL_METADATA_REPORTS_DIR,
                fetchm_app.DB_PATH,
            ) = old_paths


class MetadataStandardizationRegressionTests(unittest.TestCase):
    def test_source_sample_environment_provenance_loader(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            qa_root = root / "data" / "source_sample_environment_qa"
            qa_dir = qa_root / "20260611"
            qa_dir.mkdir(parents=True)
            (qa_dir / "summary.json").write_text(json.dumps({
                "metrics": {
                    "total_rows_scanned": 100,
                    "isolation_source_sd_present_percent": 60,
                    "isolation_source_sd_broad_present_percent": 55,
                    "raw_present_isolation_source_standardization_percent": 90,
                    "raw_only_unresolved_isolation_source_rows": 5,
                    "suspicious_exact_source_unique_values": 3,
                    "non_approved_broad_rows": 0,
                },
                "provenance": {
                    "qa_timestamp": "2026-06-11T00:00:00+00:00",
                    "qa_commit": "test",
                    "controlled_categories_sha256": "controlled",
                    "approved_broad_categories_sha256": "broad",
                    "generated_artifacts": ["summary.json"],
                },
            }), encoding="utf-8")
            (qa_root / "latest.json").write_text(json.dumps({
                "summary": "20260611/summary.json",
            }), encoding="utf-8")
            provenance = latest_source_sample_environment_provenance(root)
            self.assertEqual(provenance["total_canonical_rows_audited"], 100)
            self.assertEqual(provenance["raw_present_standardization_percent"], 90)
            self.assertEqual(provenance["broad_vocabulary_leakage_rows"], 0)

    def test_geography_collection_date_provenance_loader(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            qa_root = root / "data" / "geography_collection_date_qa"
            qa_dir = qa_root / "20260610"
            qa_dir.mkdir(parents=True)
            (qa_dir / "summary.json").write_text(json.dumps({
                "metrics": {
                    "total_rows_scanned": 100,
                    "country_present_percent": 80,
                    "continent_present_percent": 80,
                    "subcontinent_present_percent": 80,
                    "collection_year_present_percent": 70,
                    "non_country_values_in_country_rows": 0,
                    "country_continent_mismatch_rows": 0,
                    "country_subcontinent_mismatch_rows": 0,
                    "invalid_collection_year_rows": 0,
                    "future_collection_year_rows": 0,
                    "impossible_collection_year_rows": 0,
                },
                "provenance": {
                    "qa_timestamp": "2026-06-10T00:00:00+00:00",
                    "qa_commit": "test",
                    "country_lookup_sha256": "country",
                    "collection_date_parser_sha256": "date",
                    "generated_artifacts": ["summary.json"],
                },
            }), encoding="utf-8")
            (qa_root / "latest.json").write_text(json.dumps({
                "summary": "20260610/summary.json",
            }), encoding="utf-8")
            provenance = latest_geography_collection_date_provenance(root)
            self.assertEqual(provenance["total_canonical_rows_audited"], 100)
            self.assertEqual(provenance["country_lookup_sha256"], "country")
            self.assertEqual(provenance["invalid_future_impossible_collection_year_rows"], 0)

    def test_country_aliases_and_false_positive_blocks(self) -> None:
        for value in ["USA", "U.S.A.", "United States of America"]:
            self.assertEqual(extract_country(value), "United States")
        for value in ["ground turkey", "Guinea pig", "Norway rat", "Aspergillus niger", "turkey meat"]:
            self.assertIsNone(extract_country(value))

    def test_collection_year_parser_accepts_valid_dates_and_rejects_false_dates(self) -> None:
        for value in ["2019-05-10", "May 2019", "2019"]:
            self.assertEqual(standardize_collection_year_value(value), "2019")
        for value in ["1899", "2099", "publication date 2019", "submitted 2019", "accession 2019", "protocol 2019"]:
            self.assertIsNone(standardize_collection_year_value(value))
        self.assertIsNone(extract_year_from_collection_text("published in 2019", require_context=True))
        self.assertIsNone(extract_year_from_collection_text("submitted in 2019", require_context=True))

    def test_host_provenance_loader_reads_latest_monitoring_summary(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            monitoring = root / "data" / "host_standardization_monitoring"
            snapshot_dir = monitoring / "snapshot"
            snapshot_dir.mkdir(parents=True)
            (snapshot_dir / "qa.json").write_text(json.dumps({
                "rule_row_counts": {"host_synonyms": 10},
                "host_rule_version": "sha256:test",
                "host_rule_commit": "b92a591",
                "microbial_leakage_count": 0,
                "generated_at": "2026-06-10T00:00:00+00:00",
                "validation_sample_filename": "host_validation_sample_600.csv",
                "validation_sample_sha256": "abc",
            }), encoding="utf-8")
            (monitoring / "latest.json").write_text(json.dumps({
                "summary": "snapshot/qa.json",
            }), encoding="utf-8")
            provenance = latest_host_standardization_provenance(root)
            self.assertEqual(provenance["host_rule_commit"], "b92a591")
            self.assertEqual(provenance["host_rule_version"], "sha256:test")
            self.assertEqual(provenance["host_synonyms_row_count"], 10)
            self.assertEqual(provenance["host_sd_microbial_leakage_count"], 0)
            self.assertEqual(provenance["validation_sample_sha256"], "abc")

    def test_non_taxonomic_host_labels_preserve_broad_context(self) -> None:
        for raw_host, expected_context in [
            ("invertebrate", "invertebrate"),
            ("invertebrates", "invertebrate"),
            ("mudfish", "fish"),
            ("algea", "algae"),
            ("algae", "algae"),
            ("lichen", "lichen"),
            ("lichens", "lichen"),
            ("crutose lichen", "lichen"),
            ("crustose lichen", "lichen"),
            ("foliose lichen", "lichen"),
            ("Pisces", "fish"),
            ("Zooplatnkon", "zooplankton"),
            ("pelagic zooplankton", "zooplankton"),
            ("bracket mold", "fungus"),
            ("green alga", "green algae"),
            ("Basunti mas", "fish"),
            ("Crasosstrea spp., Farfantepenaeus spp.", "marine invertebrate"),
        ]:
            standardized = fetchm_app.enrich_host_standardization(
                raw_host,
                {
                    "Host_SD": "",
                    "Host_TaxID": "",
                    "Host_SD_Method": "not_identifiable",
                    "Host_SD_Confidence": "none",
                },
            )
            self.assertEqual(standardized["Host_Context_SD"], expected_context)
            self.assertEqual(standardized["Host_Review_Status"], "not_identifiable")
            self.assertEqual(standardized["Host_TaxID"], "")

    def test_host_review_import_rejects_supplied_taxid_mismatch(self) -> None:
        rows = [{
            "final_is_approved": "TRUE",
            "rule_type": "exact_host",
            "final_host": "Cervus nippon",
            "final_taxid": "9872",
        }]
        completed = type("Completed", (), {
            "returncode": 0,
            "stdout": "Cervus nippon\t9863\n",
            "stderr": "",
        })()
        with patch.object(host_review_importer.subprocess, "run", return_value=completed):
            error = host_review_importer.resolve_and_validate_taxids(rows)
        self.assertEqual(error, "")
        self.assertIn("does not match", rows[0]["_taxonomy_error"])

    def test_exact_medium_rule_precedes_generic_broad_substring(self) -> None:
        key = "shore bird"
        previous = fetchm_app.HOST_BROAD_SYNONYMS.get(key)
        fetchm_app.HOST_BROAD_SYNONYMS[key] = ("Charadriiformes", "8906")
        standardized = fetchm_app.standardize_host_metadata(key)
        self.assertEqual(standardized["Host_SD"], "Charadriiformes")
        self.assertEqual(standardized["Host_TaxID"], "8906")
        if previous is None:
            fetchm_app.HOST_BROAD_SYNONYMS.pop(key, None)
        else:
            fetchm_app.HOST_BROAD_SYNONYMS[key] = previous

    def test_microbial_host_leakage_is_demoted_without_demoting_eukaryotic_algae(self) -> None:
        for raw_host, expected_host, expected_taxid in [
            ("Sargassum hemiphyllum", "Sargassum hemiphyllum", "127544"),
            ("red marine alga", "Rhodophyta", "2763"),
        ]:
            enriched = fetchm_app.enrich_host_standardization(
                raw_host,
                fetchm_app.standardize_host_metadata(raw_host),
            )
            self.assertEqual(enriched["Host_SD"], expected_host)
            self.assertEqual(enriched["Host_TaxID"], expected_taxid)
            self.assertEqual(enriched["Host_Superkingdom"], "Eukaryota")

        for raw_host in [
            "Microcystis aeruginosa",
            "Nostoc sp.",
            "Prochlorococcus",
            "Klebsiella pneumoniae",
            "Streptococcus agalactiae",
            "Campylobacter jejuni",
            "Salmonella enterica",
            "Salmonella enterica subsp. enterica serovar Typhi",
            "Streptococcus pneumoniae",
        ]:
            enriched = fetchm_app.enrich_host_standardization(
                raw_host,
                fetchm_app.standardize_host_metadata(raw_host),
            )
            self.assertEqual(enriched["Host_SD"], "")
            self.assertEqual(enriched["Host_TaxID"], "")
            self.assertEqual(enriched["Host_Review_Status"], "non_host_source")

    def test_explicit_microbial_allowlist_preserves_intentional_host(self) -> None:
        key = fetchm_app.normalize_standardization_lookup("Salmonella enterica")
        fetchm_app.HOST_MICROBIAL_ALLOWLIST_KEYS.add(key)
        try:
            enriched = fetchm_app.enrich_host_standardization(
                "Salmonella enterica",
                fetchm_app.standardize_host_metadata("Salmonella enterica"),
            )
            self.assertEqual(enriched["Host_SD"], "Salmonella enterica")
            self.assertEqual(enriched["Host_TaxID"], "28901")
            self.assertEqual(enriched["Host_Superkingdom"], "Bacteria")
        finally:
            fetchm_app.HOST_MICROBIAL_ALLOWLIST_KEYS.discard(key)

    def test_reviewed_context_only_labels_do_not_force_host_sd(self) -> None:
        for raw_host, expected_context in [
            ("algae", "algae"),
            ("algea", "algae"),
            ("fish", "fish"),
            ("Pisces", "fish"),
            ("Zooplatnkon", "zooplankton"),
            ("zooplankton", "zooplankton"),
            ("crutose lichen", "lichen"),
            ("crustose lichen", "lichen"),
            ("foliose lichen", "lichen"),
            ("pelagic zooplankton", "zooplankton"),
            ("green alga", "green algae"),
            ("marine invertebrate", "marine invertebrate"),
        ]:
            enriched = fetchm_app.enrich_host_standardization(
                raw_host,
                fetchm_app.standardize_host_metadata(raw_host),
            )
            self.assertEqual(enriched["Host_SD"], "")
            self.assertEqual(enriched["Host_TaxID"], "")
            self.assertEqual(enriched["Host_Context_SD"], expected_context)

    def test_reviewed_microbial_group_labels_do_not_force_host_sd(self) -> None:
        for raw_host in ["Archaea_Bacteria", "gram-positive bacteria"]:
            enriched = fetchm_app.enrich_host_standardization(
                raw_host,
                fetchm_app.standardize_host_metadata(raw_host),
            )
            self.assertEqual(enriched["Host_SD"], "")
            self.assertEqual(enriched["Host_TaxID"], "")
            self.assertEqual(enriched["Host_Review_Status"], "non_host_source")

    def test_reviewed_common_name_rule_overrides_broad_dictionary(self) -> None:
        key = "water lettuce"
        fetchm_app.HOST_SYNONYMS.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_CONFIDENCE.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_METHOD.pop(key, None)
        fetchm_app.apply_approved_standardization_rule_to_memory({
            "original_value": key,
            "destination": "Host_SD",
            "proposed_value": "Pistia stratiotes",
            "ontology_id": "4477",
            "method": "reviewed_common_name",
            "confidence": "high",
        })

        standardized = fetchm_app.standardize_host_metadata(key)
        enriched = fetchm_app.enrich_host_standardization(key, standardized)

        self.assertEqual(enriched["Host_SD"], "Pistia stratiotes")
        self.assertEqual(enriched["Host_TaxID"], "4477")
        self.assertEqual(enriched["Host_Match_Method"], "reviewed_common_name")
        self.assertEqual(enriched["Host_Confidence"], "high")
        self.assertEqual(enriched["Host_Review_Status"], "approved")
        fetchm_app.HOST_SYNONYMS.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_CONFIDENCE.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_METHOD.pop(key, None)

    def test_medium_confidence_manual_exact_rule_remains_exact(self) -> None:
        key = "reviewed exact medium host"
        fetchm_app.HOST_SYNONYMS.pop(key, None)
        fetchm_app.HOST_BROAD_SYNONYMS.pop(key, None)
        fetchm_app.apply_approved_standardization_rule_to_memory({
            "original_value": key,
            "destination": "Host_SD",
            "proposed_value": "Cervus nippon",
            "ontology_id": "9863",
            "method": "manual_host_curation",
            "confidence": "medium",
        })
        self.assertEqual(fetchm_app.HOST_SYNONYMS[key], ("Cervus nippon", "9863"))
        self.assertNotIn(key, fetchm_app.HOST_BROAD_SYNONYMS)
        standardized = fetchm_app.standardize_host_metadata(key)
        self.assertEqual(standardized["Host_SD_Confidence"], "medium")
        fetchm_app.HOST_SYNONYMS.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_CONFIDENCE.pop(key, None)
        fetchm_app.HOST_APPROVED_RULE_METHOD.pop(key, None)

    def test_host_curation_approval_overlay_avoids_full_cache_rebuild(self) -> None:
        rows = [
            {
                "raw_host": "human",
                "display_decision": "ambiguous",
                "live_status": "pending",
                "needs_review": "1",
            },
            {
                "raw_host": "soil",
                "display_decision": "ambiguous",
                "live_status": "pending",
                "needs_review": "1",
            },
        ]
        approved_rules = {
            "human": {
                "approved_by": "admin",
                "approved_at": "2026-06-09T00:00:00+00:00",
                "method": "manual_host_curation",
                "proposed_value": "Homo sapiens",
                "ontology_id": "9606",
                "confidence": "high",
            }
        }

        result = fetchm_app.apply_host_curation_approval_overlay(rows, approved_rules)

        self.assertTrue(result[0]["is_approved"])
        self.assertEqual(result[0]["live_status"], "resolved")
        self.assertEqual(result[0]["live_host"], "Homo sapiens")
        self.assertEqual(result[0]["live_taxid"], "9606")
        self.assertEqual(result[0]["needs_review"], "0")
        self.assertFalse(result[1]["is_approved"])
        self.assertEqual(result[1]["live_status"], "pending")

    def test_canonical_partition_classification_keeps_root_inventory_honest(self) -> None:
        canonical = canonical_partition_from_organism_name("Morganella morganii strain ABC")
        self.assertEqual(canonical["partition_type"], "named_species")
        self.assertEqual(canonical["species_label"], "Morganella morganii")
        provisional = canonical_partition_from_organism_name("Morganella sp. FDAARGOS_123")
        self.assertEqual(provisional["partition_type"], "provisional_species")
        self.assertEqual(provisional["genus_name"], "Morganella")
        candidatus = canonical_partition_from_organism_name("Candidatus Liberibacter asiaticus")
        self.assertEqual(candidatus["partition_type"], "provisional_species")
        self.assertEqual(candidatus["genus_name"], "Liberibacter")
        genus_only = canonical_partition_from_organism_name("Morganella")
        self.assertEqual(genus_only["partition_type"], "genus_only")
        uncultured_genus = canonical_partition_from_organism_name("uncultured Prevotella sp.")
        self.assertEqual(uncultured_genus["partition_type"], "provisional_species")
        self.assertEqual(uncultured_genus["genus_name"], "Prevotella")
        self.assertEqual(uncultured_genus["assignment_reason"], "recoverable_uncultured_genus_label")
        uncultured_family = canonical_partition_from_organism_name("uncultured Lachnospiraceae bacterium")
        self.assertEqual(uncultured_family["partition_type"], "unresolved_genus")
        high_rank = canonical_partition_from_organism_name("Pseudomonadota bacterium")
        self.assertEqual(high_rank["partition_type"], "unresolved_genus")
        self.assertEqual(high_rank["genus_name"], "")
        self.assertEqual(high_rank["assignment_reason"], "higher_rank_placeholder_label")
        genus_like = canonical_partition_from_organism_name("Listeria bacterium")
        self.assertEqual(genus_like["partition_type"], "provisional_species")
        self.assertEqual(genus_like["genus_name"], "Listeria")

    def test_canonical_admin_pipeline_exposes_staged_safe_workflow(self) -> None:
        root = {
            "configured": True,
            "available": True,
            "status": "completed",
            "snapshot_id": "canonical-root-test",
            "root_unique_assemblies": 3126169,
            "metadata_seed_status": "completed",
            "metadata_fetch_status": "completed",
            "metadata_restandardization_status": "not generated",
            "partition_status": "completed",
            "standardized_metadata_coverage": {
                "root_unique_assemblies": 3126169,
                "standardized_assemblies": 3126169,
                "missing_standardized_assemblies": 0,
            },
            "latest_reconciliation": {
                "root_unique_assemblies": 3126169,
                "accounted_unique_assemblies": 3126169,
            },
            "taxonomy_lineage_summary": {
                "distinct_rank_labels": {"species": 25000, "species_level_labels": 33000}
            },
        }
        gate = {
            "status": "ready",
            "snapshot_id": "canonical-root-test",
            "active_snapshot_id": "canonical-root-live",
            "can_activate": True,
            "root_unique_assemblies": 3126169,
            "standardized_assemblies": 3126169,
            "accounted_unique_assemblies": 3126169,
            "unresolved_genus_assemblies": 0,
        }
        cards = fetchm_app.build_canonical_pipeline_cards(root, gate, None)
        self.assertEqual([card["key"] for card in cards], [
            "inventory", "metadata_seed", "metadata_fetch", "metadata_restandardization",
            "partitions", "verify", "activate", "global_insights",
        ])
        standardization = cards[3]
        self.assertEqual(standardization["status"], "incremental ready")
        self.assertIn("Incremental", standardization["short"])
        self.assertEqual(cards[6]["endpoint"], "admin_activate_canonical_metadata_release")
        self.assertEqual(cards[7]["extra_fields"], {"next": "admin"})
        self.assertFalse(any(card["publish_control"] for card in cards))
        self.assertEqual(cards[0]["button"], "Update inventory")
        self.assertIn("Change from previous", cards[0]["details"][1])
        self.assertTrue(cards[2]["disabled"])
        self.assertEqual(cards[1]["label"], "Reuse cached metadata")
        root["metadata_fetch_task_active"] = True
        busy_cards = fetchm_app.build_canonical_pipeline_cards(root, gate, None)
        self.assertTrue(all(card["disabled"] for card in busy_cards))

    def test_canonical_pipeline_domain_options_include_hidden_archaea(self) -> None:
        self.assertEqual(fetchm_app.normalize_canonical_pipeline_domain(None), "bacteria")
        self.assertEqual(fetchm_app.normalize_canonical_pipeline_domain("Archaea"), "archaea")
        self.assertEqual(fetchm_app.normalize_canonical_pipeline_domain("unknown"), "bacteria")

        options = fetchm_app.canonical_pipeline_domain_options("archaea")
        by_key = {option["key"]: option for option in options}
        self.assertEqual(set(by_key), {"bacteria", "archaea"})
        self.assertTrue(by_key["archaea"]["selected"])
        self.assertFalse(by_key["archaea"]["public_enabled"])
        self.assertFalse(by_key["archaea"]["canonical_backend_ready"])
        self.assertTrue(by_key["bacteria"]["canonical_backend_ready"])

    def test_archaea_background_pipeline_preview_is_disabled(self) -> None:
        domain = fetchm_app.canonical_pipeline_domain_config("archaea")
        cards = fetchm_app.background_pipeline_preview_cards(domain)
        self.assertEqual([card["key"] for card in cards], ["inventory", "standardization_rules", "qa_gate"])
        self.assertTrue(all(card["disabled"] for card in cards))
        self.assertTrue(all(card["percent"] == 0 for card in cards))
        self.assertIn("TaxID 2157", cards[0]["details"][0])
        self.assertIn("bacterial tables are not reused", cards[0]["details"][1])

    def test_archaea_background_pipeline_preview_reports_hidden_inventory(self) -> None:
        domain = fetchm_app.canonical_pipeline_domain_config("archaea")
        cards = fetchm_app.background_pipeline_preview_cards(
            domain,
            {
                "available": True,
                "status": "completed",
                "root_unique_assemblies": 1234,
                "snapshot_id": "20260706T000000Z_genbank_archaea_root",
                "visibility": "admin_hidden",
                "release_locked": True,
                "standardized_metadata_coverage": {
                    "standardized_assemblies": 1000,
                    "missing_standardized_assemblies": 234,
                },
            },
        )
        self.assertEqual(cards[0]["status"], "completed")
        self.assertEqual(cards[0]["percent"], 100)
        self.assertIn("1,234", cards[0]["details"][0])
        self.assertIn("release locked: yes", cards[0]["details"][2])
        self.assertEqual(cards[1]["status"], "hidden partial")
        self.assertEqual(cards[1]["percent"], 81)
        self.assertIn("1,000 / 1,234", cards[1]["details"][0])
        self.assertTrue(all(card["disabled"] for card in cards))

    def test_hidden_domain_metadata_rows_are_tagged_admin_hidden(self) -> None:
        report = {
            "accession": "GCA_000000001.1",
            "organism": {"organism_name": "Methanocaldococcus jannaschii", "tax_id": 2190},
            "assembly_info": {
                "assembly_name": "ASM1",
                "assembly_level": "Complete Genome",
                "biosample": {
                    "accession": "SAMN00000001",
                    "host": "",
                    "isolation_source": "hot spring",
                    "attributes": [{"name": "geo_loc_name", "value": "USA"}],
                },
            },
        }
        row = domain_fetch_tool.standardizable_domain_row(report)
        self.assertEqual(row["Assembly Accession"], "GCA_000000001.1")
        self.assertEqual(row["FetchM_Domain"], "Archaea")
        self.assertEqual(row["FetchM_Domain_Key"], "archaea")
        self.assertEqual(row["FetchM_Domain_Profile"], "archaea_hidden_v1")
        self.assertEqual(row["FetchM_Public_Release_Status"], "locked_admin_hidden")
        self.assertEqual(row["Isolation Source"], "hot spring")

    def test_hidden_domain_taxon_labels_derive_genus_and_species(self) -> None:
        labels = production_store.domain_taxon_labels_for_organism("Methanocaldococcus jannaschii DSM 2661")
        self.assertIn({"rank": "genus", "name": "Methanocaldococcus"}, labels)
        self.assertIn({"rank": "species", "name": "Methanocaldococcus jannaschii"}, labels)

    def test_hidden_archaea_admin_routes_require_admin_and_render_results(self) -> None:
        with isolated_initialized_app_client() as client:
            response = client.get("/admin/archaea", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertIn("/login", response.headers.get("Location", ""))
            with fetchm_app.app.app_context():
                user = fetchm_app.create_user("archaea-admin", "archaea-admin@example.com", "long-password-1")
            with client.session_transaction() as session:
                session["user_id"] = int(user["id"])
            with patch.object(fetchm_app, "ADMIN_USERS", {"archaea-admin"}), patch(
                "dataset_production_store.domain_taxon_search_results",
                return_value=[{
                    "domain_key": "archaea",
                    "snapshot_id": "20260706T000000Z_genbank_archaea_root",
                    "rank": "genus",
                    "name": "Methanocaldococcus",
                    "genome_count": 12,
                    "public_enabled": False,
                    "release_locked": True,
                }],
            ):
                page = client.get("/admin/archaea?q=Methano")
            html = page.data.decode("utf-8")
            self.assertEqual(page.status_code, 200)
            self.assertIn("Hidden Archaea Metadata", html)
            self.assertIn("Methanocaldococcus", html)
            self.assertIn("release locked", html.lower())
            self.assertIn("/admin/archaea/genus/Methanocaldococcus", html)

    def test_hidden_archaea_admin_queue_routes_call_domain_tasks(self) -> None:
        with isolated_initialized_app_client() as client:
            with fetchm_app.app.app_context():
                user = fetchm_app.create_user("archaea-admin", "archaea-admin@example.com", "long-password-1")
            with client.session_transaction() as session:
                session["user_id"] = int(user["id"])
                session["_csrf_token"] = "token"
            with patch.object(fetchm_app, "ADMIN_USERS", {"archaea-admin"}), patch(
                "dataset_production_store.queue_domain_inventory_task", return_value=("20260706T000000Z_genbank_archaea_root", None)
            ) as queue_inventory:
                response = client.post(
                    "/admin/archaea/pipeline/inventory",
                    data={"_csrf_token": "token", "continue_after": "1"},
                    follow_redirects=False,
                )
            self.assertEqual(response.status_code, 302)
            queue_inventory.assert_called_once()
            self.assertEqual(queue_inventory.call_args.args[0], "archaea")
            self.assertTrue(queue_inventory.call_args.kwargs["continue_after"])

            with client.session_transaction() as session:
                session["_csrf_token"] = "token"
            with patch.object(fetchm_app, "ADMIN_USERS", {"archaea-admin"}), patch(
                "dataset_production_store.queue_domain_metadata_fetch_task", return_value=("20260706T000000Z_genbank_archaea_root", None)
            ) as queue_fetch:
                response = client.post(
                    "/admin/archaea/pipeline/metadata-fetch",
                    data={"_csrf_token": "token", "refetch_all": "1"},
                    follow_redirects=False,
                )
            self.assertEqual(response.status_code, 302)
            queue_fetch.assert_called_once()
            self.assertEqual(queue_fetch.call_args.args[0], "archaea")
            self.assertTrue(queue_fetch.call_args.kwargs["refetch_all"])

    def test_hidden_archaea_admin_report_renders_summary(self) -> None:
        with isolated_initialized_app_client() as client:
            with fetchm_app.app.app_context():
                user = fetchm_app.create_user("archaea-admin", "archaea-admin@example.com", "long-password-1")
            with client.session_transaction() as session:
                session["user_id"] = int(user["id"])
            report = {
                "domain_key": "archaea",
                "snapshot_id": "20260706T000000Z_genbank_archaea_root",
                "rank": "genus",
                "rank_label": "Genus",
                "name": "Methanocaldococcus",
                "row_count": 1,
                "public_enabled": False,
                "release_locked": True,
                "top_countries": [{"value": "USA", "count": 1}],
                "top_hosts": [],
                "top_isolation_sources": [{"value": "hot spring", "count": 1}],
                "top_sample_types": [],
                "top_environment_media": [{"value": "water", "count": 1}],
                "top_assembly_levels": [{"value": "Complete Genome", "count": 1}],
                "examples": [{
                    "assembly_accession": "GCA_000000001.1",
                    "organism_name": "Methanocaldococcus jannaschii",
                    "biosample_accession": "SAMN00000001",
                    "country": "USA",
                    "host": "",
                    "isolation_source": "hot spring",
                    "sample_type": "",
                    "environment_medium": "water",
                    "assembly_level": "Complete Genome",
                }],
            }
            with patch.object(fetchm_app, "ADMIN_USERS", {"archaea-admin"}), patch(
                "dataset_production_store.domain_taxon_report", return_value=report
            ):
                page = client.get("/admin/archaea/genus/Methanocaldococcus")
            html = page.data.decode("utf-8")
            self.assertEqual(page.status_code, 200)
            self.assertIn("Methanocaldococcus", html)
            self.assertIn("GCA_000000001.1", html)
            self.assertIn("hot spring", html)
            self.assertIn("Admin-only report", html)

    def test_hidden_domain_store_allowlists_archaea_only(self) -> None:
        self.assertEqual(production_store.normalize_domain_pipeline_key("Archaea"), "archaea")
        with self.assertRaises(ValueError):
            production_store.normalize_domain_pipeline_key("bacteria")
        with self.assertRaises(ValueError):
            production_store.normalize_domain_pipeline_key("virus")
        config = production_store.domain_pipeline_config("archaea")
        self.assertEqual(config["root_taxon_id"], production_store.ARCHAEA_TAXON_ID)
        self.assertFalse(config["public_enabled"])
        self.assertTrue(config["release_locked"])
        self.assertTrue(production_store.domain_inventory_api_url("archaea").endswith("/2157/dataset_report"))
        self.assertRegex(production_store.default_domain_snapshot_id("archaea"), r"^\d{8}T\d{6}Z_genbank_archaea_root$")

    def test_hidden_domain_schema_isolated_from_bacterial_tables(self) -> None:
        schema = production_store.SCHEMA_SQL
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_inventory_task", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_metadata_fetch_task", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_inventory_snapshot", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_assembly_master", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_assembly_standardization", schema)
        self.assertIn("standardized_payload JSONB NOT NULL", schema)
        self.assertIn("CHECK (domain_key <> 'bacteria')", schema)
        self.assertIn("release_locked BOOLEAN NOT NULL DEFAULT TRUE", schema)
        self.assertIn("PRIMARY KEY (domain_key, snapshot_id)", schema)

    def test_canonical_seed_skips_legacy_sqlite_when_accession_cache_exists(self) -> None:
        coverage = {
            "root_unique_assemblies": 1000,
            "standardized_assemblies": 990,
            "missing_standardized_assemblies": 10,
        }
        with patch.object(canonical_seed_tool, "standardized_metadata_coverage", side_effect=[coverage, coverage]), patch.object(
            canonical_seed_tool, "iter_existing_metadata_rows"
        ) as legacy_scan, patch.object(
            sys, "argv", ["seed", "--snapshot-id", "cache-refresh", "--sqlite-db", "/does/not/exist.sqlite"]
        ):
            self.assertEqual(canonical_seed_tool.main(), 0)
        legacy_scan.assert_not_called()

    def test_canonical_metadata_cache_summary_reports_reuse_without_rewrite(self) -> None:
        root = {
            "configured": True,
            "available": True,
            "status": "completed",
            "snapshot_id": "cache-refresh",
            "root_unique_assemblies": 1000,
            "metadata_seed_status": "completed",
            "metadata_seed_snapshot_id": "cache-refresh",
            "metadata_seed_summary": {
                "cached_standardized_rows_at_start": 990,
                "seeded_standardized_rows": 0,
                "missing_standardized_assemblies": 10,
            },
            "standardized_metadata_coverage": {
                "standardized_assemblies": 990,
                "missing_standardized_assemblies": 10,
            },
        }
        cards = fetchm_app.build_canonical_pipeline_cards(root, {"status": "blocked"}, None)
        self.assertEqual(cards[1]["details"], [
            "Already in canonical cache: 990",
            "Written from legacy cache: 0",
            "Remaining for NCBI retrieval: 10",
            "Time taken: unknown",
        ])

    def test_canonical_inventory_card_shows_checkpoint_progress_and_eta(self) -> None:
        root = {
            "configured": True,
            "available": True,
            "status": "running",
            "task_active": True,
            "snapshot_id": "active-scan",
            "root_unique_assemblies": 0,
            "inventory_elapsed_seconds": 3600,
            "chunk_progress": {
                "records_processed": 250000,
                "expected_total": 1000000,
                "completed": 250,
                "expected_pages": 1000,
                "failed": 0,
            },
        }
        cards = fetchm_app.build_canonical_pipeline_cards(root, {"status": "blocked"}, None)
        inventory = cards[0]
        self.assertEqual(inventory["percent"], 25)
        self.assertIn("Retrieved accessions: 250,000 / 1,000,000", inventory["details"])
        self.assertIn("Pages completed: 250 / 1,000; failed: 0", inventory["details"])
        self.assertIn("throughput: 250,000 accessions/hour; ETA: 3h 0m", inventory["details"][2])

    def test_canonical_auto_publish_stops_when_release_gate_blocks(self) -> None:
        with patch.object(fetchm_app, "canonical_auto_publish_intended", return_value=True), patch.object(
            fetchm_app, "activate_verified_canonical_snapshot", return_value=({"blockers": ["metadata incomplete"]}, "metadata incomplete")
        ), patch.object(fetchm_app, "queue_global_insights_generation") as queue_insights, patch.object(
            fetchm_app, "record_audit_event"
        ):
            fetchm_app.maybe_auto_publish_verified_canonical_snapshot("candidate")
        queue_insights.assert_not_called()

    def test_canonical_auto_publish_activates_then_queues_insights_only_after_pass(self) -> None:
        with patch.object(fetchm_app, "canonical_auto_publish_intended", return_value=True), patch.object(
            fetchm_app, "activate_verified_canonical_snapshot", return_value=({"blockers": []}, None)
        ) as activate, patch.object(fetchm_app, "set_canonical_auto_publish_intent") as clear_intent, patch.object(
            fetchm_app, "queue_global_insights_generation", return_value=("insight-id", [])
        ) as queue_insights, patch.object(fetchm_app, "record_audit_event"):
            fetchm_app.maybe_auto_publish_verified_canonical_snapshot("candidate")
        activate.assert_called_once_with("candidate", automatic=True)
        clear_intent.assert_called_once_with("candidate", False)
        queue_insights.assert_called_once_with(None, demo=False)

    def test_canonical_taxonkit_lineage_keeps_higher_rank_labels_at_true_rank(self) -> None:
        lineage = "1977087\tcellular organisms;Bacteria;Pseudomonadati;Pseudomonadota;Pseudomonadota bacterium\tspecies\n210\tcellular organisms;Bacteria;Campylobacterota;Helicobacter;Helicobacter pylori\tspecies\n"
        reformatted = "1977087\tlineage\tspecies\t\tPseudomonadota\t\t\t\t\tPseudomonadota bacterium\n210\tlineage\tspecies\t\tCampylobacterota\tEpsilonproteobacteria\tCampylobacterales\tHelicobacteraceae\tHelicobacter\tHelicobacter pylori\n"
        resolved = parse_taxonkit_taxonomy_lineages(lineage, reformatted)
        self.assertEqual(resolved[1977087]["domain_name"], "Bacteria")
        self.assertEqual(resolved[1977087]["phylum_name"], "Pseudomonadota")
        self.assertEqual(resolved[1977087]["genus_name"], "")
        self.assertEqual(resolved[210]["genus_name"], "Helicobacter")
        self.assertEqual(resolved[210]["species_name"], "Helicobacter pylori")

    def test_taxonomy_label_classification_keeps_noncanonical_labels_honest(self) -> None:
        self.assertEqual(global_taxonomy_label_metadata("Escherichia coli")["key"], "canonical_species")
        self.assertEqual(global_taxonomy_label_metadata("Staphylococcus sp. HMSC06C11")["key"], "unresolved_species_level_label")
        self.assertEqual(global_taxonomy_label_metadata("Prevotella sp016901395")["key"], "unresolved_species_level_label")
        self.assertEqual(global_taxonomy_label_metadata("Candidatus Liberibacter asiaticus")["key"], "provisional_taxonomic_label")
        self.assertEqual(global_taxonomy_label_metadata("Staphylococcus cohnii species complex 1637")["key"], "unresolved_species_level_label")
        candidatus = fetchm_app.SpeciesRecord(
            id=0, species_name="Candidatus Liberibacter asiaticus", slug="", status="ready",
            created_at="", updated_at="", query_name="Candidatus Liberibacter asiaticus", taxon_rank="species"
        )
        self.assertEqual(fetchm_app.species_parent_genus_name(candidatus), "Liberibacter")
        self.assertEqual(
            fetchm_app.species_derivation_label_from_organism_name("Morganella morganii strain ABC", "Morganella"),
            "Morganella morganii",
        )
        self.assertEqual(
            fetchm_app.species_derivation_label_from_organism_name("Morganella sp. FDAARGOS_123", "Morganella"),
            "Morganella sp. FDAARGOS_123",
        )
        self.assertEqual(
            fetchm_app.species_derivation_label_from_organism_name("Staphylococcus cohnii species complex 1637", "Staphylococcus"),
            "Staphylococcus cohnii species complex 1637",
        )
        self.assertIsNone(
            fetchm_app.species_derivation_label_from_organism_name("Proteus mirabilis", "Morganella")
        )
        self.assertIsNone(
            fetchm_app.species_derivation_candidate_label_from_row(
                "Morganella sp. singleton", "Morganella", source_kind="metadata_search", genome_count=1
            )
        )
        self.assertEqual(
            fetchm_app.species_derivation_candidate_label_from_row(
                "Morganella sp. repeated", "Morganella", source_kind="metadata_search", genome_count=2
            ),
            "Morganella sp. repeated",
        )
        self.assertEqual(
            fetchm_app.species_derivation_candidate_label_from_row(
                "Morganella sp. singleton", "Morganella", source_kind="existing_species", genome_count=1
            ),
            "Morganella sp. singleton",
        )

    def test_non_exact_taxonomy_error_is_genus_only_not_failed(self) -> None:
        error = RuntimeError(
            "Error: The taxonomy name 'Rothia similmucilaginosa' is not exact. "
            "Try using one of the suggested taxids: Rothia mucilaginosa (species, taxid: 43675)"
        )
        self.assertTrue(fetchm_app.is_non_exact_taxonomy_error(error))

    def test_dataset_pipeline_derives_species_after_genus_standardization(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    self.assertEqual(
                        fetchm_app.dataset_pipeline_steps_for_start("metadata", db),
                        ["metadata", "standardization", "derive_species", "verify", "replace", "global_insights"],
                    )
                    self.assertLess(
                        fetchm_app.dataset_pipeline_step_keys().index("standardization"),
                        fetchm_app.dataset_pipeline_step_keys().index("derive_species"),
                    )
                    self.assertIn("genus", fetchm_app.DATASET_PIPELINE_STEP_COPY["standardization"]["short"].lower())
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_release_requires_canonical_genbank_root_reconciliation(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    version_id = "legacy-genus-first-staging"
                    db.execute(
                        "INSERT INTO dataset_versions (version_id, status, created_at, root_path, summary_json) VALUES (?, 'staging', ?, ?, '{}')",
                        (version_id, fetchm_app.utc_now(), str(root / version_id)),
                    )
                    staged = {"staged_genus_metadata_ready": 1, "staged_species_metadata_ready": 1, "staged_unique_assemblies": 1}
                    with patch.object(fetchm_app, "dataset_version_metadata_summary", return_value=staged), patch.object(fetchm_app, "current_live_dataset_summary", return_value={}), patch.object(fetchm_app, "staged_species_search_summary", return_value={}), patch.object(fetchm_app, "derived_species_metadata_task_progress", return_value={}), patch.object(fetchm_app, "staged_non_regression_blockers", return_value=[]):
                        payload, blockers = fetchm_app.build_dataset_release_verification(db, version_id)
                    self.assertFalse(payload["safe_to_replace"])
                    self.assertTrue(any("Canonical GenBank root inventory reconciliation is missing" in blocker for blocker in blockers))
                    root_gate = {
                        "status": "pass", "source_database": "genbank",
                        "canonical_accession_namespace": "GCA", "root_unique_assemblies": 7,
                        "accounted_unique_assemblies": 7, "release_views_materialized": False,
                    }
                    db.execute("UPDATE dataset_versions SET summary_json = ? WHERE version_id = ?", (json.dumps({"canonical_root_reconciliation": root_gate}), version_id))
                    with patch.object(fetchm_app, "dataset_version_metadata_summary", return_value=staged), patch.object(fetchm_app, "current_live_dataset_summary", return_value={}), patch.object(fetchm_app, "staged_species_search_summary", return_value={}), patch.object(fetchm_app, "derived_species_metadata_task_progress", return_value={}), patch.object(fetchm_app, "staged_non_regression_blockers", return_value=[]):
                        payload, blockers = fetchm_app.build_dataset_release_verification(db, version_id)
                    self.assertFalse(payload["safe_to_replace"])
                    self.assertTrue(any("preview" in blocker for blocker in blockers))
                    root_gate["release_views_materialized"] = True
                    db.execute("UPDATE dataset_versions SET summary_json = ? WHERE version_id = ?", (json.dumps({"canonical_root_reconciliation": root_gate}), version_id))
                    with patch.object(fetchm_app, "dataset_version_metadata_summary", return_value=staged), patch.object(fetchm_app, "current_live_dataset_summary", return_value={}), patch.object(fetchm_app, "staged_species_search_summary", return_value={}), patch.object(fetchm_app, "derived_species_metadata_task_progress", return_value={}), patch.object(fetchm_app, "staged_non_regression_blockers", return_value=[]):
                        payload, blockers = fetchm_app.build_dataset_release_verification(db, version_id)
                    self.assertTrue(payload["safe_to_replace"])
                    self.assertFalse(any("Canonical" in blocker for blocker in blockers))
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_pipeline_completion_does_not_queue_past_pending_successor(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    fetchm_app.set_setting("dataset_pipeline_auto_publish_insights", "1", db)
                    run_id, error = fetchm_app.queue_dataset_update_pipeline_run("manual", start_step="verify")
                    self.assertIsNone(error)
                    self.assertIsNotNone(run_id)
                    steps = db.execute(
                        "SELECT step_key, step_order, status FROM dataset_update_pipeline_steps WHERE run_id = ? ORDER BY step_order",
                        (run_id,),
                    ).fetchall()
                    self.assertEqual([row["step_key"] for row in steps], ["verify", "replace", "global_insights"])
                    db.execute("UPDATE dataset_update_pipeline_steps SET status = 'completed' WHERE run_id = ? AND step_key = 'verify'", (run_id,))
                    db.execute("UPDATE dataset_update_pipeline_steps SET status = 'pending' WHERE run_id = ? AND step_key = 'replace'", (run_id,))
                    fetchm_app.queue_next_pipeline_step(db, str(run_id), int(steps[0]["step_order"]))
                    statuses = {
                        row["step_key"]: row["status"]
                        for row in db.execute(
                            "SELECT step_key, status FROM dataset_update_pipeline_steps WHERE run_id = ?",
                            (run_id,),
                        ).fetchall()
                    }
                    self.assertEqual(statuses["replace"], "pending")
                    self.assertEqual(statuses["global_insights"], "waiting")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_global_insights_pipeline_step_is_reserved_for_dedicated_worker(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    fetchm_app.set_setting("dataset_pipeline_auto_publish_insights", "1", db)
                    run_id, error = fetchm_app.queue_dataset_update_pipeline_run("manual", start_step="verify")
                    self.assertIsNone(error)
                    db.execute(
                        "UPDATE dataset_update_pipeline_steps SET status = 'completed' WHERE run_id = ? AND step_key IN ('verify', 'replace')",
                        (run_id,),
                    )
                    db.execute(
                        "UPDATE dataset_update_pipeline_steps SET status = 'pending' WHERE run_id = ? AND step_key = 'global_insights'",
                        (run_id,),
                    )
                    db.execute("UPDATE dataset_update_pipeline_runs SET status = 'running' WHERE run_id = ?", (run_id,))
                    db.commit()
                    metadata_claim = fetchm_app.claim_next_dataset_pipeline_step(
                        "metadata-worker", excluded_step_keys={"global_insights"}
                    )
                    self.assertIsNone(metadata_claim)
                    with patch.object(fetchm_app, "global_insights_step_blockers", return_value=[]):
                        insight_claim = fetchm_app.claim_next_dataset_pipeline_step(
                            "insights-worker", allowed_step_keys={"global_insights"}
                        )
                    self.assertIsNotNone(insight_claim)
                    self.assertEqual(str(insight_claim["step_key"]), "global_insights")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_derive_species_reuses_prior_live_species_when_source_genus_is_current(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    version_id = "staging-single-inventory-test"
                    db.execute(
                        "INSERT INTO dataset_versions (version_id, status, created_at, root_path, summary_json) VALUES (?, 'staging', ?, ?, '{}')",
                        (version_id, fetchm_app.utc_now(), str(root / version_id)),
                    )
                    genus = fetchm_app.create_species("Example", db=db, taxon_rank="genus", staging_dataset_version_id=version_id)
                    genus.staging_dataset_version_id = None
                    genus.is_live = True
                    genus.live_status = "ready"
                    genus.live_metadata_status = "ready"
                    genus.live_metadata_clean_path = str(root / "genus_clean.csv")
                    genus.live_metadata_path = str(root / "genus_clean.csv")
                    fetchm_app.save_species(genus, db)
                    self.assertEqual(fetchm_app.seed_live_genus_inputs_for_species_derivation(db, version_id), 1)
                    genus = fetchm_app.get_species_by_id(genus.id, db)
                    self.assertEqual(genus.staging_dataset_version_id, version_id)
                    species_file = root / "old_species_clean.csv"
                    species_file.write_text("Assembly Accession\nGCA_1\n", encoding="utf-8")
                    reusable = fetchm_app.create_species("Example reusable", db=db, taxon_rank="species")
                    reusable.is_live = True
                    reusable.live_status = "ready"
                    reusable.live_metadata_status = "ready"
                    reusable.live_metadata_clean_path = str(species_file)
                    reusable.live_metadata_path = str(species_file)
                    reusable.live_genome_count = 1
                    reusable.live_metadata_last_built_at = genus.metadata_last_built_at
                    reusable.metadata_source_taxon_id = genus.id
                    fetchm_app.save_species(reusable, db)
                    reused = fetchm_app.pre_stage_reusable_species_metadata(db, version_id)
                    self.assertEqual(reused["reuse_preflight_reused"], 1)
                    progress = fetchm_app.dataset_version_metadata_summary(db, version_id)
                    self.assertEqual(progress["staged_species_metadata_ready"], 1)
                    queued = db.execute(
                        "SELECT COUNT(*) AS total FROM species_reconciliation_tasks WHERE dataset_version_id=?",
                        (version_id,),
                    ).fetchone()
                    self.assertEqual(int(queued["total"] or 0), 0)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_species_derivation_preflight_reuses_only_current_existing_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    version_id = "staging-reuse-test"
                    genus_file = root / "genus_clean.csv"
                    genus_file.write_text("Assembly Accession\nGCA_1\n", encoding="utf-8")
                    genus = fetchm_app.create_species("Example", db=db, taxon_rank="genus")
                    genus.is_live = True
                    genus.live_status = "ready"
                    genus.live_metadata_status = "ready"
                    genus.live_metadata_clean_path = str(genus_file)
                    genus.live_metadata_path = str(genus_file)
                    genus.live_metadata_last_built_at = "2026-05-20T00:00:00+00:00"
                    fetchm_app.save_species(genus, db)
                    self.assertEqual(fetchm_app.seed_live_genus_inputs_for_species_derivation(db, version_id), 1)

                    reusable_file = root / "reusable_clean.csv"
                    reusable_file.write_text("Assembly Accession\nGCA_1\n", encoding="utf-8")
                    reusable = fetchm_app.create_species("Example testus", db=db, taxon_rank="species")
                    reusable.is_live = True
                    reusable.live_status = "ready"
                    reusable.live_metadata_status = "ready"
                    reusable.live_metadata_clean_path = str(reusable_file)
                    reusable.live_metadata_path = str(reusable_file)
                    reusable.live_metadata_last_built_at = "2026-05-21T00:00:00+00:00"
                    reusable.metadata_source_taxon_id = genus.id
                    fetchm_app.save_species(reusable, db)

                    stale_file = root / "stale_clean.csv"
                    stale_file.write_text("Assembly Accession\nGCA_2\n", encoding="utf-8")
                    stale = fetchm_app.create_species("Example staleus", db=db, taxon_rank="species")
                    stale.is_live = True
                    stale.live_status = "ready"
                    stale.live_metadata_status = "ready"
                    stale.live_metadata_clean_path = str(stale_file)
                    stale.live_metadata_path = str(stale_file)
                    stale.live_metadata_last_built_at = "2026-05-19T00:00:00+00:00"
                    stale.metadata_source_taxon_id = genus.id
                    fetchm_app.save_species(stale, db)

                    missing = fetchm_app.create_species("Example missingus", db=db, taxon_rank="species")
                    missing.is_live = True
                    missing.live_status = "ready"
                    missing.live_metadata_status = "ready"
                    missing.live_metadata_clean_path = str(root / "missing_clean.csv")
                    missing.live_metadata_path = missing.live_metadata_clean_path
                    missing.live_metadata_last_built_at = "2026-05-21T00:00:00+00:00"
                    missing.metadata_source_taxon_id = genus.id
                    fetchm_app.save_species(missing, db)

                    summary = fetchm_app.pre_stage_reusable_species_metadata(db, version_id)
                    self.assertEqual(summary["reuse_preflight_reused"], 1)
                    self.assertEqual(summary["reuse_preflight_eligible"], 1)
                    self.assertEqual(summary["reuse_preflight_missing_files"], 1)
                    self.assertEqual(fetchm_app.get_species_by_id(reusable.id, db).staging_dataset_version_id, version_id)
                    self.assertIsNone(fetchm_app.get_species_by_id(stale.id, db).staging_dataset_version_id)
                    self.assertIsNone(fetchm_app.get_species_by_id(missing.id, db).staging_dataset_version_id)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_standardization_progress_counts_done_tasks(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    clean_path = fetchm_app.DATA_DIR / "clean.tsv"
                    clean_path.write_text("Assembly Accession\nGCA_000001.1\n", encoding="utf-8")
                    genus = fetchm_app.create_species("Example", taxon_rank="genus")
                    genus.status = "ready"
                    genus.metadata_status = "ready"
                    genus.metadata_clean_path = str(clean_path)
                    genus.genome_count = 1
                    fetchm_app.save_species(genus)
                    requested_at = "2026-05-16T10:57:35.206585+00:00"
                    db = fetchm_app.get_db()
                    db.execute(
                        """
                        INSERT INTO standardization_refresh_tasks (
                            species_id, status, requested_at, completed_at, total_rows, updated_rows
                        )
                        VALUES (?, 'done', ?, ?, 1, 1)
                        """,
                        (genus.id, requested_at, requested_at),
                    )
                    db.commit()

                    progress = fetchm_app.standardization_refresh_progress(
                        db,
                        requested_at=requested_at,
                        rank_scope="genus",
                    )
                    counts = fetchm_app.dataset_pipeline_rank_counts(db)

                    self.assertEqual(progress["standardization_scope_total"], 1)
                    self.assertEqual(progress["standardization_scope_done"], 1)
                    self.assertEqual(progress["standardization_scope_active"], 0)
                    self.assertEqual(counts["standardization_completed"], 1)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_metadata_prune_removes_stale_accessions_without_rewriting_all_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    species = fetchm_app.create_species("Example testus", taxon_rank="species")
                    fetchm_app.save_taxon_metadata_rows(
                        species.id,
                        [
                            {"Assembly Accession": "GCA_KEEP.1", "Organism Name": "Example testus"},
                            {"Assembly Accession": "GCA_DROP.1", "Organism Name": "Example testus"},
                        ],
                        refreshed_at=fetchm_app.utc_now(),
                    )

                    removed = fetchm_app.prune_taxon_metadata_rows_to_accessions(species.id, {"GCA_KEEP.1"})
                    rows = fetchm_app.load_taxon_metadata_rows(species.id)

                    self.assertEqual(removed, 1)
                    self.assertEqual(set(rows), {"GCA_KEEP.1"})
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_species_derived_outputs_skip_updated_tsv(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    metadata_path, clean_path, clean_count = fetchm_app.write_taxon_metadata_outputs(
                        "example-testus",
                        [
                            {
                                "Assembly Accession": "GCA_000001.1",
                                "Assembly Name": "ASM1",
                                "Organism Name": "Example testus",
                                "Country": "United States",
                            }
                        ],
                        normalize_rows=False,
                        dataset_version_id="staging-test",
                        write_updated_tsv=False,
                    )

                    self.assertEqual(metadata_path, clean_path)
                    self.assertEqual(clean_count, 1)
                    self.assertTrue(Path(clean_path).exists())
                    self.assertFalse((Path(clean_path).parent / "ncbi_dataset_updated.tsv").exists())
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_standardization_reuses_unchanged_rows_and_forces_when_requested(self) -> None:
        row = {
            "Assembly Accession": "GCA_000001.1",
            "Assembly Name": "ASM1",
            "Organism Name": "Escherichia coli strain test",
            "Host": "human",
            "Isolation Source": "stool",
            "Geographic Location": "USA",
            "Collection Date": "2024",
        }
        first = ensure_managed_metadata_schema(dict(row))
        fingerprint_column = fetchm_app.METADATA_STANDARDIZATION_INPUT_FINGERPRINT_COLUMN
        self.assertTrue(first.get(fingerprint_column))

        legacy_reusable = dict(first)
        legacy_reusable.pop(fingerprint_column, None)
        legacy_reusable.pop(fetchm_app.METADATA_STANDARDIZATION_UPDATED_AT_COLUMN, None)
        legacy_reusable["Host_SD"] = "Existing standardized host"
        seeded = ensure_managed_metadata_schema(legacy_reusable)
        self.assertEqual(seeded["Host_SD"], "Existing standardized host")
        self.assertTrue(seeded.get(fingerprint_column))

        changed_same_input = dict(seeded)
        changed_same_input["Host_SD"] = "Preserved standardized host"
        reused = ensure_managed_metadata_schema(changed_same_input)
        self.assertEqual(reused["Host_SD"], "Preserved standardized host")

        forced = ensure_managed_metadata_schema(changed_same_input, force_standardization=True)
        self.assertNotEqual(forced["Host_SD"], "Preserved standardized host")

        changed_raw_input = dict(reused)
        changed_raw_input["Host"] = "mouse"
        refreshed = ensure_managed_metadata_schema(changed_raw_input)
        self.assertNotEqual(refreshed["Host_SD"], "Preserved standardized host")

    def test_advanced_quality_job_detail_loads_parent_owner_field(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    parent = fetchm_app.JobRecord(
                        id="parent-standard",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="parent.csv",
                        input_path=str(root / "parent.csv"),
                        output_dir=str(root / "parent_outputs"),
                        log_path=str(root / "parent.log"),
                        command=[],
                        owner_user_id=42,
                        filters={},
                    )
                    advanced = fetchm_app.JobRecord(
                        id="advanced-child",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="advanced.csv",
                        input_path=str(root / "advanced.csv"),
                        output_dir=str(root / "advanced_outputs"),
                        log_path=str(root / "advanced.log"),
                        command=[],
                        owner_user_id=42,
                        filters={"advanced_qc": True, "parent_quality_job_id": parent.id},
                    )
                    fetchm_app.save_job(parent)
                    fetchm_app.save_job(advanced)

                    with fetchm_app.app.test_request_context("/jobs/advanced-child"):
                        loaded = fetchm_app.load_job("advanced-child")
                        parent_quality_job = None
                        parent_quality_job_id = str(loaded.filters.get("parent_quality_job_id") or "").strip()
                        if parent_quality_job_id:
                            candidate_parent = fetchm_app.load_job(parent_quality_job_id)
                            if candidate_parent.owner_user_id == loaded.owner_user_id:
                                parent_quality_job = candidate_parent

                    self.assertIsNotNone(parent_quality_job)
                    self.assertEqual(parent_quality_job.id, "parent-standard")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_qc_preview_does_not_display_numeric_ani_closest_as_genome(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_000001.1",
                    "Sequence_QC_Status": "review",
                    "ANI_Closest_ANI": "87.79",
                    "ANI_Closest_Genome": "97.69",
                }
            ]
        )

        preview = build_qc_decision_preview(frame)

        self.assertEqual(preview[0]["ani"], "87.79")
        self.assertEqual(preview[0]["ani_closest"], "")

    def test_qc_preview_treats_zero_ani_as_unresolved(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_000001.1",
                    "Sequence_QC_Status": "review",
                    "Sequence_QC_Review_Reasons": "ani_species_warning:0<95",
                    "ANI_Closest_ANI": "0.0",
                    "ANI_Closest_Genome": "0.00",
                    "ANI_Species_Consistency_Status": "WARN",
                }
            ]
        )

        preview = build_qc_decision_preview(frame)

        self.assertEqual(preview[0]["ani"], "")
        self.assertEqual(preview[0]["ani_closest"], "")
        self.assertEqual(preview[0]["ani_status"], "WARN")
        self.assertEqual(preview[0]["reasons"], "NO_VALID_ANI_RESULT")

    def test_quality_submission_blockers_guard_low_memory_and_active_qc(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            old_reader = fetchm_app.read_memory_usage
            old_min_memory = fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES
            old_max_active = fetchm_app.MAX_ACTIVE_QUALITY_JOBS
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES = 12 * 1024**3
            fetchm_app.MAX_ACTIVE_QUALITY_JOBS = 1
            fetchm_app.read_memory_usage = lambda: {
                "total_bytes": 128 * 1024**3,
                "used_bytes": 124 * 1024**3,
                "available_bytes": 4 * 1024**3,
                "used_percent": 96.9,
                "total_label": "128 GiB",
                "used_label": "124 GiB",
                "available_label": "4 GiB",
            }
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    fetchm_app.save_job(
                        fetchm_app.JobRecord(
                            id="active-qc",
                            mode="qc",
                            status="running",
                            created_at=fetchm_app.utc_now(),
                            updated_at=fetchm_app.utc_now(),
                            input_name="input.csv",
                            input_path=str(root / "input.csv"),
                            output_dir=str(root / "outputs"),
                            log_path=str(root / "job.log"),
                            command=[],
                            owner_user_id=1,
                        )
                    )

                    blockers = fetchm_app.quality_submission_blockers()

                    self.assertTrue(any("low on available memory" in item for item in blockers))
                    self.assertTrue(any("memory load is too high" in item for item in blockers))
                    self.assertTrue(any("quality job(s) are already queued or running" in item for item in blockers))
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths
                fetchm_app.read_memory_usage = old_reader
                fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES = old_min_memory
                fetchm_app.MAX_ACTIVE_QUALITY_JOBS = old_max_active

    def test_qc_reason_text_is_deduplicated(self) -> None:
        self.assertEqual(
            dedupe_reason_text(
                "min_completeness:87.48<90;min_completeness:87.48<90",
                " min_completeness:87.48<90 ",
            ),
            "min_completeness:87.48<90",
        )

    def test_cancelled_running_job_reconciles_when_worker_claim_is_gone(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    job = fetchm_app.JobRecord(
                        id="stale-cancel",
                        mode="qc",
                        status="running",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="input.csv",
                        input_path=str(root / "input.csv"),
                        output_dir=str(root / "outputs"),
                        log_path=str(root / "data" / "jobs" / "stale-cancel" / "job.log"),
                        command=[],
                        return_code=None,
                        cancel_requested=True,
                        claimed_by="dead-worker:123",
                        claimed_at=fetchm_app.utc_now(),
                    )
                    fetchm_app.save_job(job)

                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(), 1)
                    updated = fetchm_app.load_job("stale-cancel")

                    self.assertEqual(updated.status, "cancelled")
                    self.assertEqual(updated.return_code, 1)
                    self.assertIsNone(updated.claimed_by)
                    self.assertIn("no live worker claim", updated.error or "")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_worker_reconciles_own_cancelled_job_after_returning_to_queue(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    worker_name = "live-worker:456"
                    fetchm_app.touch_worker_heartbeat(worker_name)
                    job = fetchm_app.JobRecord(
                        id="own-stale-cancel",
                        mode="qc",
                        status="running",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="input.csv",
                        input_path=str(root / "input.csv"),
                        output_dir=str(root / "outputs"),
                        log_path=str(root / "data" / "jobs" / "own-stale-cancel" / "job.log"),
                        command=[],
                        return_code=None,
                        cancel_requested=True,
                        claimed_by=worker_name,
                        claimed_at=fetchm_app.utc_now(),
                    )
                    fetchm_app.save_job(job)

                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(), 0)
                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(worker_name), 1)
                    updated = fetchm_app.load_job("own-stale-cancel")

                    self.assertEqual(updated.status, "cancelled")
                    self.assertIsNone(updated.claimed_by)
                    self.assertIn("claiming worker returned", updated.error or "")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_geography_false_positive_guards(self) -> None:
        self.assertIsNone(extract_country("Hospital"))
        self.assertIsNone(extract_country("St Margaret's Hospital"))
        self.assertIsNone(extract_country("Outpatient"))
        self.assertIsNone(extract_country("ground turkey"))
        self.assertIsNone(extract_country("Guinea pig"))
        self.assertIsNone(extract_country("Norway rat"))
        self.assertIsNone(extract_country("Aspergillus niger"))

    def test_valid_geography_still_maps(self) -> None:
        bangladesh = ensure_managed_metadata_schema({"Geographic Location": "Bangladesh: Dhaka"})
        self.assertEqual(bangladesh["Country"], "Bangladesh")
        self.assertEqual(bangladesh["Continent"], "Asia")
        united_states = ensure_managed_metadata_schema({"Geographic Location": "United States: California"})
        self.assertEqual(united_states["Country"], "United States")
        self.assertEqual(united_states["Continent"], "North America")

    def test_host_sample_source_separation(self) -> None:
        human = ensure_managed_metadata_schema({"Host": "", "Sample Type": "human"})
        self.assertEqual(human["Sample_Type_SD"], "")

        human_blood = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human blood"})
        self.assertEqual(human_blood["Host_SD"], "Homo sapiens")
        self.assertEqual(human_blood["Sample_Type_SD"], "blood")

        human_feces = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human feces"})
        self.assertEqual(human_feces["Host_SD"], "Homo sapiens")
        self.assertEqual(human_feces["Sample_Type_SD"], "feces/stool")
        self.assertNotEqual(human_feces["Environment_Medium_SD"], "feces/stool")

        bacteria_culture = standardize_host_metadata("bacteria culture")
        self.assertEqual(bacteria_culture["Host_SD"], "")
        self.assertEqual(bacteria_culture["Host_TaxID"], "")

        dh5a = standardize_host_metadata("DH5a")
        self.assertEqual(dh5a["Host_SD"], "")
        self.assertEqual(dh5a["Host_TaxID"], "")

        xl10 = standardize_host_metadata("XL10-gold")
        self.assertEqual(xl10["Host_SD"], "")
        self.assertEqual(xl10["Host_TaxID"], "")

        patient = ensure_managed_metadata_schema({"Host": "", "Sample Type": "patient"})
        self.assertEqual(patient["Sample_Type_SD"], "")

        cattle_feces = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "cattle feces"})
        self.assertEqual(cattle_feces["Host_SD"], "Bos taurus")
        self.assertEqual(cattle_feces["Sample_Type_SD"], "feces/stool")

        water_deer = ensure_managed_metadata_schema({"Host": "water deer", "Isolation Source": ""})
        self.assertEqual(water_deer["Host_SD"], "Hydropotes inermis")
        self.assertNotEqual(water_deer["Environment_Medium_SD"], "water")

        water_buffalo = ensure_managed_metadata_schema({"Host": "water buffalo", "Isolation Source": ""})
        self.assertEqual(water_buffalo["Host_SD"], "Bubalus bubalis")
        self.assertNotEqual(water_buffalo["Environment_Medium_SD"], "water")

    def test_environment_medium_examples(self) -> None:
        feces = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "feces/stool"})
        self.assertNotEqual(feces["Environment_Medium_SD"], "feces/stool")

        soil = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "soil"})
        self.assertEqual(soil["Environment_Medium_SD"], "soil")

        wastewater = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "wastewater"})
        self.assertEqual(wastewater["Environment_Medium_SD"], "wastewater")

        seawater = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "seawater"})
        self.assertEqual(seawater["Environment_Medium_SD"], "seawater")

    def test_broad_categories_do_not_leak_raw_values(self) -> None:
        self.assertEqual(broad_standardization_category("Marmota himalayana"), "")
        self.assertEqual(broad_standardization_category("Nottingham"), "")
        self.assertEqual(broad_standardization_category("Osteomyelitis"), "")
        self.assertEqual(broad_standardization_category("L_cheek"), "")
        self.assertEqual(broad_standardization_category("#REF!"), "")
        self.assertEqual(broad_standardization_category("whole organism"), "host-associated context")
        self.assertEqual(broad_standardization_category("poultry"), "host-associated context")
        self.assertEqual(broad_standardization_category("host-associated organism"), "host-associated context")
        self.assertEqual(
            broad_standardization_category("urogenital/gastrointestinal site"),
            "clinical/host-associated material",
        )
        self.assertEqual(broad_standardization_category("Klíčava reservoir"), "water")
        self.assertEqual(broad_standardization_category("hydrothermal vent"), "environmental/geologic material")
        self.assertEqual(broad_standardization_category("Wall biofilm"), "biofilm")
        self.assertEqual(broad_standardization_category("rectal swab"), "swab")
        self.assertEqual(broad_standardization_category("urogenital/reproductive swab"), "swab")
        self.assertEqual(broad_standardization_category("river water"), "water")

    def test_secondary_only_restandardization_matches_full_routing(self) -> None:
        from tools.restandardize_canonical_metadata import restandardize_secondary_row

        secondary_fields = [
            "Isolation_Source_SD",
            "Isolation_Source_SD_Broad",
            "Sample_Type_SD",
            "Environment_Medium_SD",
            "Isolation_Site_SD",
            "Host_Disease_SD",
            "Host_Health_State_SD",
        ]
        for source in ["blood", "soil", "lake water", "groin", "metagenome", "chicken meat"]:
            existing = ensure_managed_metadata_schema({"Host": "", "Isolation Source": source})
            full = ensure_managed_metadata_schema(dict(existing), force_standardization=True)
            secondary = restandardize_secondary_row(existing)
            self.assertEqual(
                {field: secondary.get(field, "") for field in secondary_fields},
                {field: full.get(field, "") for field in secondary_fields},
            )

    def test_body_site_sample_source_separation(self) -> None:
        groin = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "groin"})
        self.assertEqual(groin["Isolation_Site_SD"], "skin/body surface")
        self.assertNotEqual(groin["Isolation_Source_SD"], "groin")

        soil = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "soil"})
        self.assertEqual(soil["Environment_Medium_SD"], "soil")
        self.assertEqual(soil["Isolation_Source_SD"], "environmental material")

        lake_water = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "lake water"})
        self.assertEqual(lake_water["Environment_Medium_SD"], "lake water")
        self.assertEqual(lake_water["Isolation_Source_SD"], "environmental material")

        canal_water = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "canal water"})
        self.assertEqual(canal_water["Environment_Medium_SD"], "canal water")
        self.assertEqual(canal_water["Isolation_Source_SD"], "environmental material")

        ear_canal = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "ear canal"})
        self.assertEqual(ear_canal["Isolation_Site_SD"], "organ/tissue site")
        self.assertEqual(ear_canal["Isolation_Source_SD"], "clinical/host-associated material")

        respiratory = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "respiratory sample"})
        self.assertEqual(respiratory["Sample_Type_SD"], "respiratory sample")
        self.assertEqual(respiratory["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(respiratory["Isolation_Source_SD_Method"], "sample_context_router")

        leaf_tissue = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "leaf tissue"})
        self.assertEqual(leaf_tissue["Isolation_Site_SD"], "leaf tissue")
        self.assertEqual(leaf_tissue["Isolation_Source_SD"], "plant-associated material")
        self.assertEqual(leaf_tissue["Isolation_Source_SD_Method"], "host_context_router")

        for material, expected in [
            ("blood", "blood"),
            ("stool", "feces/stool"),
            ("urine", "urine"),
            ("sputum", "sputum"),
        ]:
            standardized = ensure_managed_metadata_schema({"Host": "", "Isolation Source": material})
            self.assertEqual(standardized["Sample_Type_SD"], expected)
            self.assertEqual(standardized["Isolation_Source_SD"], "clinical/host-associated material")
            self.assertEqual(standardized["Isolation_Site_SD"], "")

        for host_only in ["human", "patient", "chicken"]:
            standardized = ensure_managed_metadata_schema({"Host": "", "Isolation Source": host_only})
            self.assertNotEqual(standardized["Isolation_Source_SD"], host_only)

        rectal_swab = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "rectal swab"})
        self.assertEqual(rectal_swab["Sample_Type_SD"], "rectal swab")
        self.assertEqual(rectal_swab["Isolation_Site_SD"], "rectum/perianal region")
        self.assertEqual(rectal_swab["Host_Anatomical_Site_SD"], "rectum/perianal region")
        self.assertEqual(rectal_swab["Isolation_Source_SD"], "clinical/host-associated material")

        nasal_swab = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "nasal swab"})
        self.assertEqual(nasal_swab["Sample_Type_SD"], "nasal swab")
        self.assertEqual(nasal_swab["Isolation_Site_SD"], "nasal cavity/sinus/upper respiratory tract")

        bronchial_lavage = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "bronchial lavage"})
        self.assertEqual(bronchial_lavage["Sample_Type_SD"], "bronchial wash/lavage")
        self.assertEqual(bronchial_lavage["Isolation_Site_SD"], "lower respiratory tract/bronch/pleural cavity")

        pleural_fluid = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "pleural fluid"})
        self.assertEqual(pleural_fluid["Sample_Type_SD"], "pleural fluid")
        self.assertEqual(pleural_fluid["Isolation_Source_SD"], "clinical fluid/material")

        dental_plaque = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "dental plaque"})
        self.assertEqual(dental_plaque["Sample_Type_SD"], "dental plaque")
        self.assertEqual(dental_plaque["Sample_Type_SD_Broad"], "clinical/host-associated material")
        self.assertEqual(dental_plaque["Isolation_Site_SD"], "oral cavity")

        perineum = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Perineum"})
        self.assertEqual(perineum["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(perineum["Isolation_Site_SD"], "skin/body surface")

        nasal_context = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Healthcare worker (nasal)"})
        self.assertEqual(nasal_context["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(nasal_context["Isolation_Site_SD"], "nasal cavity/sinus/upper respiratory tract")

        conjunctiva = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Right conjunctiva of a child"})
        self.assertEqual(conjunctiva["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(conjunctiva["Isolation_Site_SD"], "organ/tissue site")

    def test_batch2_material_fallback_after_descriptor_sample_type(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source, expected in [
            ("feces", "feces/stool"),
            ("stool", "feces/stool"),
            ("faeces", "feces/stool"),
            ("blood", "blood"),
            ("serum", "blood-derived material"),
            ("plasma", "blood-derived material"),
            ("urine", "urine"),
            ("sputum", "sputum"),
            ("saliva", "saliva"),
            ("tissue", "tissue"),
            ("swab", "swab"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Sample_Type_SD"], expected, source)
            self.assertNotEqual(result["Isolation_Source_SD"].casefold(), expected.casefold(), source)

        milk = standardized("milk")
        self.assertEqual(milk["Sample_Type_SD"], "milk")
        self.assertEqual(milk["Isolation_Source_SD"], "")

        for source, sample_type, site in [
            ("pericardial tissue", "tissue", "organ/tissue site"),
            ("bladder tissue", "tissue", "urogenital tract"),
            ("Gall bladder tissue", "tissue", "organ/tissue site"),
            ("wound swab", "wound swab", "wound"),
            ("rectal swab", "rectal swab", "rectum/perianal region"),
            ("nasal swab", "nasal swab", "nasal cavity/sinus/upper respiratory tract"),
            ("throat swab", "oropharyngeal/throat swab", "nasopharynx/oropharynx"),
            ("oropharyngeal/throat swab", "oropharyngeal/throat swab", "nasopharynx/oropharynx"),
            ("cloacal swab", "cloacal swab", "cloaca"),
            ("tracheal aspirate", "tracheal aspirate/secretion", "lower respiratory tract/bronch/pleural cavity"),
            ("bronchoalveolar lavage", "bronchoalveolar lavage fluid", "lower respiratory tract/bronch/pleural cavity"),
            ("gastric biopsy", "gastric biopsy", "gastrointestinal tract"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Sample_Type_SD"], sample_type, source)
            self.assertEqual(result["Isolation_Site_SD"], site, source)
            self.assertNotEqual(result["Isolation_Source_SD"].casefold(), source.casefold(), source)

        plant_tissue = standardized("leaf tissue")
        self.assertEqual(plant_tissue["Sample_Type_SD"], "plant tissue")
        self.assertEqual(plant_tissue["Isolation_Source_SD"], "plant-associated material")
        self.assertEqual(plant_tissue["Isolation_Site_SD"], "leaf tissue")

        clinical = standardized("clinical sample")
        self.assertEqual(clinical["Isolation_Source_SD"], "clinical/host-associated material")
        respiratory = standardized("respiratory sample")
        self.assertEqual(respiratory["Isolation_Site_SD"], "respiratory tract")
        chicken_meat = standardized("chicken meat")
        self.assertEqual(chicken_meat["Isolation_Source_SD_Broad"], "food/meat")

    def test_batch3_body_site_ambiguity_and_context_precedence(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source, expected_site in [
            ("groin", "skin/body surface"),
            ("wound", "wound"),
            ("surgical site", "wound"),
            ("bite wound", "wound"),
            ("rectum", "rectum/perianal region"),
            ("throat", "nasopharynx/oropharynx"),
            ("nasal cavity", "nasal cavity/sinus/upper respiratory tract"),
            ("lung", "lower respiratory tract/bronch/pleural cavity"),
            ("bladder", "urogenital tract"),
            ("cloaca", "cloaca"),
            ("ear canal", "organ/tissue site"),
            ("birth canal", "urogenital tract"),
            ("anal canal", "rectum/perianal region"),
            ("root canal", "oral cavity"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Site_SD"], expected_site, source)
            self.assertNotEqual(result["Isolation_Source_SD"].casefold(), source.casefold(), source)

        for source, medium in [
            ("canal water", "canal water"),
            ("canal sediment", "sediment"),
            ("drainage water", "drainage water"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Environment_Medium_SD"], medium, source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
            self.assertEqual(result["Isolation_Source_SD"], "environmental material", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)

        irrigation = standardized("irrigation canal")
        self.assertEqual(irrigation["Environment_Local_Scale_SD"], "irrigation canal")
        self.assertEqual(irrigation["Isolation_Site_SD"], "")
        self.assertEqual(irrigation["Isolation_Source_SD"], "environmental material")

        for source in ["canal", "drainage", "surface"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)

        wound_drainage = standardized("wound drainage")
        self.assertEqual(wound_drainage["Sample_Type_SD"], "drainage")
        self.assertEqual(wound_drainage["Isolation_Site_SD"], "wound")

        hospital_surface = standardized("hospital surface")
        self.assertEqual(hospital_surface["Isolation_Source_SD_Broad"], "healthcare-associated environment")
        self.assertEqual(hospital_surface["Isolation_Site_SD"], "")

        skin_surface = standardized("skin surface")
        self.assertEqual(skin_surface["Isolation_Site_SD"], "skin/body surface")

        for source in ["chicken breast meat", "pork liver", "beef heart"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "food/meat", source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)

        for source in ["fish gut", "oyster tissue"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
            self.assertNotEqual(result["Isolation_Source_SD"], "clinical/host-associated material", source)

        self.assertEqual(standardized("clinical sample")["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(standardized("respiratory sample")["Isolation_Site_SD"], "respiratory tract")

    def test_batch4_environment_media_context_and_precedence(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source, medium in [
            ("river sediment", "sediment"),
            ("canal sediment", "sediment"),
            ("wastewater treatment plant influent", "wastewater"),
            ("wastewater treatment plant effluent", "wastewater"),
            ("sewage influent", "sewage"),
            ("drainage water", "drainage water"),
            ("poultry litter", "agricultural organic material"),
            ("animal bedding", "agricultural organic material"),
            ("sink biofilm", "biofilm"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Environment_Medium_SD"], medium, source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
            self.assertNotEqual(result["Isolation_Source_SD"], "clinical/host-associated material", source)

        treatment_plant = standardized("wastewater treatment plant influent")
        self.assertEqual(treatment_plant["Isolation_Source_SD"], "environmental material")
        self.assertEqual(treatment_plant["Sample_Type_SD"], "")

        estuary = standardized("estuary")
        self.assertEqual(estuary["Environment_Broad_Scale_SD"], "estuarine environment")
        self.assertEqual(estuary["Environment_Local_Scale_SD"], "estuary")
        self.assertEqual(estuary["Isolation_Source_SD"], "environmental material")
        self.assertEqual(estuary["Sample_Type_SD"], "")

        digester = standardized("anaerobic digester")
        self.assertEqual(digester["Isolation_Source_SD"], "wastewater/organic waste")
        self.assertEqual(digester["Sample_Type_SD"], "")

        enrichment = standardized("enrichment culture")
        self.assertEqual(enrichment["Isolation_Source_SD"], "culture")
        self.assertEqual(enrichment["Sample_Type_SD"], "enrichment culture")

        benzene_enrichment = standardized("benzene-degrading enrichment culture")
        self.assertEqual(benzene_enrichment["Isolation_Source_SD"], "")
        self.assertEqual(benzene_enrichment["Sample_Type_SD"], "enrichment culture")
        benzene_culture_sample = ensure_managed_metadata_schema(
            {"Host": "", "Isolation Source": "benzene-degrading enrichment culture", "Sample Type": "culture"}
        )
        self.assertEqual(benzene_culture_sample["Isolation_Source_SD"], "")
        self.assertEqual(benzene_culture_sample["Sample_Type_SD"], "culture")

        dental = standardized("dental biofilm")
        self.assertEqual(dental["Environment_Medium_SD"], "biofilm")
        self.assertEqual(dental["Isolation_Site_SD"], "oral cavity")

        for source in ["canal", "drainage", "surface", "influent", "mud", "sand"]:
            result = standardized(source)
            self.assertNotEqual(result["Isolation_Source_SD"], "environmental material", source)

    def test_batch5_food_commodity_context_and_safeguards(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source in [
            "chicken meat",
            "poultry meat/product",
            "ground turkey",
            "turkey meat",
            "beef",
            "pork",
            "retail meat",
            "sausage",
        ]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "food/meat", source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
            self.assertNotEqual(result["Environment_Medium_SD"], "food/meat", source)
        self.assertIsNone(extract_country("ground turkey"))

        for source in ["seafood", "fish meat", "oyster meat", "shrimp seafood"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "aquatic food product", source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
        for source in ["oyster", "fish", "shellfish", "shrimp"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)

        milk = standardized("milk")
        self.assertEqual(milk["Sample_Type_SD"], "milk")
        self.assertEqual(milk["Isolation_Source_SD"], "")
        raw_milk = standardized("raw milk")
        self.assertEqual(raw_milk["Sample_Type_SD"], "milk")
        self.assertEqual(raw_milk["Environment_Medium_SD"], "")
        for source in ["dairy product", "cheese", "yogurt"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "food/dairy", source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        egg_product = standardized("egg product")
        self.assertEqual(egg_product["Isolation_Source_SD"], "egg product")
        self.assertEqual(egg_product["Isolation_Source_SD_Broad"], "food")
        egg = standardized("egg")
        self.assertEqual(egg["Isolation_Source_SD"], "")
        self.assertEqual(egg["Sample_Type_SD"], "")

        for source in ["lettuce produce", "spinach produce", "tomato food product", "salad"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "food", source)
        lettuce_plant = standardized("lettuce plant")
        self.assertEqual(lettuce_plant["Isolation_Source_SD"], "plant-associated material")
        self.assertNotEqual(lettuce_plant["Isolation_Source_SD_Broad"], "food")
        for source in ["lettuce", "spinach", "tomato", "vegetable"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)

        for source in ["chicken", "turkey", "cattle", "cow", "swine", "pig"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "", source)

        for source in ["chicken breast meat", "pork liver", "beef heart"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD_Broad"], "food/meat", source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
        for source in ["fish gut", "oyster tissue"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Site_SD"], "", source)
            self.assertNotEqual(result["Isolation_Source_SD"], "clinical/host-associated material", source)

        slaughterhouse = standardized("slaughterhouse")
        self.assertEqual(slaughterhouse["Isolation_Source_SD_Broad"], "food/processing environment")
        abattoir = standardized("abattoir")
        self.assertEqual(abattoir["Isolation_Source_SD_Broad"], "food/processing environment")
        poultry_farm = standardized("poultry farm")
        self.assertEqual(poultry_farm["Isolation_Source_SD"], "agricultural environment")
        for source in ["farm", "market"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)

        for source in ["blood", "feces", "urine"]:
            result = standardized(source)
            self.assertTrue(result["Sample_Type_SD"], source)
            self.assertNotEqual(result["Isolation_Source_SD_Broad"], "food/meat", source)
        self.assertEqual(standardized("wound")["Isolation_Site_SD"], "wound")
        self.assertEqual(standardized("canal water")["Environment_Medium_SD"], "canal water")
        self.assertEqual(standardized("ear canal")["Isolation_Site_SD"], "organ/tissue site")
        self.assertEqual(standardized("clinical sample")["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(standardized("respiratory sample")["Isolation_Site_SD"], "respiratory tract")
        self.assertEqual(standardized("metagenome")["Isolation_Source_SD"], "")

    def test_batch6_host_only_animal_plant_context(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source, context in [
            ("human", "human-associated"),
            ("patient", "human-associated"),
            ("human patient", "human-associated"),
            ("animal", "animal-associated"),
            ("livestock", "livestock/cattle"),
            ("calf", "livestock/cattle"),
            ("horse", "livestock/horse"),
            ("cat", "pet/companion animal"),
            ("insect", "arthropod"),
            ("tick", "arthropod"),
            ("bird", "bird"),
            ("wild bird", "bird"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Host_Context_SD"], context, source)
            expected_source = "clinical/host-associated material" if source == "patient" else ""
            self.assertEqual(result["Isolation_Source_SD"], expected_source, source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        clinical_patient = standardized("clinical patient")
        self.assertEqual(clinical_patient["Host_Context_SD"], "human-associated")
        self.assertEqual(clinical_patient["Isolation_Source_SD_Broad"], "clinical/host-associated material")
        self.assertEqual(clinical_patient["Environment_Medium_SD"], "")

        for source, context in [
            ("chicken feces", "poultry"),
            ("cattle feces", "livestock/cattle"),
            ("swine feces", "livestock/swine"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Host_Context_SD"], context, source)
            self.assertEqual(result["Sample_Type_SD"], "feces/stool", source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)

        cow_milk = standardized("cow milk")
        self.assertEqual(cow_milk["Host_Context_SD"], "livestock/cattle")
        self.assertEqual(cow_milk["Sample_Type_SD"], "milk")
        self.assertEqual(cow_milk["Isolation_Source_SD"], "")

        poultry_litter = standardized("poultry litter")
        self.assertEqual(poultry_litter["Host_Context_SD"], "poultry")
        self.assertEqual(poultry_litter["Environment_Medium_SD"], "agricultural organic material")
        self.assertNotEqual(poultry_litter["Isolation_Source_SD_Broad"], "food/meat")
        animal_bedding = standardized("animal bedding")
        self.assertEqual(animal_bedding["Host_Context_SD"], "animal-associated")
        self.assertEqual(animal_bedding["Environment_Medium_SD"], "agricultural organic material")

        for source in ["chicken meat", "turkey meat", "beef", "pork", "oyster meat", "fish meat"]:
            result = standardized(source)
            self.assertIn(result["Isolation_Source_SD_Broad"], {"food/meat", "aquatic food product"}, source)
            self.assertNotEqual(result["Host_Context_SD"], "poultry", source)

        for source in ["fish gut", "oyster tissue", "animal tissue"]:
            result = standardized(source)
            self.assertTrue(result["Host_Context_SD"], source)
            self.assertNotEqual(result["Isolation_Site_SD"], "gastrointestinal tract", source)
            self.assertNotEqual(result["Isolation_Source_SD"], "clinical/host-associated material", source)

        for source in ["plant", "leaf", "root", "stem", "seed", "flower", "phyllosphere", "endosphere"]:
            result = standardized(source)
            self.assertEqual(result["Host_Context_SD"], "plant-associated", source)
            self.assertEqual(result["Isolation_Source_SD"], "plant-associated material", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        for source, site in [
            ("leaf tissue", "leaf tissue"),
            ("root tissue", "root"),
            ("stem tissue", "plant stem"),
            ("plant tissue", ""),
        ]:
            result = standardized(source)
            self.assertEqual(result["Host_Context_SD"], "plant-associated", source)
            self.assertEqual(result["Sample_Type_SD"], "plant tissue", source)
            self.assertEqual(result["Isolation_Source_SD"], "plant-associated material", source)
            self.assertEqual(result["Isolation_Site_SD"], site, source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        leaf_surface = standardized("leaf surface")
        self.assertEqual(leaf_surface["Host_Context_SD"], "plant-associated")
        self.assertEqual(leaf_surface["Isolation_Source_SD"], "plant-associated material")
        self.assertEqual(leaf_surface["Isolation_Site_SD"], "leaf tissue")
        self.assertNotEqual(leaf_surface["Isolation_Site_SD"], "skin/body surface")

        rhizosphere_soil = standardized("rhizosphere soil")
        self.assertEqual(rhizosphere_soil["Host_Context_SD"], "plant-associated")
        self.assertEqual(rhizosphere_soil["Environment_Medium_SD"], "soil")
        self.assertEqual(rhizosphere_soil["Sample_Type_SD"], "")
        root_soil = standardized("root-associated soil")
        self.assertEqual(root_soil["Host_Context_SD"], "plant-associated")
        self.assertEqual(root_soil["Environment_Medium_SD"], "soil")
        self.assertEqual(root_soil["Sample_Type_SD"], "")

        for source in ["lettuce", "spinach", "tomato", "vegetable"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
        self.assertEqual(standardized("lettuce plant")["Host_Context_SD"], "plant-associated")
        self.assertEqual(standardized("lettuce produce")["Isolation_Source_SD_Broad"], "food")
        self.assertEqual(standardized("fresh-cut lettuce")["Isolation_Source_SD_Broad"], "food")

        poultry_farm = standardized("poultry farm")
        self.assertEqual(poultry_farm["Host_Context_SD"], "poultry")
        self.assertEqual(poultry_farm["Isolation_Source_SD"], "agricultural environment")
        self.assertEqual(poultry_farm["Sample_Type_SD"], "")
        for source in ["dairy farm", "farm environment", "livestock market"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "agricultural environment", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)
        for source in ["farm", "market"]:
            result = standardized(source)
            self.assertEqual(result["Isolation_Source_SD"], "", source)
            self.assertEqual(result["Sample_Type_SD"], "", source)

        self.assertEqual(standardized("metagenome")["Isolation_Source_SD"], "")
        self.assertEqual(standardized("blood")["Sample_Type_SD"], "blood")
        self.assertEqual(standardized("wound")["Isolation_Site_SD"], "wound")
        self.assertEqual(standardized("canal water")["Environment_Medium_SD"], "canal water")
        self.assertEqual(standardized("cheese")["Isolation_Source_SD_Broad"], "food/dairy")

    def test_batch7_disease_health_source_context(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        for source, disease in [
            ("diarrhea", "diarrheal disease"),
            ("diarrhoea", "diarrheal disease"),
            ("diarrheal", "diarrheal disease"),
            ("pneumonia", "pneumonia"),
            ("sepsis", "sepsis/bacteremia"),
            ("septicemia", "sepsis/bacteremia"),
            ("bacteremia", "sepsis/bacteremia"),
            ("bacteraemia", "sepsis/bacteremia"),
            ("urinary tract infection", "urinary tract infection"),
            ("UTI", "urinary tract infection"),
            ("mastitis", "mastitis"),
            ("meningitis", "meningitis"),
            ("gastroenteritis", "gastroenteritis"),
            ("cystic fibrosis", "cystic fibrosis"),
            ("wound infection", "wound infection"),
            ("skin infection", "skin infection"),
            ("respiratory infection", "respiratory infection"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Host_Disease_SD"], disease, source)
            self.assertNotEqual(result["Isolation_Source_SD"], source, source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        wound_infection = standardized("wound infection")
        self.assertEqual(wound_infection["Isolation_Site_SD"], "wound")
        skin_infection = standardized("skin infection")
        self.assertEqual(skin_infection["Isolation_Site_SD"], "skin/body surface")
        respiratory_infection = standardized("respiratory infection")
        self.assertEqual(respiratory_infection["Isolation_Site_SD"], "respiratory tract")

        for source, health_state in [
            ("healthy", "healthy"),
            ("healthy control", "healthy"),
            ("asymptomatic", "asymptomatic"),
            ("symptomatic", "symptomatic"),
            ("diseased", "diseased"),
            ("control", "healthy/control"),
            ("normal", "healthy"),
        ]:
            result = standardized(source)
            self.assertEqual(result["Host_Health_State_SD"], health_state, source)
            self.assertNotEqual(result["Isolation_Source_SD"], source, source)
            self.assertEqual(result["Environment_Medium_SD"], "", source)

        diarrheal_stool = standardized("diarrheal stool")
        self.assertEqual(diarrheal_stool["Sample_Type_SD"], "feces/stool")
        self.assertEqual(diarrheal_stool["Host_Disease_SD"], "diarrheal disease")
        urine_uti = standardized("urine from UTI")
        self.assertEqual(urine_uti["Sample_Type_SD"], "urine")
        self.assertEqual(urine_uti["Host_Disease_SD"], "urinary tract infection")
        blood_sepsis = standardized("blood from sepsis")
        self.assertEqual(blood_sepsis["Sample_Type_SD"], "blood")
        self.assertEqual(blood_sepsis["Host_Disease_SD"], "sepsis/bacteremia")
        mastitis_milk = standardized("mastitis milk")
        self.assertEqual(mastitis_milk["Sample_Type_SD"], "milk")
        self.assertEqual(mastitis_milk["Host_Disease_SD"], "mastitis")
        self.assertEqual(mastitis_milk["Isolation_Source_SD"], "")
        abscess_aspirate = standardized("abscess aspirate")
        self.assertIn(abscess_aspirate["Sample_Type_SD"], {"aspirate", "clinical fluid/material"})
        self.assertNotEqual(abscess_aspirate["Environment_Medium_SD"], "abscess aspirate")

        for source in ["clinical sample", "clinical patient", "patient sample", "respiratory sample"]:
            result = standardized(source)
            self.assertEqual(result["Host_Disease_SD"], "", source)
            self.assertNotEqual(result["Environment_Medium_SD"], source, source)
        self.assertEqual(standardized("clinical sample")["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(standardized("respiratory sample")["Isolation_Site_SD"], "respiratory tract")

        for source in [
            "infected animal",
            "diseased plant",
            "contaminated food",
            "outbreak food source",
            "wastewater surveillance",
            "hospital environment",
        ]:
            result = standardized(source)
            self.assertNotEqual(result["Host_Disease_SD"], source, source)
        self.assertEqual(standardized("contaminated food")["Isolation_Source_SD_Broad"], "food")
        self.assertTrue(standardized("wastewater surveillance")["Environment_Medium_SD"])
        self.assertEqual(standardized("hospital environment")["Isolation_Source_SD_Broad"], "healthcare-associated environment")

        self.assertEqual(standardized("metagenome")["Isolation_Source_SD"], "")
        self.assertEqual(standardized("feces")["Sample_Type_SD"], "feces/stool")
        self.assertEqual(standardized("canal water")["Environment_Medium_SD"], "canal water")
        self.assertEqual(standardized("chicken meat")["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(standardized("human")["Host_Context_SD"], "human-associated")

    def test_batch8_admin_review_values_are_resolved_conservatively(self) -> None:
        def standardized(source: str) -> dict[str, str]:
            return ensure_managed_metadata_schema(
                {"Host": "", "Isolation Source": source, "Sample Type": "metagenomic assembly"}
            )

        infection = standardized("infection")
        self.assertEqual(infection["Host_Disease_SD"], "infectious disease")
        self.assertNotIn(infection["Host_Disease_SD"], {"pneumonia", "sepsis/bacteremia", "urinary tract infection"})
        self.assertEqual(infection["Host_Health_State_SD"], "diseased")
        self.assertEqual(infection["Isolation_Source_SD"], "clinical/host-associated material")

        infected_animal = standardized("infected animal")
        self.assertEqual(infected_animal["Host_Context_SD"], "animal-associated")
        self.assertEqual(infected_animal["Host_Health_State_SD"], "diseased")
        self.assertEqual(infected_animal["Host_Disease_SD"], "infectious disease")
        self.assertEqual(infected_animal["Isolation_Source_SD"], "host-associated context")

        diseased_plant = standardized("diseased plant")
        self.assertEqual(diseased_plant["Host_Context_SD"], "plant-associated")
        self.assertEqual(diseased_plant["Host_Health_State_SD"], "diseased")
        self.assertEqual(diseased_plant["Host_Disease_SD"], "plant disease, unspecified")
        self.assertEqual(diseased_plant["Isolation_Source_SD"], "plant-associated material")

        outbreak = standardized("outbreak")
        self.assertEqual(outbreak["Host_Disease_SD"], "")
        self.assertEqual(outbreak["Host_Health_State_SD"], "")
        self.assertEqual(outbreak["Isolation_Source_SD"], "")

        outbreak_food = standardized("outbreak food source")
        self.assertEqual(outbreak_food["Host_Disease_SD"], "")
        self.assertEqual(outbreak_food["Isolation_Source_SD_Broad"], "food")
        self.assertEqual(outbreak_food["Isolation_Source_SD"], "food/food product")

        contaminated_food = standardized("contaminated food")
        self.assertEqual(contaminated_food["Host_Disease_SD"], "")
        self.assertEqual(contaminated_food["Isolation_Source_SD_Broad"], "food")
        self.assertEqual(contaminated_food["Isolation_Source_SD"], "food/food product")

        wastewater_surveillance = standardized("wastewater surveillance")
        self.assertEqual(wastewater_surveillance["Host_Disease_SD"], "")
        self.assertEqual(wastewater_surveillance["Environment_Medium_SD"], "wastewater")
        self.assertEqual(wastewater_surveillance["Isolation_Source_SD"], "environmental material")

        carrier = standardized("carrier")
        self.assertEqual(carrier["Host_Disease_SD"], "")
        self.assertEqual(carrier["Host_Health_State_SD"], "")
        self.assertEqual(carrier.get("Host_Colonization_Status_SD", ""), "")
        self.assertEqual(carrier["Isolation_Source_SD"], "clinical/host-associated material")

        colonized = standardized("colonized")
        self.assertEqual(colonized["Host_Disease_SD"], "")
        self.assertEqual(colonized["Host_Health_State_SD"], "")
        self.assertEqual(colonized.get("Host_Colonization_Status_SD", ""), "")
        self.assertEqual(colonized["Isolation_Source_SD"], "clinical/host-associated material")

        clinical = standardized("clinical")
        self.assertEqual(clinical["Host_Disease_SD"], "")
        self.assertEqual(clinical["Host_Health_State_SD"], "")
        self.assertEqual(clinical["Isolation_Source_SD"], "clinical/host-associated material")

        patient = standardized("patient")
        self.assertEqual(patient["Host_Disease_SD"], "")
        self.assertEqual(patient["Host_Health_State_SD"], "")
        self.assertEqual(patient["Host_Context_SD"], "human-associated")
        self.assertEqual(patient["Isolation_Source_SD"], "clinical/host-associated material")

    def test_food_cut_terms_are_not_clinical_anatomy(self) -> None:
        retail_breast = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Retail Breast"})
        self.assertEqual(retail_breast["Sample_Type_SD"], "poultry meat")
        self.assertEqual(retail_breast["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(retail_breast["Isolation_Site_SD"], "")

        turkey_sandwich = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "turkey breast sandwich"})
        self.assertEqual(turkey_sandwich["Sample_Type_SD"], "poultry meat")
        self.assertEqual(turkey_sandwich["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(turkey_sandwich["Isolation_Site_SD"], "")

        ground_breast = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Ground (breast)"})
        self.assertEqual(ground_breast["Sample_Type_SD"], "poultry meat")
        self.assertEqual(ground_breast["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(ground_breast["Isolation_Site_SD"], "")

        human_breast_milk = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human breast milk"})
        self.assertEqual(human_breast_milk["Host_SD"], "Homo sapiens")
        self.assertEqual(human_breast_milk["Sample_Type_SD"], "milk")
        self.assertEqual(human_breast_milk["Host_Anatomical_Site_SD"], "breast")

    def test_disease_and_lab_artifacts_do_not_leak_as_source(self) -> None:
        osteomyelitis = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Osteomyelitis"})
        self.assertEqual(osteomyelitis["Host_Disease_SD"], "osteomyelitis")
        self.assertEqual(osteomyelitis["Host_Health_State_SD"], "diseased")
        self.assertEqual(osteomyelitis["Isolation_Source_SD"], "clinical/host-associated material")

        aborted = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "aborted uteroplacental unit"})
        self.assertEqual(aborted["Host_Disease_SD"], "abortion/reproductive disorder")
        self.assertEqual(aborted["Host_Health_State_SD"], "diseased")
        self.assertEqual(aborted["Isolation_Source_SD"], "clinical/host-associated material")

        leukemia = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Leukemia cell line (SEM)"})
        self.assertEqual(leukemia["Host_Disease_SD"], "leukemia")
        self.assertEqual(leukemia["Isolation_Source_SD"], "clinical/host-associated material")

        derived_strain = ensure_managed_metadata_schema(
            {"Host": "", "Isolation Source": "derived from the strain Pseudomonas aeruginosa ATCC 27853"}
        )
        self.assertEqual(derived_strain["Isolation_Source_SD"], "culture")
        self.assertEqual(derived_strain["Host_SD"], "")

        spreadsheet_error = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "#REF!"})
        self.assertEqual(spreadsheet_error["Isolation_Source_SD"], "")
        self.assertEqual(spreadsheet_error["Sample_Type_SD"], "")

        facility = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Facility 4"})
        self.assertEqual(facility["Isolation_Source_SD"], "healthcare-associated environment")

        raw_code = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "cxwnd"})
        self.assertEqual(raw_code["Isolation_Source_SD"], "")

        for descriptor in [
            "metagenome",
            "sample",
            "specimen",
            "uncategorized",
            "metadata descriptor/non-source",
            "pathogen.cl",
            "other",
        ]:
            standardized = ensure_managed_metadata_schema({"Host": "", "Isolation Source": descriptor})
            self.assertEqual(standardized["Isolation_Source_SD"], "")
            self.assertEqual(standardized["Sample_Type_SD"], "")
            self.assertEqual(standardized["Environment_Medium_SD"], "")
            self.assertEqual(standardized["Isolation_Site_SD"], "")

        for missing in ["unknown", "missing", "not applicable", "none"]:
            standardized = ensure_managed_metadata_schema({"Host": "", "Isolation Source": missing})
            self.assertEqual(standardized["Isolation_Source_SD"], "")
            self.assertEqual(standardized["Sample_Type_SD"], "")

        clinical = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "clinical sample"})
        self.assertEqual(clinical["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(clinical["Sample_Type_SD"], "clinical sample")

        respiratory = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "respiratory sample"})
        self.assertEqual(respiratory["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(respiratory["Sample_Type_SD"], "respiratory sample")
        self.assertEqual(respiratory["Isolation_Site_SD"], "respiratory tract")

        environmental = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "environmental sample"})
        self.assertEqual(environmental["Isolation_Source_SD"], "environmental material")
        self.assertTrue(environmental["Environment_Medium_SD"])

        ontology_code = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "ENVO_00005801"})
        self.assertEqual(ontology_code["Isolation_Source_SD"], "ENVO_00005801")
        self.assertEqual(ontology_code["Isolation_Source_SD_Method"], "standardizer")

        culture = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "soil enrichment culture"})
        self.assertEqual(culture["Environment_Medium_SD"], "soil")
        self.assertTrue(culture["Sample_Type_SD"])
        self.assertEqual(culture["Isolation_Source_SD"], "environmental material")

        whole_organism = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "whole organism"})
        self.assertEqual(whole_organism["Isolation_Source_SD"], "host-associated context")

    def test_source_sample_environment_consolidation_gate_is_manual(self) -> None:
        from tools import check_deployment_readiness_gate as gate_tool

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            gate = gate_tool.deployment_gate(
                root,
                {"duplicate_keys": 0, "conflict_keys": 0, "hard_rule_leakage": 0},
                {
                    "status": "pass",
                    "hard_failures": [],
                    "metrics": {
                        "hard_exact_leakage_rows": 0,
                        "non_approved_broad_rows": 0,
                        "controlled_category_duplicate_keys": 0,
                        "controlled_category_conflict_keys": 0,
                    },
                },
                {"pass": True, "global_insights_snapshot_sha256": "abc123", "snapshot_id": "test_snapshot"},
                tests_passed=True,
            )
            self.assertTrue(gate["canonical_refresh_pass"])
            self.assertTrue(gate["global_insights_regenerated"])
            self.assertEqual(gate["hard_failures"], [])
            self.assertFalse(gate["safe_to_deploy"])
            self.assertEqual(gate["reason"], "deployment intentionally manual")
            self.assertTrue((root / "deployment_readiness_gate.json").exists())

    def test_source_sample_environment_consolidation_gate_blocks_missing_outputs(self) -> None:
        from tools import check_deployment_readiness_gate as gate_tool

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            gate = gate_tool.deployment_gate(
                root,
                {"duplicate_keys": 0, "conflict_keys": 0, "hard_rule_leakage": 0},
                {
                    "status": "fail",
                    "hard_failures": ["hard exact-source leakage: 2"],
                    "metrics": {
                        "hard_exact_leakage_rows": 2,
                        "non_approved_broad_rows": 0,
                        "controlled_category_duplicate_keys": 0,
                        "controlled_category_conflict_keys": 0,
                    },
                },
                {},
                tests_passed=False,
            )
            self.assertFalse(gate["canonical_refresh_pass"])
            self.assertFalse(gate["global_insights_regenerated"])
            self.assertIn("tests failed or not recorded", gate["hard_failures"])
            self.assertIn("Global Insights regeneration missing or failed", gate["hard_failures"])
            self.assertIn("hard leakage after refresh > 0", gate["hard_failures"])
            self.assertFalse(gate["safe_to_deploy"])

    def test_controlled_category_consolidation_audit_detects_conflicts(self) -> None:
        from tools import check_deployment_readiness_gate as gate_tool

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rules = root / "controlled_categories.csv"
            broad = root / "approved_broad_categories.csv"
            rules.write_text(
                "synonym,source_column,original_value,normalized_value,destination,category,proposed_value,broad_value,ontology_id,method,confidence,status,note\n"
                "x,Isolation Source,x,x,Sample_Type_SD,blood,blood,clinical/host-associated material,,test,high,approved,\n"
                "x,Isolation Source,x,x,Sample_Type_SD,urine,urine,clinical/host-associated material,,test,high,approved,\n",
                encoding="utf-8",
            )
            broad.write_text("field,approved_value\nIsolation_Source_SD_Broad,clinical/host-associated material\n", encoding="utf-8")
            with patch.object(gate_tool, "CONTROLLED_CATEGORIES", rules), patch.object(gate_tool, "APPROVED_BROAD_CATEGORIES", broad):
                metrics = gate_tool.controlled_category_audit(root)
            self.assertEqual(metrics["conflict_keys"], 1)
            self.assertEqual(metrics["duplicate_keys"], 0)
            self.assertTrue((root / "controlled_category_conflict_keys.tsv").exists())

    def test_external_nextflow_qc_master_imports_as_canonical_qc_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.csv"
            output_dir = root / "outputs"
            qc_dir = output_dir / "sequence_qc"
            master_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "qc"
            ani_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "ani" / "analysis"
            mash_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "mash" / "analysis"
            gtdbtk_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "gtdbtk"
            qc_dir.mkdir(parents=True)
            master_dir.mkdir(parents=True)
            ani_dir.mkdir(parents=True)
            mash_dir.mkdir(parents=True)
            gtdbtk_dir.mkdir(parents=True)

            input_path.write_text(
                "\n".join(
                    [
                        "Assembly Accession,Assembly Name,Organism Name",
                        "GCF_000001.1,ASM1,Klebsiella pneumoniae",
                        "GCF_000002.1,ASM2,Klebsiella pneumoniae",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (master_dir / "qc_master_report.csv").write_text(
                "\n".join(
                    [
                        "Assembly Accession,Assembly Name,sequence_file,sequence_total_length,sequence_num_contigs,sequence_n50,sequence_gc_percent,sequence_ambiguous_bases,checkm2_completeness,checkm2_contamination,ani_closest_ani,ani_species_consistency_status,ani_cluster,gtdbtk_qc_status,gtdbtk_genus,gtdbtk_species,gtdbtk_qc_fail_reasons,qc_master_status,qc_master_fail_reasons,qc_master_warning_reasons",
                        "GCF_000001.1,ASM1,GCF_000001.1_ASM1_genomic.fna,5200000,81,120000,57.3,0,98.4,0.8,99.98,PASS,ANI_CLUSTER_0001,PASS,Klebsiella,Klebsiella pneumoniae,,PASS,,",
                        "GCF_000002.1,ASM2,GCF_000002.1_ASM2_genomic.fna,4100000,300,5000,56.9,10,72.0,8.5,0.0,WARN,ANI_CLUSTER_0002,FAIL,Enterobacter,Enterobacter cloacae,GENUS_MISMATCH,FAIL,CheckM2 completeness below threshold,ani_species_warning:0<95",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (ani_dir / "panr2_ani_summary.csv").write_text(
                "\n".join(
                    [
                        "sample_id,assembly_accession,database,feature_id,feature_category,presence,tool",
                        "GCF_000001.1_ASM1,GCF_000001.1_ASM1,ani,ANI_CLUSTER_0001,ani_cluster,1,skani",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (ani_dir / "ani_run_status.tsv").write_text(
                "tool\tgenome_count\testimated_comparisons\tstrategy\tstatus\tmessage\n"
                "skani\t2\t4\tauto\tPASS\tRunning all-vs-all ANI.\n",
                encoding="utf-8",
            )
            (mash_dir / "closest_mash_neighbor.csv").write_text(
                "\n".join(
                    [
                        "query,reference,mash_distance,p_value,matching_hashes",
                        "GCF_000001.1_ASM1_genomic,GCF_000002.1_ASM2_genomic,0.001,0,950/1000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (mash_dir / "mash_distance_long.csv").write_text(
                "\n".join(
                    [
                        "query,reference,mash_distance,p_value,matching_hashes",
                        "GCF_000001.1_ASM1_genomic,GCF_000001.1_ASM1_genomic,0,0,1000/1000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (gtdbtk_dir / "gtdbtk.bac120.summary.tsv").write_text(
                "user_genome\tclassification\n"
                "GCF_000001.1_ASM1_genomic\td__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Klebsiella;s__Klebsiella pneumoniae\n",
                encoding="utf-8",
            )

            result = import_nextflow_qc_outputs(input_path, output_dir, qc_dir)
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["pass"], 1)
            self.assertEqual(result["fail"], 1)
            self.assertTrue((qc_dir / "external_qc_master_report.csv").exists())
            self.assertTrue((qc_dir / "qc_all_metadata.csv").exists())
            self.assertTrue((qc_dir / "external_ani_summary.csv").exists())
            self.assertTrue((qc_dir / "external_ani_run_status.tsv").exists())
            self.assertTrue((qc_dir / "external_mash_closest_neighbors.csv").exists())
            self.assertTrue((qc_dir / "external_mash_distance_long.csv").exists())
            self.assertTrue((qc_dir / "external_gtdbtk_bac120_summary.tsv").exists())
            decisions = (qc_dir / "qc_decisions.csv").read_text(encoding="utf-8")
            self.assertIn("ANI_Closest_ANI", decisions)
            self.assertIn("Mash_Distance", decisions)
            self.assertIn("GTDBTK_QC_Status", decisions)
            self.assertIn("Klebsiella pneumoniae", decisions)
            self.assertIn("0.001", decisions)
            self.assertIn("NO_VALID_ANI_RESULT", decisions)
            self.assertNotIn("ani_species_warning:0<95", decisions)
            self.assertIn("GCF_000001.1", (qc_dir / "qc_pass_metadata.csv").read_text(encoding="utf-8"))
            self.assertIn("CheckM2 completeness below threshold", (qc_dir / "qc_failed_metadata.csv").read_text(encoding="utf-8"))

    def test_gtdbtk_selection_requires_reference_data(self) -> None:
        errors = validate_quality_runtime(
            {"run_mode": "nextflow", "selected_modules": ["quick_fasta", "gtdbtk"]},
            {
                "nextflow_enabled": True,
                "nextflow_available": True,
                "conda_available": True,
                "nextflow_config_exists": True,
                "nextflow_workflow_exists": True,
                "available_tools": {"gtdbtk": False},
                "gtdbtk_data_path_exists": False,
                "gtdbtk_data_ready": False,
                "nextflow_managed_tools": {"gtdbtk": False},
            },
        )
        self.assertTrue(any("GTDB-Tk" in error for error in errors))

    def test_public_auth_pages_use_refreshed_fetchm_web_copy(self) -> None:
        with isolated_initialized_app_client() as client:
            health = client.get("/healthz")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.get_json()["status"], "ok")

            home = client.get("/")
            self.assertEqual(home.status_code, 200)
            home_html = home.data.decode("utf-8")
            self.assertIn("Select your target species or genus", home_html)
            self.assertIn("FetchM automatically standardizes genome metadata", home_html)
            self.assertIn("10.1093/bioadv/vbag124", home_html)
            self.assertIn("Examples</span>", home_html)
            self.assertIn("Browse taxa", home_html)
            self.assertNotIn("Start with the managed catalog", home_html)
            self.assertNotIn("Standardized metadata</strong>", home_html)

            for path, phrase in [
                ("/about", "Resource overview"),
                ("/help", "How to use FetchM"),
                ("/tutorial", "Worked examples"),
                ("/browse", "Browse prepared bacterial taxa"),
                ("/downloads", "Bulk downloads"),
                ("/api", "Programmatic Access"),
                ("/citation", "How to cite"),
                ("/nar-readiness", "Submission checklist"),
                ("/status", "Service status"),
            ]:
                response = client.get(path)
                self.assertEqual(response.status_code, 200, path)
                self.assertIn(phrase, response.data.decode("utf-8"), path)

            with client.session_transaction() as session:
                session["_csrf_token"] = "token"
            with patch.object(fetchm_app, "create_problem_report", return_value=1) as create_report, patch.object(fetchm_app, "mail_is_configured", return_value=False):
                report = client.post(
                    "/report-problem",
                    data={"_csrf_token": "token", "message": "This public feedback route should not require sign in."},
                    follow_redirects=False,
                )
            self.assertEqual(report.status_code, 302)
            self.assertNotIn("/login", report.headers.get("Location", ""))
            create_report.assert_called_once()
            self.assertIsNone(create_report.call_args.kwargs["user_id"])
            self.assertEqual(create_report.call_args.kwargs["username"], "anonymous")

            login = client.get("/login")
            self.assertEqual(login.status_code, 200)
            login_html = login.data.decode("utf-8")
            self.assertIn("FetchM Web", login_html)
            self.assertIn("Run metadata analyses, launch sequence downloads", login_html)
            self.assertNotIn("FetckM", login_html)
            self.assertNotIn("FetchM WEB", login_html)
            self.assertIn("Create an account", login_html)
            self.assertIn("Forgot password?", login_html)

            register = client.get("/register")
            self.assertEqual(register.status_code, 200)
            register_html = register.data.decode("utf-8")
            self.assertIn("Register for FetchM Web", register_html)
            self.assertIn("private workspace", register_html)

            forgot = client.get("/forgot-password")
            self.assertEqual(forgot.status_code, 200)
            self.assertIn("Forgot your password?", forgot.data.decode("utf-8"))

    def test_theme_assets_include_design_tokens_and_cache_bust(self) -> None:
        css = Path(fetchm_app.app.root_path, "static", "styles.css").read_text(encoding="utf-8")
        self.assertIn("--accent-bright", css)
        self.assertIn("--radius-xl", css)
        self.assertIn(".auth-unified-panel::after", css)
        self.assertIn("Aptos Display", css)

        base = Path(fetchm_app.app.root_path, "templates", "base.html").read_text(encoding="utf-8")
        self.assertRegex(base, r"url_for\('static', filename='styles\.css', v='[^']+'\)")
        self.assertIn(">FetchM Web<", base)

    def test_sequence_pages_require_login_but_metadata_routes_are_public(self) -> None:
        with isolated_initialized_app_client() as client:
            self.assertEqual(client.get("/api/taxa/search?q=Klebsiella").status_code, 200)
            sequence = client.get("/taxa/1/sequences", follow_redirects=False)
            self.assertEqual(sequence.status_code, 302)
            self.assertIn("/login", sequence.headers.get("Location", ""))

    def test_external_profile_without_javascript_does_not_fall_back_to_quick_mode(self) -> None:
        class Form:
            def get(self, key: str, default=None):
                values = {"quality_profile": "standard", "quality_run_mode": "quick"}
                return values.get(key, default)

            def getlist(self, key: str):
                if key == "quality_module":
                    return ["quick_fasta"]
                return []

        config = build_quality_config(Form())
        self.assertEqual(config["run_mode"], "handoff")
        self.assertIn("checkm2", config["selected_modules"])
        self.assertIn("quast", config["selected_modules"])

    def test_quality_profiles_use_profile_specific_threshold_defaults(self) -> None:
        class Form:
            def __init__(self, values: dict[str, str]):
                self.values = values

            def get(self, key: str, default=None):
                return self.values.get(key, default)

            def getlist(self, key: str):
                return []

        quick = build_quality_config(Form({"quality_profile": "quick"}))
        self.assertEqual(quick["run_mode"], "quick")
        self.assertEqual(quick["selected_modules"], ["quick_fasta"])
        self.assertIsNone(quick["thresholds"]["min_completeness"])
        self.assertIsNone(quick["thresholds"]["max_contamination"])
        self.assertIsNone(quick["thresholds"]["min_ani_percent"])
        self.assertEqual(quick["thresholds"]["max_n_percent"], 5.0)

        quick_with_checkm = build_quality_config(
            Form(
                {
                    "quality_profile": "quick",
                    "qc_use_existing_checkm": "1",
                    "qc_min_completeness": "85",
                    "qc_max_contamination": "10",
                }
            )
        )
        self.assertTrue(quick_with_checkm["use_existing_checkm"])
        self.assertEqual(quick_with_checkm["thresholds"]["min_completeness"], 85.0)
        self.assertEqual(quick_with_checkm["thresholds"]["max_contamination"], 10.0)

        standard = build_quality_config(Form({"quality_profile": "standard"}))
        self.assertEqual(standard["run_mode"], "handoff")
        self.assertEqual(standard["thresholds"]["min_completeness"], 90.0)
        self.assertEqual(standard["thresholds"]["max_contamination"], 5.0)
        self.assertIsNone(standard["thresholds"]["min_ani_percent"])
        self.assertIn("checkm2", standard["selected_modules"])
        self.assertIn("quast", standard["selected_modules"])

        advanced = build_quality_config(Form({"quality_profile": "advanced"}))
        self.assertEqual(advanced["thresholds"]["min_ani_percent"], 95.0)
        self.assertIn("ani", advanced["selected_modules"])
        self.assertNotIn("checkm2", advanced["selected_modules"])
        self.assertNotIn("quast", advanced["selected_modules"])
        self.assertNotIn("mash", advanced["selected_modules"])
        self.assertNotIn("gtdbtk", advanced["selected_modules"])

    def test_quick_qc_missing_checkm_only_matters_when_thresholds_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.csv"
            output_dir = root / "outputs"
            output_dir.mkdir()
            input_path.write_text(
                "Assembly Accession,Assembly Name,CheckM completeness,CheckM contamination\n"
                "GCA_000001.1,ASM1,,\n",
                encoding="utf-8",
            )
            (output_dir / "GCA_000001.1_ASM1_genomic.fna").write_text(">contig1\nACGTACGTACGT\n", encoding="utf-8")
            job = fetchm_app.JobRecord(
                id="quick-checkm",
                mode="qc",
                status="running",
                created_at=fetchm_app.utc_now(),
                updated_at=fetchm_app.utc_now(),
                input_name="input.csv",
                input_path=str(input_path),
                output_dir=str(output_dir),
                log_path=str(root / "job.log"),
                command=[],
                owner_user_id=1,
            )

            quick = build_quality_config(type("Form", (), {"get": lambda self, key, default=None: {"quality_profile": "quick"}.get(key, default), "getlist": lambda self, key: []})())
            run_sequence_quality_checks(job, quick["thresholds"], quick)
            decisions = fetchm_app.pd.read_csv(output_dir / "sequence_qc" / "qc_decisions.csv", dtype=str).fillna("")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Status"], "pass")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Review_Reasons"], "")

            strict = build_quality_config(
                type(
                    "Form",
                    (),
                    {
                        "get": lambda self, key, default=None: {
                            "quality_profile": "quick",
                            "qc_use_existing_checkm": "1",
                            "qc_min_completeness": "90",
                            "qc_max_contamination": "5",
                        }.get(key, default),
                        "getlist": lambda self, key: [],
                    },
                )()
            )
            run_sequence_quality_checks(job, strict["thresholds"], strict)
            decisions = fetchm_app.pd.read_csv(output_dir / "sequence_qc" / "qc_decisions.csv", dtype=str).fillna("")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Status"], "fail")
            self.assertIn("CheckM completeness missing", decisions.loc[0, "Sequence_QC_Failure_Reasons"])
            self.assertIn("CheckM contamination missing", decisions.loc[0, "Sequence_QC_Failure_Reasons"])

    def test_sequence_filter_and_or_logic(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {"Assembly Accession": "GCA_1", "Country": "India", "Host_SD": "Homo sapiens"},
                {"Assembly Accession": "GCA_2", "Country": "India", "Host_SD": "Sus scrofa"},
                {"Assembly Accession": "GCA_3", "Country": "Italy", "Host_SD": "Homo sapiens"},
            ]
        )
        filters = {"country": ["India"], "host_sd": ["Homo sapiens"], "filter_logic": "and"}
        self.assertEqual(apply_sequence_filters(frame, filters)["Assembly Accession"].tolist(), ["GCA_1"])
        filters["filter_logic"] = "or"
        self.assertEqual(
            apply_sequence_filters(frame, filters)["Assembly Accession"].tolist(),
            ["GCA_1", "GCA_2", "GCA_3"],
        )


    def test_sequence_download_subset_random_and_manual_modes(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {"Assembly Accession": "GCA_000004", "Country": "Bangladesh"},
                {"Assembly Accession": "GCA_000001", "Country": "Bangladesh"},
                {"Assembly Accession": "GCA_000003", "Country": "Bangladesh"},
                {"Assembly Accession": "GCF_000002", "Country": "Bangladesh"},
            ]
        )
        default = apply_sequence_download_subset(frame, {})
        self.assertEqual(default["selected_row_total"], 4)
        self.assertEqual(default["frame"]["Assembly Accession"].tolist(), ["GCA_000004", "GCA_000001", "GCA_000003", "GCF_000002"])

        random_one = apply_sequence_download_subset(frame, {"sequence_subset_mode": "random", "sequence_subset_count": "2", "sequence_subset_seed": "7"})
        random_two = apply_sequence_download_subset(frame.sample(frac=1, random_state=22), {"sequence_subset_mode": "random", "sequence_subset_count": "2", "sequence_subset_seed": "7"})
        random_three = apply_sequence_download_subset(frame, {"sequence_subset_mode": "random", "sequence_subset_count": "2", "sequence_subset_seed": "9"})
        self.assertEqual(random_one["selected_row_total"], 2)
        self.assertEqual(random_one["frame"]["Assembly Accession"].tolist(), random_two["frame"]["Assembly Accession"].tolist())
        self.assertNotEqual(random_one["frame"]["Assembly Accession"].tolist(), random_three["frame"]["Assembly Accession"].tolist())
        self.assertEqual(random_one["requested_count"], 2)
        self.assertEqual(random_one["random_seed"], "7")
        self.assertEqual(random_one["mode"], "random")

        overflow = apply_sequence_download_subset(frame, {"sequence_subset_mode": "random", "sequence_subset_count": "20", "sequence_subset_seed": "7"})
        self.assertEqual(overflow["selected_row_total"], 4)
        self.assertTrue(overflow["random_request_exceeds_matches"])

        manual = apply_sequence_download_subset(
            frame,
            {"sequence_subset_mode": "manual", "sequence_subset_accessions": "GCF_000002\nGCA_999999, GCA_000001 GCA_000001"},
        )
        self.assertEqual(manual["frame"]["Assembly Accession"].tolist(), ["GCF_000002", "GCA_000001"])
        self.assertEqual(manual["manual_accessions_missing"], ["GCA_999999"])
        self.assertEqual(manual["manual_duplicate_total"], 1)
        self.assertEqual(manual["manual_submitted_total"], 4)
        self.assertEqual(manual["requested_count"], 3)
        metadata = sequence_download_subset_metadata(manual)
        self.assertEqual(metadata["manual_missing_total"], 1)
        self.assertEqual(metadata["manual_duplicate_total"], 1)
        self.assertEqual(metadata["selected_accession_total"], 2)
        self.assertNotIn("selected_accessions", metadata)

    def test_sequence_download_subset_rejects_missing_subset_inputs(self) -> None:
        frame = fetchm_app.pd.DataFrame([{"Assembly Accession": "GCA_000001"}])
        for count in ["", "0", "-3"]:
            with self.subTest(count=count):
                random_missing = apply_sequence_download_subset(frame, {"sequence_subset_mode": "random", "sequence_subset_count": count})
                self.assertIn("positive number", random_missing["error"])
                self.assertTrue(random_missing["frame"].empty)

        invalid_seed = apply_sequence_download_subset(frame, {"sequence_subset_mode": "random", "sequence_subset_count": "1", "sequence_subset_seed": "demo"})
        self.assertIn("whole number", invalid_seed["error"])

        malformed = apply_sequence_download_subset(frame, {"sequence_subset_mode": "manual", "sequence_subset_accessions": "SAMN123 bad-token"})
        self.assertIn("malformed", malformed["error"])
        self.assertEqual(malformed["manual_submitted_total"], 2)
        self.assertEqual(malformed["manual_accessions_invalid"], ["SAMN123", "BAD-TOKEN"])

        manual_missing = apply_sequence_download_subset(frame, {"sequence_subset_mode": "manual", "sequence_subset_accessions": "GCA_999999"})
        self.assertIn("None of the manually selected", manual_missing["error"])
        self.assertTrue(manual_missing["frame"].empty)
        self.assertEqual(manual_missing["manual_accessions_missing"], ["GCA_999999"])

        with patch.object(fetchm_app, "MAX_SEQUENCE_SUBSET_ACCESSIONS", 1):
            too_many = apply_sequence_download_subset(frame, {"sequence_subset_mode": "manual", "sequence_subset_accessions": "GCA_000001 GCA_000002"})
        self.assertIn("more than", too_many["error"])

        with patch.object(fetchm_app, "MAX_SEQUENCE_SUBSET_TEXT_BYTES", 5):
            too_large = apply_sequence_download_subset(frame, {"sequence_subset_mode": "manual", "sequence_subset_accessions": "GCA_000001"})
        self.assertIn("too large", too_large["error"])

    def test_sequence_subset_job_routes_write_manifest_and_compact_metadata(self) -> None:
        def metadata_csv(path: Path) -> None:
            path.write_text(
                "Assembly Accession,Country,Host,Assembly Stats Total Sequence Length\n"
                "GCA_000001,Bangladesh,Homo sapiens,1000\n"
                "GCF_000002,Bangladesh,Homo sapiens,1000\n"
                "GCA_000003,India,Homo sapiens,1000\n",
                encoding="utf-8",
            )

        with isolated_initialized_app_client() as client:
            with fetchm_app.app.app_context():
                user = fetchm_app.create_user("subset-user", "subset@example.com", "long-password-1")
                clean_path = fetchm_app.DATA_DIR / "managed_metadata.csv"
                metadata_csv(clean_path)
                species = fetchm_app.create_species("Acinetobacter baumannii", taxon_rank="species")
                species.status = "ready"
                species.tsv_path = str(clean_path)
                species.metadata_status = "ready"
                species.metadata_path = str(clean_path)
                species.metadata_clean_path = str(clean_path)
                species.is_live = True
                species.live_status = "ready"
                species.live_tsv_path = str(clean_path)
                species.live_metadata_status = "ready"
                species.live_metadata_path = str(clean_path)
                species.live_metadata_clean_path = str(clean_path)
                species.live_genome_count = 3
                species = fetchm_app.save_species(species)
            with client.session_transaction() as session:
                session["user_id"] = int(user["id"])
                session["_csrf_token"] = "token"
            with patch.object(fetchm_app, "count_active_jobs_for_user", return_value=0), patch.object(
                fetchm_app, "build_command", return_value=(["fetchm", "seq"], {"check_only": False})
            ), patch.object(fetchm_app, "notify_job_event"), patch.object(fetchm_app, "record_audit_event"):
                response = client.post(
                    f"/taxa/{species.id}/sequences/jobs",
                    data={
                        "_csrf_token": "token",
                        "country": "Bangladesh",
                        "sequence_subset_mode": "manual",
                        "sequence_subset_accessions": "GCF_000002\nGCA_000003 GCA_000001 GCA_000001",
                    },
                    follow_redirects=False,
                )
            self.assertEqual(response.status_code, 302)
            with fetchm_app.app.app_context():
                row = fetchm_app.get_db().execute("SELECT id FROM jobs WHERE mode='seq'").fetchone()
                self.assertIsNotNone(row)
                job = fetchm_app.load_job(str(row["id"]))
            subset = job.filters["sequence_download_subset"]
            self.assertEqual(subset["matched_row_total"], 2)
            self.assertEqual(subset["selected_row_total"], 2)
            self.assertEqual(subset["manual_submitted_total"], 4)
            self.assertEqual(subset["manual_duplicate_total"], 1)
            self.assertEqual(subset["manual_missing_total"], 1)
            self.assertEqual(subset["selected_accession_total"], 2)
            self.assertNotIn("selected_accessions", subset)
            manifest_path = Path(job.input_path).parent.parent / subset["selected_accessions_manifest_path"]
            self.assertEqual(manifest_path.read_text(encoding="utf-8").splitlines(), ["GCF_000002", "GCA_000001"])
            self.assertEqual(hashlib.sha256(manifest_path.read_bytes()).hexdigest(), subset["selected_accessions_manifest_sha256"])
            self.assertEqual(job.filters["download_row_total"], 2)

    def test_canonical_sequence_subset_route_redirects_safely_and_writes_manifest(self) -> None:
        with isolated_initialized_app_client() as client:
            with fetchm_app.app.app_context():
                user = fetchm_app.create_user("canonical-subset", "canonical-subset@example.com", "long-password-1")
                clean_path = fetchm_app.DATA_DIR / "canonical_metadata.csv"
                clean_path.write_text(
                    "Assembly Accession,Country,Host,Assembly Stats Total Sequence Length\n"
                    "GCA_000001,Bangladesh,Homo sapiens,1000\n"
                    "GCF_000002,Bangladesh,Homo sapiens,1000\n"
                    "GCA_000003,India,Homo sapiens,1000\n",
                    encoding="utf-8",
                )
                species = fetchm_app.create_species("Acinetobacter baumannii", taxon_rank="species")
                species.status = "ready"
                species.tsv_path = str(clean_path)
                species.metadata_status = "ready"
                species.metadata_path = str(clean_path)
                species.metadata_clean_path = str(clean_path)
                species.genome_count = 3
                target = SimpleNamespace(snapshot_id="canonical-test", rank="species", name="Acinetobacter baumannii")
            with client.session_transaction() as session:
                session["user_id"] = int(user["id"])
                session["_csrf_token"] = "token"
            patches = [
                patch.object(fetchm_app, "canonical_workflow_species", return_value=(species, target)),
                patch.object(fetchm_app, "count_active_jobs_for_user", return_value=0),
                patch.object(fetchm_app, "build_command", return_value=(["fetchm", "seq"], {})),
                patch.object(fetchm_app, "notify_job_event"),
                patch.object(fetchm_app, "record_audit_event"),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4]:
                bad = client.post(
                    "/metadata-analysis/species/Acinetobacter%20baumannii/sequences/jobs",
                    data={
                        "_csrf_token": "token",
                        "country": "Bangladesh",
                        "sequence_subset_mode": "manual",
                        "sequence_subset_accessions": "BADTOKEN " + "GCA_000001 " * 50,
                    },
                    follow_redirects=False,
                )
                self.assertEqual(bad.status_code, 302)
                location = bad.headers["Location"]
                self.assertNotIn("sequence_subset_accessions", location)
                self.assertNotIn("BADTOKEN", location)

                good = client.post(
                    "/metadata-analysis/species/Acinetobacter%20baumannii/sequences/jobs",
                    data={
                        "_csrf_token": "token",
                        "country": "Bangladesh",
                        "sequence_subset_mode": "random",
                        "sequence_subset_count": "1",
                        "sequence_subset_seed": "5",
                    },
                    follow_redirects=False,
                )
            self.assertEqual(good.status_code, 302)
            with fetchm_app.app.app_context():
                rows = fetchm_app.get_db().execute("SELECT id FROM jobs WHERE mode='seq' ORDER BY created_at").fetchall()
                self.assertEqual(len(rows), 1)
                job = fetchm_app.load_job(str(rows[0]["id"]))
            subset = job.filters["sequence_download_subset"]
            self.assertEqual(subset["mode"], "random")
            self.assertEqual(subset["matched_row_total"], 2)
            self.assertEqual(subset["selected_row_total"], 1)
            self.assertEqual(subset["selected_accession_total"], 1)
            self.assertNotIn("selected_accessions", subset)
            manifest_path = Path(job.input_path).parent.parent / subset["selected_accessions_manifest_path"]
            self.assertEqual(len(manifest_path.read_text(encoding="utf-8").splitlines()), 1)

    def test_pass_fail_decision_mode_collapses_review_into_fail(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_1",
                    "Sequence_QC_Status": "review",
                    "Sequence_QC_Pass": False,
                    "Sequence_QC_Failure_Reasons": "",
                    "Sequence_QC_Review_Reasons": "CheckM completeness missing",
                },
                {
                    "Assembly Accession": "GCA_2",
                    "Sequence_QC_Status": "pass",
                    "Sequence_QC_Pass": True,
                    "Sequence_QC_Failure_Reasons": "",
                    "Sequence_QC_Review_Reasons": "",
                },
            ]
        )
        collapsed = apply_pass_fail_decision_mode(frame)
        self.assertEqual(collapsed["Sequence_QC_Status"].tolist(), ["fail", "pass"])
        self.assertEqual(collapsed.loc[0, "Sequence_QC_Failure_Reasons"], "CheckM completeness missing")
        self.assertEqual(collapsed.loc[0, "Sequence_QC_Review_Reasons"], "")

    def test_post_qc_filters_support_comprehensive_metrics(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_1",
                    "Sequence_QC_Status": "pass",
                    "CheckM completeness": "96",
                    "CheckM contamination": "1",
                    "QC_total_bp": "3000000",
                    "QC_gc_percent": "50",
                    "QC_ambiguous_n_percent": "0.1",
                    "ANI_Closest_ANI": "96.2",
                    "ANI_Species_Consistency_Status": "PASS",
                    "Mash_Distance": "0.01",
                    "GTDBTK_QC_Status": "PASS",
                    "GTDBTK_Match_Rank": "species",
                },
                {
                    "Assembly Accession": "GCA_2",
                    "Sequence_QC_Status": "review",
                    "CheckM completeness": "91",
                    "CheckM contamination": "2",
                    "QC_total_bp": "4500000",
                    "QC_gc_percent": "63",
                    "QC_ambiguous_n_percent": "0.2",
                    "ANI_Closest_ANI": "89",
                    "ANI_Species_Consistency_Status": "WARN",
                    "Mash_Distance": "0.08",
                    "GTDBTK_QC_Status": "WARN",
                    "GTDBTK_Match_Rank": "genus",
                },
            ]
        )

        class Query:
            values = {
                "qc_status": ["pass", "review"],
                "min_ani_percent": "95",
                "max_mash_distance": "0.02",
                "gtdbtk_rank": ["species"],
            }

            def get(self, key: str, default=None):
                value = self.values.get(key, default)
                if isinstance(value, list):
                    return value[0] if value else default
                return value

            def getlist(self, key: str):
                value = self.values.get(key, [])
                return value if isinstance(value, list) else [value]

            def keys(self):
                return self.values.keys()

        filtered, applied = apply_quality_post_filters(frame, Query())
        self.assertEqual(filtered["Assembly Accession"].tolist(), ["GCA_1"])
        self.assertEqual(applied["min_ani_percent"], 95.0)
        self.assertEqual(applied["max_mash_distance"], 0.02)
        self.assertEqual(applied["gtdbtk_rank"], ["species"])

    def test_post_qc_filtered_download_sequence_job_and_owner_protection(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("owner", "owner@example.com", "long-password-1")
                    other = fetchm_app.create_user("other", "other@example.com", "long-password-2")
                    output_dir = fetchm_app.JOBS_DIR / "qc-owner" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    qc_dir.joinpath("qc_enriched_metadata.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination\n"
                        "GCA_1,pass,99,0.1\n"
                        "GCA_2,fail,80,6\n",
                        encoding="utf-8",
                    )
                    qc_dir.joinpath("qc_decisions.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status\nGCA_1,pass\nGCA_2,fail\n",
                        encoding="utf-8",
                    )
                    job = fetchm_app.JobRecord(
                        id="qc-owner",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="qc.csv",
                        input_path=str(root / "qc.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "qc.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"taxon_id": 1, "taxon_name": "Klebsiella", "taxon_rank": "genus"},
                    )
                    fetchm_app.save_job(job)

                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                        session["_csrf_token"] = "token"

                    response = client.get("/jobs/qc-owner/quality-filtered-metadata.csv?qc_status=pass")
                    self.assertEqual(response.status_code, 200)
                    body = response.data.decode("utf-8")
                    self.assertIn("GCA_1", body)
                    self.assertNotIn("GCA_2", body)

                    response = client.post(
                        "/jobs/qc-owner/quality-filtered-sequence-job",
                        data={"_csrf_token": "token", "qc_status": "pass"},
                        follow_redirects=False,
                    )
                    self.assertEqual(response.status_code, 302)
                    created = fetchm_app.get_db().execute(
                        "SELECT * FROM jobs WHERE mode='seq' AND json_extract(filters_json, '$.parent_quality_job_id') = ?",
                        ("qc-owner",),
                    ).fetchone()
                    self.assertIsNotNone(created)
                    self.assertEqual(fetchm_app.load_job(str(created["id"])).filters["matched_row_total"], 1)

                    with client.session_transaction() as session:
                        session["user_id"] = int(other["id"])
                    denied = client.get("/jobs/qc-owner/files/sequence_qc/qc_all_metadata.csv")
                    self.assertEqual(denied.status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_quality_summary_route_generates_report_with_figures_and_links(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("summary-owner", "summary-owner@example.com", "long-password-1")
                    other = fetchm_app.create_user("summary-other", "summary-other@example.com", "long-password-2")
                    output_dir = fetchm_app.JOBS_DIR / "qc-summary" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    (output_dir / "quality_check_bundle.zip").write_bytes(b"zip")
                    qc_dir.joinpath("qc_decisions.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination,"
                        "QC_total_bp,QC_contig_count,QC_n50,QC_gc_percent,QC_ambiguous_n_percent,"
                        "ANI_Closest_ANI,ANI_Species_Consistency_Status,Mash_Distance,Mash_Closest_Genome,"
                        "GTDBTK_QC_Status,GTDBTK_Match_Rank,GTDBTK_Species,GTDBTK_FastANI,"
                        "Sequence_QC_Failure_Reasons,Sequence_QC_Review_Reasons\n"
                        "GCA_1,pass,99.1,0.1,3200000,120,51000,45.2,0.0,97.4,PASS,0.01,GCA_2,PASS,species,Prevotella copri,98.8,,\n"
                        "GCA_2,fail,82.0,6.2,2500000,450,9000,43.1,0.2,88.1,WARN,0.08,GCA_1,FAIL,genus,Prevotella sp,,min_completeness:82<90,ani_species_warning:88.1<95\n",
                        encoding="utf-8",
                    )
                    for name in [
                        "qc_all_metadata.csv",
                        "qc_pass_metadata.csv",
                        "qc_review_metadata.csv",
                        "qc_failed_metadata.csv",
                        "external_qc_master_report.csv",
                    ]:
                        qc_dir.joinpath(name).write_text("Assembly Accession\nGCA_1\n", encoding="utf-8")
                    qc_dir.joinpath("quality_check_report.md").write_text("# report\n", encoding="utf-8")
                    qc_dir.joinpath("quality_check_summary.json").write_text(
                        json.dumps(
                            {
                                "total": 2,
                                "pass": 1,
                                "review": 0,
                                "fail": 1,
                                "quality_profile": "Standard QC",
                                "decision_mode": "pass_fail",
                                "run_mode": "nextflow",
                                "qc_decision_source": "nextflow",
                                "selected_modules": ["quick_fasta", "checkm2", "quast", "ani", "mash", "gtdbtk"],
                                "thresholds": {
                                    "min_completeness": 90.0,
                                    "max_contamination": 5.0,
                                    "min_ani_percent": 95.0,
                                },
                            }
                        ),
                        encoding="utf-8",
                    )
                    job = fetchm_app.JobRecord(
                        id="qc-summary",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="qc.csv",
                        input_path=str(root / "qc.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "qc.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"sequence_filter_sentence": "Matching genomes where Country is India."},
                    )
                    fetchm_app.save_job(job)

                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                    response = client.get("/jobs/qc-summary/quality-summary")
                    self.assertEqual(response.status_code, 200)
                    html = response.data.decode("utf-8")
                    response.close()
                    self.assertIn("QC Figures", html)
                    self.assertIn("Important Files", html)
                    self.assertIn("ANI, Mash, and GTDB-Tk Summary", html)
                    self.assertIn("Download QC ZIP", html)
                    self.assertIn("Passed metadata", html)
                    self.assertIn("GTDB reference ANI", html)
                    self.assertIn("GCA_1", html)
                    self.assertTrue(qc_dir.joinpath("quality_check_summary.html").exists())

                    with client.session_transaction() as session:
                        session["user_id"] = int(other["id"])
                    denied = client.get("/jobs/qc-summary/quality-summary")
                    self.assertEqual(denied.status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_advanced_qc_job_uses_current_post_qc_filtered_subset(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            old_blockers = fetchm_app.quality_submission_blockers
            old_validator = fetchm_app.validate_quality_runtime
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.quality_submission_blockers = lambda: []
            fetchm_app.validate_quality_runtime = lambda quality_config, tool_status: []
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("owner", "owner@example.com", "long-password-1")
                    output_dir = fetchm_app.JOBS_DIR / "standard-parent" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    qc_dir.joinpath("qc_enriched_metadata.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination\n"
                        "GCA_1,pass,99,0.1\n"
                        "GCA_2,fail,80,6\n",
                        encoding="utf-8",
                    )
                    parent = fetchm_app.JobRecord(
                        id="standard-parent",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="standard.csv",
                        input_path=str(root / "standard.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "standard.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"quality_config": {"profile": {"key": "standard"}}},
                    )
                    fetchm_app.save_job(parent)
                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                        session["_csrf_token"] = "token"

                    response = client.post(
                        "/jobs/standard-parent/advanced-quality-job",
                        data={
                            "_csrf_token": "token",
                            "qc_status": "pass",
                            "advanced_module_mash": "1",
                            "advanced_min_ani_percent": "96",
                        },
                        follow_redirects=False,
                    )
                    self.assertEqual(response.status_code, 302)
                    child = fetchm_app.get_db().execute(
                        "SELECT * FROM jobs WHERE mode='qc' AND id != ? ORDER BY created_at DESC LIMIT 1",
                        ("standard-parent",),
                    ).fetchone()
                    self.assertIsNotNone(child)
                    child_job = fetchm_app.load_job(str(child["id"]))
                    self.assertEqual(child_job.filters["parent_quality_job_id"], "standard-parent")
                    self.assertTrue(child_job.filters["advanced_qc"])
                    self.assertEqual(child_job.filters["matched_row_total"], 1)
                    self.assertEqual(child_job.filters["post_qc_filters"]["qc_status"], ["pass"])
                    self.assertIn("ani", child_job.filters["quality_config"]["selected_modules"])
                    self.assertIn("mash", child_job.filters["quality_config"]["selected_modules"])
                    self.assertNotIn("checkm2", child_job.filters["quality_config"]["selected_modules"])
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths
                fetchm_app.quality_submission_blockers = old_blockers
                fetchm_app.validate_quality_runtime = old_validator

    def test_internal_nextflow_work_files_are_hidden_from_user_outputs(self) -> None:
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/nextflow_work/aa/bb/.command.sh")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/local_samples/fetchm_web_qc/sequence/example.fna")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/.nextflow.log")))
        self.assertTrue(should_expose_output_file(Path("external_tools/quality_check/nextflow_execution.log")))
        self.assertTrue(should_expose_output_file(Path("sequence_qc/qc_decisions.csv")))

    def test_global_insights_snapshot_prefers_species_rows_and_writes_simulator_records(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            species_csv = root / "species.csv"
            genus_csv = root / "genus.csv"
            species_csv.write_text(
                "Assembly Accession,Organism Name,Geographic Location,Country,Host,Host_SD,"
                "Isolation Source,Isolation_Source_SD,Collection Date,Collection_Date_Evidence,Assembly Release Date,"
                "Assembly Level,Assembly BioProject Accession,CheckM completeness,Country_Confidence\n"
                "GCA_000001.1,Escherichia coli,USA,United States,Homo sapiens,Human,"
                "human stool,feces/stool,2020,2020-05-01,2021-01-02,Scaffold,PRJNA1,99,exact\n",
                encoding="utf-8",
            )
            genus_csv.write_text(
                "Assembly Accession,Organism Name,Geographic Location,Country,Host,Host_SD,"
                "Isolation Source,Isolation_Source_SD,Collection Date,Collection_Date_Evidence,Assembly Release Date,"
                "Assembly Level,Assembly BioProject Accession,CheckM completeness,Country_Confidence\n"
                "GCA_000001.1,Escherichia coli duplicate,USA,United States,Homo sapiens,Human,"
                "human stool,feces/stool,2020,2020-05-01,2021-01-02,Scaffold,PRJNA1,99,exact\n"
                "GCA_000002.1,Escherichia albertii,Bangladesh: Dhaka,Bangladesh,duck,Anas platyrhynchos,"
                "pond water,wastewater,2021,,2022-04-05,Contig,PRJNA2,96,synonym\n",
                encoding="utf-8",
            )

            summary = generate_global_insights_snapshot(
                [
                    {
                        "id": 1,
                        "species_name": "Escherichia",
                        "taxon_rank": "genus",
                        "genome_count": 2,
                        "metadata_clean_path": str(genus_csv),
                        "last_synced_at": "2026-05-14T01:00:00+00:00",
                    },
                    {
                        "id": 2,
                        "species_name": "Escherichia coli",
                        "taxon_rank": "species",
                        "genome_count": 1,
                        "metadata_clean_path": str(species_csv),
                        "last_synced_at": "2026-05-14T00:00:00+00:00",
                    },
                ],
                root / "global_insights",
                app_version="test",
                app_commit="unit",
                snapshot_id="unit_global_insights",
            )

            self.assertEqual(summary["overview"]["unique_assemblies"], 2)
            self.assertEqual(summary["overview"]["duplicate_rows_skipped"], 1)
            self.assertEqual(summary["overview"]["metadata_files_scanned"], 2)
            self.assertEqual(summary["taxonomic_landscape"]["top_genera"][0]["label"], "Escherichia")
            self.assertEqual(summary["taxonomic_landscape"]["top_genera"][0]["count"], 2)
            collection_year = next(row for row in summary["metadata_completeness"] if row["field"] == "Collection year from collection-date metadata")
            self.assertEqual(collection_year["raw_usable"], 2)
            self.assertEqual(collection_year["standardized_usable"], 2)
            self.assertEqual(collection_year["changed_mappings"], 1)
            corrections = summary["standardization_impact"]["top_corrections"]
            self.assertTrue(any(row["field"] == "Country" and row["raw_value"] == "USA" for row in corrections))
            self.assertTrue((root / "global_insights" / "snapshots" / "unit_global_insights" / "summary.json").exists())

            simulator = run_standardization_simulator(
                root / "global_insights" / "snapshots" / "unit_global_insights" / "tables" / "simulator_records.csv",
                {"taxon": "Escherichia", "country": "United States", "host": "Human"},
            )
            self.assertTrue(simulator["available"])
            self.assertEqual(simulator["raw_count"], 0)
            self.assertEqual(simulator["standardized_count"], 1)
            self.assertEqual(simulator["rescued_count"], 1)
            self.assertEqual(simulator["examples"][0]["assembly_accession"], "GCA_000001.1")

    def test_global_insights_canonical_root_scans_each_assembly_once_and_derives_taxa(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            canonical_csv = root / "canonical_root.csv"
            canonical_csv.write_text(
                "Assembly Accession,Organism Name,Taxonomy Genus,Taxonomy Species,Country,Host_SD,Isolation_Source_SD,Collection Date,Assembly BioProject Accession\n"
                "GCA_000001.1,Prevotella copri strain A,Prevotella,Prevotella copri,India,Human,feces/stool,2020,PRJNA1\n"
                "GCA_000002.1,Prevotella sp. isolate B,Prevotella,Prevotella sp.,India,Human,feces/stool,2021,PRJNA2\n",
                encoding="utf-8",
            )
            summary = generate_global_insights_snapshot(
                [{"id": 0, "species_name": "Bacteria", "taxon_rank": "canonical_root", "genome_count": 2, "metadata_clean_path": str(canonical_csv)}],
                root / "global_insights",
                app_version="test",
                app_commit="unit",
                snapshot_id="canonical_unit",
                canonical_root_source=True,
                source_snapshot_id="root_snapshot",
            )
            self.assertEqual(summary["overview"]["unique_assemblies"], 2)
            self.assertEqual(summary["overview"]["duplicate_rows_skipped"], 0)
            self.assertEqual(summary["methods"]["source_snapshot_id"], "root_snapshot")
            self.assertTrue(summary["methods"]["canonical_root_source"])
            self.assertIn("root_snapshot", summary["manuscript"]["methods"])
            self.assertIn("scanned once at assembly level", summary["manuscript"]["methods"])
            self.assertNotIn("species-level rows preferred over genus-level rows", summary["manuscript"]["methods"])
            self.assertEqual(summary["taxonomic_landscape"]["top_genera"][0]["label"], "Prevotella")
            quality_by_taxon = {row["taxon"]: row for row in summary["metadata_quality"]}
            self.assertEqual(quality_by_taxon["Prevotella"]["assemblies"], 2)
            self.assertEqual(quality_by_taxon["Prevotella copri"]["assemblies"], 1)

    def test_global_insights_metadata_readiness_ranking_uses_score_threshold(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_csv = root / "canonical_root.csv"
            rows = [
                "Assembly Accession,Organism Name,Taxonomy Genus,Taxonomy Species,Country,Host_SD,Isolation_Source_SD,Collection Date,Assembly BioProject Accession,CheckM completeness,Country_Confidence",
            ]
            for index in range(150):
                rows.append(f"GCA_LARGE_{index:06d}.1,Large taxon strain {index},Large,Large taxon,United States,Human,feces/stool,2020,PRJNA_L,{90 + index % 10},exact")
            for index in range(120):
                rows.append(f"GCA_READY_{index:06d}.1,Ready taxon strain {index},Ready,Ready taxon,United States,Human,feces/stool,2020,PRJNA_R{index % 4},{95 + index % 5},exact")
            for index in range(5):
                rows.append(f"GCA_TINY_{index:06d}.1,Tiny taxon strain {index},Tiny,Tiny taxon,United States,Human,feces/stool,2020,PRJNA_T,99,exact")
            source_csv.write_text("\n".join(rows) + "\n", encoding="utf-8")
            summary = generate_global_insights_snapshot(
                [{"id": 0, "species_name": "Bacteria", "taxon_rank": "canonical_root", "genome_count": 275, "metadata_clean_path": str(source_csv)}],
                root / "global_insights",
                app_version="test",
                app_commit="unit",
                snapshot_id="ranking_unit",
                canonical_root_source=True,
                source_snapshot_id="root_snapshot",
            )
            ranked_taxa = [row["taxon"] for row in summary["metadata_quality"]]
            self.assertLess(ranked_taxa.index("Ready taxon"), ranked_taxa.index("Large taxon"))
            self.assertLess(ranked_taxa.index("Large taxon"), ranked_taxa.index("Tiny taxon"))
            self.assertIn("FetchM metadata-readiness index", summary["methods"]["metadata_quality_score"])
            self.assertIn("not a biological genome-quality score", summary["manuscript"]["metadata_quality_by_taxon"])

    def test_global_insights_page_and_summary_download_are_public(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    generate_demo_snapshot(
                        fetchm_app.global_insights_root(),
                        app_version="test",
                        app_commit="unit",
                        snapshot_id="demo_global_insights",
                    )
                    client = fetchm_app.app.test_client()
                    page = client.get("/global-insights")
                    self.assertEqual(page.status_code, 200)
                    html = page.data.decode("utf-8")
                    self.assertIn("Global Bacterial Metadata Insights | FetchM", html)
                    self.assertIn("The public bacterial genome metadata landscape", html)
                    self.assertIn("Structured summary", html)
                    self.assertIn("Publication figures", html)
                    self.assertIn("Cite this snapshot", html)
                    self.assertIn("application/ld+json", html)
                    self.assertIn("DEMO DATA - NOT REAL RESULTS", html)
                    self.assertIn("Methods & Reproducibility", html)
                    self.assertIn("Raw versus Standardized Dataset-selection Simulator", html)

                    snapshot_page = client.get("/global-insights/snapshots/demo_global_insights")
                    self.assertEqual(snapshot_page.status_code, 200)
                    self.assertIn("Cite this snapshot", snapshot_page.data.decode("utf-8"))

                    download = client.get("/global-insights/download/summary.json")
                    self.assertEqual(download.status_code, 200)
                    self.assertEqual(download.get_json()["snapshot_id"], "demo_global_insights")
                    snapshot_download = client.get("/global-insights/snapshots/demo_global_insights/download/summary.json")
                    self.assertEqual(snapshot_download.status_code, 200)
                    self.assertEqual(snapshot_download.get_json()["snapshot_id"], "demo_global_insights")

                    inline_asset = client.get("/global-insights/snapshots/demo_global_insights/asset/figures/figure_1_global_snapshot.svg")
                    self.assertEqual(inline_asset.status_code, 200)
                    self.assertNotIn("attachment", inline_asset.headers.get("Content-Disposition", "").lower())

                    citation = client.get("/global-insights/snapshots/demo_global_insights/citation/bibtex")
                    self.assertEqual(citation.status_code, 200)
                    self.assertIn("@dataset{fetchm_global_insights_demo_global_insights", citation.data.decode("utf-8"))
                    self.assertEqual(client.get("/global-insights/download/../fetchm_webapp.db").status_code, 404)
                    self.assertEqual(client.get("/global-insights/snapshots/demo_global_insights/asset/../summary.json").status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths


if __name__ == "__main__":
    unittest.main()
