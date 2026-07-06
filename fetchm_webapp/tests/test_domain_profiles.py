from __future__ import annotations

import json
import unittest

import dataset_production_store as production_store
from domain_profiles import domain_profile, domain_profile_contract, hidden_domain_keys, hidden_domain_store_configs
from tools import fetch_domain_missing_metadata as domain_fetch_tool
from virus_canonical import virus_canonical_entities, virus_standardization_row_fields


class DomainProfileContractTests(unittest.TestCase):
    def test_hidden_domain_profiles_keep_virus_separate_from_prokaryotes(self) -> None:
        self.assertEqual(hidden_domain_keys(), ("archaea", "virus"))
        bacteria = domain_profile("bacteria")
        archaea = domain_profile("archaea")
        virus = domain_profile("virus")
        self.assertIn("prokaryote_assembly", bacteria.canonical_entities)
        self.assertIn("prokaryote_assembly", archaea.canonical_entities)
        self.assertIn("virus_sequence", virus.canonical_entities)
        self.assertIn("virus_genome_group", virus.canonical_entities)
        self.assertIn("taxon_relationship", virus.canonical_entities)
        self.assertNotIn("virus_sequence", archaea.canonical_entities)
        self.assertFalse(virus.public_enabled)
        self.assertTrue(virus.release_locked)
        self.assertEqual(virus.root_taxon_id, 10239)
        self.assertIn("ncbi_virus_sequence_report", virus.source_adapters)

    def test_hidden_store_configs_are_derived_from_profiles(self) -> None:
        configs = hidden_domain_store_configs()
        self.assertEqual(set(configs), {"archaea", "virus"})
        self.assertEqual(configs["virus"]["profile"], "virus_hidden_v1")
        self.assertEqual(configs["virus"]["canonical_entities"], list(domain_profile("virus").canonical_entities))
        self.assertEqual(production_store.domain_pipeline_config("virus")["record_model"], "virus_sequence_or_assembly_surrogate")

    def test_virus_contract_documents_relationship_semantics(self) -> None:
        contract = domain_profile_contract("virus")
        self.assertEqual(contract["primary_record_model"], "virus_sequence_or_assembly_surrogate")
        self.assertIn("host_relationship", contract["standardization_axes"])
        self.assertIn("virus_relationship_semantics_qa", contract["qa_gates"])
        self.assertTrue(any("records remain viral" in note for note in contract["notes"]))

    def test_virus_schema_tables_are_isolated_and_hidden(self) -> None:
        schema = production_store.SCHEMA_SQL
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_virus_sequence_record", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_virus_genome_group", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS domain_taxon_relationship", schema)
        self.assertIn("CHECK (domain_key = 'virus')", schema)
        self.assertIn("CHECK (domain_key <> 'bacteria')", schema)
        self.assertIn("idx_domain_taxon_relationship_subject", schema)


class VirusCanonicalModelTests(unittest.TestCase):
    def test_bacteriophage_host_relationship_does_not_change_record_domain(self) -> None:
        report = {
            "nuccore_accession": "NC_000866.4",
            "organism_name": "Escherichia phage T4",
            "tax_id": 10665,
            "molecule_type": "dsDNA",
            "genome_completeness": "complete",
            "segment": "segment 1",
            "host": {"name": "Escherichia coli", "tax_id": 562},
            "lab_host": "Escherichia coli K-12",
            "biosample_accession": "SAMN00000010",
            "isolate": "T4",
        }
        entities = virus_canonical_entities(report, snapshot_id="virus-snapshot")
        self.assertEqual(entities["common_record"]["record_domain"], "virus")
        sequence = entities["virus_sequence"]
        self.assertEqual(sequence["record_model"], "virus_sequence")
        self.assertEqual(sequence["primary_accession"], "NC_000866.4")
        self.assertEqual(sequence["sequence_accession"], "NC_000866.4")
        self.assertEqual(sequence["segment"], "segment 1")
        relationships = entities["host_relationships"]
        self.assertEqual({row["relationship_type"] for row in relationships}, {"natural_host", "propagated_in"})
        natural_host = [row for row in relationships if row["relationship_type"] == "natural_host"][0]
        self.assertEqual(natural_host["target_taxon_id"], 562)
        self.assertEqual(natural_host["target_domain"], "bacteria")
        self.assertEqual(natural_host["subject_record_domain"], "virus")

    def test_segmented_virus_uses_explicit_genome_group(self) -> None:
        report = {
            "accession": "OP123456.1",
            "organism_name": "Influenza A virus",
            "taxid": 11320,
            "genome_group_id": "flu-isolate-2026-01",
            "segment_name": "4",
            "molecule_type": "RNA",
            "completeness": "complete",
            "host": "Homo sapiens",
        }
        entities = virus_canonical_entities(report)
        self.assertEqual(entities["virus_sequence"]["genome_group_id"], "flu-isolate-2026-01")
        self.assertEqual(entities["virus_genome_group"]["representative_accession"], "OP123456.1")
        relationship = entities["host_relationships"][0]
        self.assertEqual(relationship["target_domain"], "eukaryota")
        self.assertEqual(relationship["relationship_type"], "natural_host")

    def test_assembly_report_is_marked_as_virus_assembly_surrogate(self) -> None:
        report = {
            "accession": "GCA_000000999.1",
            "organism": {"organism_name": "Example virus", "tax_id": 999999},
            "assembly_info": {
                "assembly_name": "ASM999",
                "assembly_level": "Complete Genome",
                "biosample": {
                    "accession": "SAMN00000999",
                    "host": "Homo sapiens",
                    "isolation_source": "nasopharyngeal swab",
                    "attributes": [{"name": "geo_loc_name", "value": "USA"}],
                },
            },
        }
        row = domain_fetch_tool.standardizable_domain_row(report, "virus")
        self.assertEqual(row["FetchM_Domain"], "Virus")
        self.assertEqual(row["FetchM_Domain_Profile"], "virus_hidden_v1")
        self.assertEqual(row["FetchM_Virus_Record_Model"], "virus_assembly_surrogate")
        self.assertEqual(row["Virus_Primary_Accession"], "GCA_000000999.1")
        self.assertEqual(row["Virus_Assembly_Accession"], "GCA_000000999.1")
        self.assertGreaterEqual(int(row["Virus_Host_Relationship_Count"]), 1)
        relationships = json.loads(row["Virus_Host_Relationships_JSON"])
        self.assertTrue(all(item["subject_record_domain"] == "virus" for item in relationships))

    def test_virus_standardization_fields_are_json_serializable(self) -> None:
        fields = virus_standardization_row_fields({
            "nuccore_accession": "NC_123456.1",
            "organism_name": "Archaeal virus example",
            "host": {"name": "Methanocaldococcus jannaschii", "tax_id": 2190},
        })
        encoded = json.dumps(fields, sort_keys=True)
        self.assertIn("Virus_Host_Relationships_JSON", fields)
        self.assertIn("NC_123456.1", encoded)
        relationship = json.loads(fields["Virus_Host_Relationships_JSON"])[0]
        self.assertEqual(relationship["target_domain"], "archaea")


if __name__ == "__main__":
    unittest.main()
