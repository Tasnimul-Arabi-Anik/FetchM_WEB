from __future__ import annotations

import json
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch

import dataset_production_store as production_store
from domain_profiles import domain_profile, domain_profile_contract, hidden_domain_keys, hidden_domain_store_configs
from tools import fetch_domain_missing_metadata as domain_fetch_tool
from tools import import_hidden_virus_sequences as virus_import_tool
from tools import qa_hidden_virus_pipeline as virus_qa_tool
from tools import run_hidden_virus_sequence_build as virus_build_tool
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


class VirusModelSummaryTests(unittest.TestCase):
    def test_hidden_virus_model_summary_aggregates_sequences_relationships_and_examples(self) -> None:
        class SimpleResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql: str, params: tuple[object, ...] = ()):  # noqa: ANN001
                if "COUNT(DISTINCT seq.genome_group_id)" in sql:
                    return SimpleResult((3, 1, 2, 2, 1, 2, 2))
                if "FROM domain_virus_genome_group AS g" in sql:
                    return SimpleResult((2,))
                if "SELECT COUNT(*)" in sql and "FROM domain_taxon_relationship AS rel" in sql:
                    return SimpleResult((4,))
                if "GROUP BY rel.relationship_type" in sql:
                    return SimpleResult(rows=[("natural_host", 3), ("propagated_in", 1)])
                if "GROUP BY rel.target_domain" in sql:
                    return SimpleResult(rows=[("eukaryota", 3), ("bacteria", 1)])
                if "GROUP BY rel.target_taxon_name" in sql:
                    return SimpleResult(rows=[("Homo sapiens", 3), ("Escherichia coli", 1)])
                if "GROUP BY seq.molecule_type" in sql:
                    return SimpleResult(rows=[("RNA", 2), ("dsDNA", 1)])
                if "SELECT seq.sequence_accession" in sql:
                    return SimpleResult(rows=[("OP123456.1", "", "Influenza A virus", "flu-group", "RNA", "4", "complete", "SAMN1")])
                raise AssertionError(f"Unexpected SQL: {sql}")

        with patch.object(production_store, "bootstrap_schema", lambda: None), patch.object(
            production_store, "connect", lambda: FakeConnection()
        ):
            summary = production_store.hidden_virus_model_summary(snapshot_id="virus-snapshot", organism_query="Influenza")
        self.assertTrue(summary["available"])
        self.assertEqual(summary["virus_sequence_records"], 3)
        self.assertEqual(summary["virus_assembly_surrogates"], 1)
        self.assertEqual(summary["virus_genome_groups"], 2)
        self.assertEqual(summary["taxon_relationships"], 4)
        self.assertEqual(summary["relationship_density"], 1.33)
        self.assertEqual(summary["top_relationship_types"][0], {"value": "natural_host", "count": 3})
        self.assertEqual(summary["top_target_domains"][1], {"value": "bacteria", "count": 1})
        self.assertEqual(summary["examples"][0]["sequence_accession"], "OP123456.1")
        self.assertFalse(summary["public_enabled"])
        self.assertTrue(summary["release_locked"])



class VirusOperationalBuildTests(unittest.TestCase):
    def _write_reviewed_reports(self, directory: Path) -> Path:
        payload = {
            "reports": [
                {
                    "nuccore_accession": "TESTVIRUS0001",
                    "organism_name": "Example segmented virus",
                    "genome_group_id": "example-virus-group",
                    "segment": "segment-a",
                    "molecule_type": "RNA",
                    "completeness": "complete",
                    "biosample_accession": "SAMNTEST1",
                }
            ]
        }
        path = directory / "reviewed_sequence_reports.json"
        path.write_text(json.dumps(payload))
        return path

    def test_hidden_virus_operational_build_dry_run_does_not_persist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = self._write_reviewed_reports(root)
            output_dir = root / "artifacts"
            with patch.object(virus_build_tool.production_store, "seed_virus_canonical_entities_batch") as seed:
                summary = virus_build_tool.run_hidden_virus_build(
                    input_path=input_path,
                    snapshot_id="virus-sequence-build",
                    output_dir=output_dir,
                    dry_run=True,
                )
            self.assertTrue((output_dir / "hidden_virus_build_summary.json").exists())
            self.assertTrue((output_dir / "hidden_virus_build_summary.md").exists())
        seed.assert_not_called()
        self.assertEqual(summary["status"], "dry_run_pass")
        self.assertEqual(summary["import_summary"]["reports_valid"], 1)
        self.assertTrue(summary["release_locked"])
        self.assertFalse(summary["release_gate"]["safe_to_publish"])

    def test_hidden_virus_operational_build_persists_and_runs_qa(self) -> None:
        qa_summary = {
            "domain_key": "virus",
            "snapshot_id": "virus-sequence-build",
            "status": "pass",
            "hard_failure_count": 0,
            "virus_sequence_records": 1,
            "virus_genome_groups": 1,
            "taxon_relationships": 0,
            "relationship_type_counts": {},
            "target_domain_counts": {},
            "checks": [{"key": "public_release_disabled", "status": "pass", "detail": "locked", "hard": True}],
            "hard_failures": [],
        }
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = self._write_reviewed_reports(root)
            output_dir = root / "artifacts"
            with patch.object(
                virus_build_tool.production_store,
                "seed_virus_canonical_entities_batch",
                return_value={
                    "domain_key": "virus",
                    "snapshot_id": "virus-sequence-build",
                    "total_reports": 1,
                    "virus_sequences_seeded": 1,
                    "virus_genome_groups_touched": 1,
                    "taxon_relationships_seeded": 0,
                    "skipped_reports": 0,
                },
            ) as seed, patch.object(
                virus_build_tool.virus_qa,
                "collect_hidden_virus_qa",
                return_value=qa_summary,
            ) as qa:
                summary = virus_build_tool.run_hidden_virus_build(
                    input_path=input_path,
                    snapshot_id="virus-sequence-build",
                    output_dir=output_dir,
                )
            build_json = json.loads((output_dir / "hidden_virus_build_summary.json").read_text())
            build_md = (output_dir / "hidden_virus_build_summary.md").read_text()
            self.assertTrue((output_dir / "virus_qa_summary.json").exists())
            self.assertTrue((output_dir / "virus_qa_summary.md").exists())
        seed.assert_called_once()
        qa.assert_called_once_with("virus-sequence-build")
        self.assertEqual(summary["status"], "pass")
        self.assertEqual(build_json["persistence"]["virus_sequences_seeded"], 1)
        self.assertFalse(build_json["release_gate"]["safe_to_publish"])
        self.assertIn("Hidden Virus Operational Build", build_md)
        self.assertIn("Release locked: true", build_md)

    def test_hidden_virus_operational_build_reports_consistency_failure(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = self._write_reviewed_reports(root)
            with patch.object(
                virus_build_tool.production_store,
                "seed_virus_canonical_entities_batch",
                return_value={"virus_sequences_seeded": 0},
            ), patch.object(
                virus_build_tool.virus_qa,
                "collect_hidden_virus_qa",
                return_value={"hard_failure_count": 0, "status": "pass"},
            ):
                summary = virus_build_tool.run_hidden_virus_build(
                    input_path=input_path,
                    snapshot_id="virus-sequence-build",
                    output_dir=root / "artifacts",
                )
        self.assertEqual(summary["status"], "fail")
        self.assertIn("persisted Virus sequence count", summary["consistency_errors"][0])



class VirusSequenceAdminReportTests(unittest.TestCase):
    def _fake_sequence_connection(self):
        sequence_row = (
            "OP123456.1",
            "",
            "Influenza A virus",
            11320,
            "SAMN1",
            "RNA",
            "4",
            "complete",
            "A/Dhaka/1/2026",
            "virus-sequence-snapshot",
            {
                "Collection Date": "2026-01-02",
                "Country": "Bangladesh",
                "host": "Homo sapiens",
                "isolation_source": "nasopharyngeal swab",
            },
        )

        class SimpleResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql: str, params: tuple[object, ...] = ()):  # noqa: ANN001
                if "SELECT source_snapshot_id" in sql:
                    return SimpleResult(("virus-sequence-snapshot",))
                if "SELECT sequence_accession, organism_name" in sql:
                    return SimpleResult(rows=[("OP123456.1", "Influenza A virus")])
                if "SELECT sequence_accession, assembly_accession, organism_name" in sql:
                    return SimpleResult(rows=[sequence_row])
                raise AssertionError(f"Unexpected SQL: {sql}")

        return FakeConnection()

    def test_virus_taxon_search_reads_sequence_model_without_assembly_inventory(self) -> None:
        with patch.object(production_store, "bootstrap_schema", lambda: None), patch.object(
            production_store, "connect", lambda: self._fake_sequence_connection()
        ):
            results = production_store.domain_taxon_search_results("virus", "Influenza A")
        species = [row for row in results if row["rank"] == "species" and row["name"] == "Influenza A virus"]
        self.assertEqual(len(species), 1)
        self.assertEqual(species[0]["snapshot_id"], "virus-sequence-snapshot")
        self.assertEqual(species[0]["sequence_count"], 1)
        self.assertFalse(species[0]["public_enabled"])
        self.assertTrue(species[0]["release_locked"])

    def test_virus_taxon_report_uses_sequence_accession_records(self) -> None:
        with patch.object(production_store, "bootstrap_schema", lambda: None), patch.object(
            production_store, "connect", lambda: self._fake_sequence_connection()
        ):
            report = production_store.domain_taxon_report("virus", "species", "Influenza A virus")
        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report["snapshot_id"], "virus-sequence-snapshot")
        self.assertEqual(report["row_count"], 1)
        self.assertEqual(report["record_label"], "hidden viral sequence records")
        self.assertEqual(report["examples"][0]["assembly_accession"], "OP123456.1")
        self.assertEqual(report["examples"][0]["molecule_type"], "RNA")
        self.assertEqual(report["top_molecule_types"], [{"value": "RNA", "count": 1}])
        self.assertEqual(report["summary_metrics"]["complete_genome_count"], 1)
        self.assertTrue(any("sequence/genome-group model" in note for note in report["presentation_notes"]))

    def test_virus_taxon_metadata_csv_exports_sequence_columns(self) -> None:
        with patch.object(production_store, "bootstrap_schema", lambda: None), patch.object(
            production_store, "connect", lambda: self._fake_sequence_connection()
        ):
            export = production_store.domain_taxon_metadata_csv("virus", "species", "Influenza A virus")
        self.assertIsNotNone(export)
        assert export is not None
        self.assertEqual(export["row_count"], 1)
        self.assertIn("virus_species_Influenza-A-virus_metadata.csv", export["filename"])
        self.assertIn("sequence_accession", export["content"])
        self.assertIn("OP123456.1", export["content"])
        self.assertIn("Virus_Molecule_Type", export["content"])




class VirusQaTests(unittest.TestCase):
    def _fake_virus_qa_connection(self, *, invalid_relationship_type: int = 0):
        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql: str, params: tuple[object, ...] = ()):  # noqa: ANN001
                if "FROM domain_inventory_snapshot" in sql:
                    return SimpleResult(("completed", "admin_hidden", True, 2))
                if "FROM domain_virus_sequence_record" in sql and "COUNT(DISTINCT" in sql:
                    return SimpleResult((2, 0, 0, 0, 2, 1))
                if "FROM domain_virus_genome_group" in sql and "zero_segment_count" in sql:
                    return SimpleResult((1, 0, 0))
                if "domain_virus_genome_group AS g" in sql:
                    return SimpleResult((0,))
                if "FROM domain_taxon_relationship" in sql and "wrong_domain" in sql:
                    return SimpleResult((2, 0, 0, invalid_relationship_type, 0, 0, 0, 0))
                if "LEFT JOIN domain_virus_sequence_record" in sql:
                    return SimpleResult((0,))
                if "GROUP BY relationship_type" in sql:
                    return SimpleResult(rows=[("natural_host", 1), ("propagated_in", 1)])
                if "GROUP BY target_domain" in sql:
                    return SimpleResult(rows=[("bacteria", 2)])
                raise AssertionError(f"Unexpected SQL: {sql}")

        class SimpleResult:
            def __init__(self, row=None, rows=None):
                self.row = row
                self.rows = rows or []

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.rows

        return FakeConnection()

    def test_hidden_virus_qa_passes_valid_sequence_group_relationship_model(self) -> None:
        with patch.object(virus_qa_tool, "connect", lambda: self._fake_virus_qa_connection()):
            summary = virus_qa_tool.collect_hidden_virus_qa("virus-snapshot")
        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["hard_failure_count"], 0)
        self.assertEqual(summary["virus_sequence_records"], 2)
        self.assertEqual(summary["virus_genome_groups"], 1)
        self.assertEqual(summary["taxon_relationships"], 2)
        self.assertEqual(summary["relationship_type_counts"], {"natural_host": 1, "propagated_in": 1})
        self.assertEqual(summary["target_domain_counts"], {"bacteria": 2})
        self.assertTrue(any(check["key"] == "snapshot_release_locked" and check["status"] == "pass" for check in summary["checks"]))

    def test_hidden_virus_qa_fails_uncontrolled_relationship_type(self) -> None:
        with patch.object(virus_qa_tool, "connect", lambda: self._fake_virus_qa_connection(invalid_relationship_type=1)):
            summary = virus_qa_tool.collect_hidden_virus_qa("virus-snapshot")
        self.assertEqual(summary["status"], "fail")
        self.assertTrue(any(check["key"] == "relationships_types_controlled" for check in summary["hard_failures"]))

    def test_hidden_virus_qa_outputs_json_and_markdown(self) -> None:
        with patch.object(virus_qa_tool, "connect", lambda: self._fake_virus_qa_connection()):
            summary = virus_qa_tool.collect_hidden_virus_qa("virus-snapshot")
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            virus_qa_tool.write_outputs(summary, output_dir)
            self.assertTrue((output_dir / "virus_qa_summary.json").exists())
            markdown = (output_dir / "virus_qa_summary.md").read_text()
        self.assertIn("Hidden Virus QA Summary", markdown)
        self.assertIn("Release locked: true", markdown)



class VirusPersistenceTests(unittest.TestCase):
    def test_seed_virus_canonical_entities_batch_writes_virus_tables(self) -> None:
        executed: list[tuple[str, tuple[object, ...]]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql: str, params: tuple[object, ...] = ()):  # noqa: ANN001
                executed.append((sql, tuple(params or ())))
                return self

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self):
                return FakeCursor()

            def commit(self):
                executed.append(("COMMIT", ()))

        reports = [{
            "nuccore_accession": "NC_000866.4",
            "organism_name": "Escherichia phage T4",
            "tax_id": 10665,
            "molecule_type": "dsDNA",
            "genome_completeness": "complete",
            "host": {"name": "Escherichia coli", "tax_id": 562},
            "lab_host": "Escherichia coli K-12",
            "biosample_accession": "SAMN00000010",
        }]
        with patch.object(production_store, "bootstrap_schema", lambda: None), patch.object(
            production_store, "connect", lambda: FakeConnection()
        ):
            summary = production_store.seed_virus_canonical_entities_batch("virus-snapshot", reports)

        self.assertEqual(summary["virus_sequences_seeded"], 1)
        self.assertEqual(summary["virus_genome_groups_touched"], 1)
        self.assertEqual(summary["taxon_relationships_seeded"], 2)
        sql_text = "\n".join(sql for sql, _ in executed)
        self.assertIn("domain_virus_sequence_record", sql_text)
        self.assertIn("domain_virus_genome_group", sql_text)
        self.assertIn("domain_taxon_relationship", sql_text)
        flattened_params = "\n".join(str(param) for _, params in executed for param in params)
        self.assertIn("NC_000866.4", flattened_params)
        self.assertIn("Escherichia coli", flattened_params)
        self.assertIn("virus-snapshot", flattened_params)

    def test_hidden_virus_sequence_import_loads_and_summarizes_reports(self) -> None:
        payload = {
            "reports": [
                {
                    "nuccore_accession": "OP123456.1",
                    "organism_name": "Influenza A virus",
                    "genome_group_id": "flu-group",
                    "segment": "4",
                    "host": "Homo sapiens",
                },
                {"not_an_accession": "skip me"},
            ]
        }
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "virus_reports.json"
            path.write_text(json.dumps(payload))
            reports = virus_import_tool.load_reports(path)
        self.assertEqual(len(reports), 2)
        summary = virus_import_tool.summarize_reports(reports, "virus-snapshot")
        self.assertEqual(summary["reports_loaded"], 2)
        self.assertEqual(summary["reports_valid"], 1)
        self.assertEqual(summary["reports_skipped"], 1)
        self.assertEqual(summary["virus_sequence_records"], 1)
        self.assertEqual(summary["virus_genome_groups"], 1)
        self.assertEqual(summary["taxon_relationships"], 1)

    def test_standardizable_domain_row_adds_virus_fields_for_virus_only(self) -> None:
        report = {
            "accession": "GCA_000000999.1",
            "organism": {"organism_name": "Example virus", "tax_id": 999999},
            "assembly_info": {"biosample": {"host": "Homo sapiens"}},
        }
        row = domain_fetch_tool.standardizable_domain_row(report, "archaea")
        self.assertNotIn("Virus_Primary_Accession", row)

        row = domain_fetch_tool.standardizable_domain_row(report, "virus")
        self.assertEqual(row["FetchM_Domain_Key"], "virus")
        self.assertEqual(row["Virus_Primary_Accession"], "GCA_000000999.1")
        self.assertEqual(row["FetchM_Virus_Record_Model"], "virus_assembly_surrogate")

    def test_hidden_virus_metadata_fetch_command_persists_virus_entities(self) -> None:
        report = {
            "accession": "GCA_000000999.1",
            "organism": {"organism_name": "Example virus", "tax_id": 999999},
            "assembly_info": {"biosample": {"host": "Homo sapiens"}},
        }
        argv = [
            "fetch_domain_missing_metadata.py",
            "--domain",
            "virus",
            "--snapshot-id",
            "virus-snapshot",
            "--batch-size",
            "1",
            "--request-workers",
            "1",
            "--standardization-workers",
            "1",
            "--request-sleep",
            "0",
        ]
        with patch.object(domain_fetch_tool.sys, "argv", argv), patch.object(
            domain_fetch_tool, "missing_domain_standardized_accession_batch", side_effect=[["GCA_000000999.1"], []]
        ), patch.object(domain_fetch_tool, "fetch_reports", return_value=[report]), patch.object(
            domain_fetch_tool, "insert_domain_inventory_batch", return_value=(1, 0, 0)
        ), patch.object(
            domain_fetch_tool, "seed_domain_standardized_metadata_batch", return_value={"total": 1, "seeded": 1, "skipped_not_in_domain_root": 0}
        ), patch.object(
            domain_fetch_tool, "seed_virus_canonical_entities_batch", return_value={
                "virus_sequences_seeded": 1,
                "taxon_relationships_seeded": 1,
            }
        ) as seed_virus, patch.object(
            domain_fetch_tool, "domain_standardized_metadata_coverage", return_value={
                "root_unique_assemblies": 1,
                "standardized_assemblies": 1,
                "missing_standardized_assemblies": 0,
            }
        ), patch.object(domain_fetch_tool, "standardization_rule_manifest", return_value={"version": "test-rules"}):
            self.assertEqual(domain_fetch_tool.main(), 0)
        seed_virus.assert_called_once()
        self.assertEqual(seed_virus.call_args.args[0], "virus-snapshot")
        self.assertEqual(seed_virus.call_args.args[1], [report])



if __name__ == "__main__":
    unittest.main()
