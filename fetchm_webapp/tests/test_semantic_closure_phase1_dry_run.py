from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools import semantic_closure_phase1_dry_run as closure


class SemanticClosurePhase1DryRunTests(unittest.TestCase):
    def test_rules_load_with_reviewed_status(self) -> None:
        rules = closure.load_rules(closure.DEFAULT_RULES)
        self.assertGreaterEqual(len(rules), 10)
        self.assertTrue(all(rule.reviewer_status == "approved_semantic_closure_phase1_dry_run" for rule in rules))
        self.assertTrue(all(rule.preserve_legacy_fields for rule in rules))

    def test_healthy_control_splits_health_state_and_study_group(self) -> None:
        rules = closure.rules_by_current(closure.load_rules(closure.DEFAULT_RULES))
        payload = {
            "Host_Health_State_SD": "healthy/control",
            "Host_Health_State_SD_Method": "legacy_rule",
            "Sample_Type_SD": "legacy sample label",
            "Isolation_Source_SD": "legacy source label",
        }
        result = closure.apply_rules_to_payload(payload, rules)
        self.assertEqual(result["after"]["Host_Health_State_SD"], "healthy")
        self.assertEqual(result["after"]["Host_Study_Group_SD"], "control")
        self.assertEqual(result["after"]["Sample_Type_SD"], "legacy sample label")
        self.assertEqual(result["after"]["Isolation_Source_SD"], "legacy source label")
        self.assertEqual(result["legacy_changed"], [])
        self.assertEqual(result["protected_changed"], [])
        self.assertTrue(result["removal_events"])

    def test_environment_axis_move_skips_clear_when_destination_existing_different(self) -> None:
        rules = closure.rules_by_current(closure.load_rules(closure.DEFAULT_RULES))
        payload = {
            "Environment_Medium_SD": "marine",
            "Environment_Broad_Scale_SD": "coastal environment",
        }
        result = closure.apply_rules_to_payload(payload, rules)
        self.assertEqual(result["after"]["Environment_Medium_SD"], "marine")
        self.assertEqual(result["after"]["Environment_Broad_Scale_SD"], "coastal environment")
        self.assertIn("existing_different", {event["status"] for event in result["outcomes"]})
        self.assertEqual(result["clears"], [])

    def test_environment_axis_move_clears_when_destination_blank(self) -> None:
        rules = closure.rules_by_current(closure.load_rules(closure.DEFAULT_RULES))
        payload = {"Environment_Medium_SD": "marine", "Environment_Broad_Scale_SD": ""}
        result = closure.apply_rules_to_payload(payload, rules)
        self.assertEqual(result["after"]["Environment_Medium_SD"], "")
        self.assertEqual(result["after"]["Environment_Broad_Scale_SD"], "marine environment")
        self.assertTrue(any(clear["field"] == "Environment_Medium_SD" for clear in result["clears"]))
        self.assertTrue(result["removal_events"])

    def test_catheter_clear_only_preserves_legacy_fields(self) -> None:
        rules = closure.rules_by_current(closure.load_rules(closure.DEFAULT_RULES))
        payload = {
            "Isolation_Site_SD": "catheter",
            "Isolation_Site_SD_Method": "legacy_rule",
            "Sample_Type_SD": "catheter tip",
            "Isolation_Source_SD": "device-associated source",
        }
        result = closure.apply_rules_to_payload(payload, rules)
        self.assertEqual(result["after"]["Isolation_Site_SD"], "")
        self.assertEqual(result["after"]["Sample_Type_SD"], "catheter tip")
        self.assertEqual(result["after"]["Isolation_Source_SD"], "device-associated source")
        self.assertEqual(result["legacy_changed"], [])
        self.assertTrue(result["removal_events"])

    def test_run_dry_run_writes_required_compact_artifacts(self) -> None:
        records = [
            {
                "assembly_accession": "GCA_TEST_1",
                "organism": "Example bacterium",
                "biosample": "SAMNTEST1",
                "payload": {"Host_Health_State_SD": "healthy/control"},
            },
            {
                "assembly_accession": "GCA_TEST_2",
                "organism": "Example bacterium",
                "biosample": "SAMNTEST2",
                "payload": {"Environment_Medium_SD": "marine"},
            },
        ]
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            original_iter = closure.iter_records
            try:
                closure.iter_records = lambda snapshot_id, chunk_size=5000: iter(records)  # type: ignore[assignment]
                summary = closure.run_dry_run("test-snapshot", closure.DEFAULT_RULES, output_dir, example_limit=10)
            finally:
                closure.iter_records = original_iter  # type: ignore[assignment]
            self.assertEqual(summary["canonical_rows_scanned"], 2)
            self.assertEqual(summary["projected_rows_changed"], 2)
            self.assertFalse(json.loads((output_dir / "promotion_gate.json").read_text())["safe_to_apply"])
            for name in [
                "semantic_closure_phase1_dry_run_summary.json",
                "semantic_closure_phase1_dry_run_summary.md",
                "reviewed_rules.tsv",
                "rule_level_before_after.tsv",
                "projected_field_changes.tsv",
                "projected_new_axis_assignments.tsv",
                "projected_clears.tsv",
                "destination_conflicts.tsv",
                "skipped_existing_different_destinations.tsv",
                "preserved_legacy_fields.tsv",
                "provenance_summary.tsv",
                "idempotency_summary.json",
                "representative_examples.tsv",
                "remaining_unresolved_strict_signals.tsv",
                "promotion_gate.json",
            ]:
                self.assertTrue((output_dir / name).exists(), name)
            with (output_dir / "reviewed_rules.tsv").open(newline="", encoding="utf-8") as handle:
                self.assertGreater(len(list(csv.DictReader(handle, delimiter="\t"))), 0)


if __name__ == "__main__":
    unittest.main()
