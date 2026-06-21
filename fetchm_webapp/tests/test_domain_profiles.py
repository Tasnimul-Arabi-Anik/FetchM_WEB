from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import unittest

from domain_profiles import (
    domain_profile,
    domain_profile_from_snapshot_id,
    domain_profile_from_taxon_id,
    validate_snapshot_id_for_profile,
)
from dataset_production_store import ARCHAEA_TAXON_ID, BACTERIA_TAXON_ID
from tools import build_canonical_root_inventory as canonical_inventory_tool


class DomainProfileTests(unittest.TestCase):
    def test_bacteria_default_and_archaea_hidden(self) -> None:
        bacteria = domain_profile()
        archaea = domain_profile("archaea")
        self.assertEqual(bacteria.key, "bacteria")
        self.assertEqual(bacteria.ncbi_taxon_id, BACTERIA_TAXON_ID)
        self.assertTrue(bacteria.public_enabled)
        self.assertEqual(archaea.key, "archaea")
        self.assertEqual(archaea.ncbi_taxon_id, ARCHAEA_TAXON_ID)
        self.assertFalse(archaea.public_enabled)
        self.assertEqual(bacteria.rule_scopes, ("common", "prokaryote", "bacteria"))
        self.assertEqual(archaea.rule_scopes, ("common", "prokaryote", "archaea"))
        self.assertTrue(bacteria.applies_rule_scope("bacteria"))
        self.assertFalse(bacteria.applies_rule_scope("archaea"))
        self.assertTrue(archaea.applies_rule_scope("archaea"))
        self.assertFalse(archaea.applies_rule_scope("bacteria"))
        timestamp = datetime(2026, 6, 19, tzinfo=timezone.utc)
        self.assertEqual(bacteria.snapshot_id(timestamp), "20260619T000000Z_genbank_bacteria_root")
        self.assertEqual(archaea.snapshot_id(timestamp), "20260619T000000Z_genbank_archaea_root")

    def test_profile_resolution_is_snapshot_and_taxon_aware(self) -> None:
        self.assertEqual(domain_profile_from_snapshot_id("20260619T000000Z_genbank_bacteria_root").key, "bacteria")
        self.assertEqual(domain_profile_from_snapshot_id("20260619T000000Z_genbank_archaea_root").key, "archaea")
        self.assertEqual(domain_profile_from_taxon_id(BACTERIA_TAXON_ID).key, "bacteria")
        self.assertEqual(domain_profile_from_taxon_id(ARCHAEA_TAXON_ID).key, "archaea")

    def test_snapshot_id_validation_prevents_cross_domain_mixes(self) -> None:
        bacteria = domain_profile("bacteria")
        archaea = domain_profile("archaea")
        self.assertEqual(
            validate_snapshot_id_for_profile("20260619T000000Z_genbank_bacteria_root", bacteria),
            "20260619T000000Z_genbank_bacteria_root",
        )
        self.assertEqual(
            validate_snapshot_id_for_profile("20260619T000000Z_genbank_archaea_root", archaea),
            "20260619T000000Z_genbank_archaea_root",
        )
        with self.assertRaises(ValueError):
            validate_snapshot_id_for_profile("20260619T000000Z_genbank_bacteria_root", archaea)
        with self.assertRaises(ValueError):
            validate_snapshot_id_for_profile("", bacteria)

    def test_canonical_inventory_url_uses_profile_taxon_roots(self) -> None:
        self.assertIn(f"/taxon/{BACTERIA_TAXON_ID}/", canonical_inventory_tool.api_url(domain_profile("bacteria").ncbi_taxon_id))
        self.assertIn(f"/taxon/{ARCHAEA_TAXON_ID}/", canonical_inventory_tool.api_url(domain_profile("archaea").ncbi_taxon_id))

    def test_canonical_inventory_cli_exposes_hidden_pilot_mode(self) -> None:
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "build_canonical_root_inventory.py"
        result = subprocess.run([sys.executable, str(tool_path), "--help"], check=True, capture_output=True, text=True)
        self.assertIn("--domain", result.stdout)
        self.assertIn("--pilot-pages", result.stdout)

    def test_metadata_fetch_cli_can_skip_pilot_side_effect_exports(self) -> None:
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "fetch_canonical_missing_metadata.py"
        source = tool_path.read_text(encoding="utf-8")
        self.assertIn("--skip-host-monitoring", source)
        self.assertIn("domain_profile_from_snapshot_id", source)
        self.assertIn("domain_profile=domain_profile_key", source)

    def test_restandardization_cli_can_use_snapshot_domain_without_side_effect_exports(self) -> None:
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "restandardize_canonical_metadata.py"
        source = tool_path.read_text(encoding="utf-8")
        self.assertIn("domain_profile_from_snapshot_id", source)
        self.assertIn("domain_profile=domain_profile_key", source)
        self.assertIn("--skip-host-monitoring", source)

    def test_hidden_archaea_pipeline_runner_keeps_outputs_non_public(self) -> None:
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_hidden_archaea_database_pipeline.py"
        result = subprocess.run([sys.executable, str(tool_path), "--help"], check=True, capture_output=True, text=True)
        self.assertIn("--snapshot-id", result.stdout)
        self.assertIn("--skip-fetch", result.stdout)
        source = tool_path.read_text(encoding="utf-8")
        self.assertIn('"visibility": "hidden_staging"', source)
        self.assertIn('"public_ui_exposed": False', source)
        self.assertIn('"global_insights_regenerated": False', source)
        self.assertIn('"deployment_run": False', source)


if __name__ == "__main__":
    unittest.main()
