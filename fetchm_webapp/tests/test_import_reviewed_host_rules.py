from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools import import_reviewed_host_rules as importer


class ReviewedHostRuleImporterTests(unittest.TestCase):
    def test_later_batch_replaces_conflicting_normalized_decision(self) -> None:
        with TemporaryDirectory() as tmp:
            first = Path(tmp) / "first.csv"
            second = Path(tmp) / "second.csv"
            fields = [
                "raw_host",
                "final_is_approved",
                "final_host",
                "rule_type",
            ]
            with first.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerow(
                    {
                        "raw_host": "water-lettuce",
                        "final_is_approved": "TRUE",
                        "final_host": "Lactuca sativa",
                        "rule_type": "exact_host",
                    }
                )
            with second.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerow(
                    {
                        "raw_host": "water lettuce",
                        "final_is_approved": "TRUE",
                        "final_host": "Pistia stratiotes",
                        "rule_type": "exact_host",
                    }
                )

            decisions, conflicts = importer.consolidate_batches([first, second])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["final_host"], "Pistia stratiotes")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["result"], "batch_conflict_latest_wins")

    def test_context_only_labels_are_explicit(self) -> None:
        self.assertEqual(importer.CONTEXT_ONLY_HOSTS["fish"], "fish")
        self.assertEqual(
            importer.CONTEXT_ONLY_HOSTS["marine invertebrate"],
            "marine invertebrate",
        )
        self.assertNotIn("Charadriiformes", importer.CONTEXT_ONLY_HOSTS)

    def test_rule_type_alias_is_preserved_with_underscores(self) -> None:
        row = importer.normalize_review_row(
            {
                "raw_host": "Latuca sp.",
                "final_is_approved": "TRUE",
                "final_host": "Lactuca",
                "rule_type": "exact_or_genus_taxon",
            },
            Path("batch.csv"),
        )
        self.assertEqual(row["rule_type"], "exact_host")


if __name__ == "__main__":
    unittest.main()
