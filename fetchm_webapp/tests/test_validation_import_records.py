from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.import_validation_records import import_validation_records


class ValidationImportRecordsTests(unittest.TestCase):
    def test_refuses_unreviewed_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review = root / "review.csv"
            output = root / "validation_records.csv"
            review.write_text(
                "field,assembly_accession,reviewer_label,reviewer_decision\n"
                "Host,GCA_000001.1,,\n",
                encoding="utf-8",
            )
            status = import_validation_records(review, output)
            self.assertEqual(status, 2)
            self.assertFalse(output.exists())

    def test_accepts_completed_review_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review = root / "review.csv"
            output = root / "validation_records.csv"
            review.write_text(
                "field,assembly_accession,reviewer_label,reviewer_decision,error_type\n"
                "Host,GCA_000001.1,Homo sapiens,correct,\n"
                "Host,GCA_000002.1,not_host,false_positive,overcalled_source_as_host\n"
                "Host,GCA_000003.1,unresolved,unresolved,ambiguous_submitter_metadata\n",
                encoding="utf-8",
            )
            status = import_validation_records(review, output)
            self.assertEqual(status, 0)
            contents = output.read_text(encoding="utf-8")
            self.assertIn("GCA_000001.1", contents)
            self.assertIn("false_positive", contents)


if __name__ == "__main__":
    unittest.main()
