from __future__ import annotations

import unittest

from tools.audit_host_clinical_site_semantics import classify_value, incompatibility_rows


class HostClinicalSiteSemanticsAuditTests(unittest.TestCase):
    def test_classifies_known_axis_splits(self) -> None:
        cases = {
            "patient": "host_context",
            "hospitalized": "care_setting",
            "control": "study_group",
            "carrier": "colonization_status",
            "dead": "vital_status",
            "convalescent": "disease_stage",
            "pus": "sample_material",
            "sink": "environment_local_scale",
            "DNA extract": "sample_processing",
            "clinical/host-associated material": "source_context",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(classify_value(value).semantic_class, expected)

    def test_compound_disease_name_does_not_become_blood_material(self) -> None:
        self.assertEqual(classify_value("banana blood disease").semantic_class, "disease")

    def test_site_classes_are_not_flagged_as_non_sites(self) -> None:
        rows = [
            {
                "field": "Isolation_Site_SD",
                "standardized_value": "sterile body site",
                "semantic_class": classify_value("sterile body site").semantic_class,
                "row_count": 10,
                "reason": "test",
            },
            {
                "field": "Isolation_Site_SD",
                "standardized_value": "organ/tissue site",
                "semantic_class": classify_value("organ/tissue site").semantic_class,
                "row_count": 5,
                "reason": "test",
            },
        ]
        flags = incompatibility_rows(rows)
        self.assertEqual(flags["non_site_values_in_isolation_site"], [])

    def test_patient_in_health_state_is_flagged_for_host_context(self) -> None:
        rows = [
            {
                "field": "Host_Health_State_SD",
                "standardized_value": "patient",
                "semantic_class": classify_value("patient").semantic_class,
                "row_count": 7,
                "reason": "test",
            }
        ]
        flags = incompatibility_rows(rows)
        self.assertEqual(flags["non_health_values_in_host_health_state"][0]["proposed_destination"], "Host_Context_SD")


if __name__ == "__main__":
    unittest.main()
