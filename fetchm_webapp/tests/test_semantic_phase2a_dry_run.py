from __future__ import annotations

import unittest

from tools.semantic_phase2a_dry_run import (
    DEFAULT_RULES,
    PROVENANCE_FIELD,
    apply_rules_to_payload,
    load_rules,
    rules_by_current,
)


def run_payload(payload: dict[str, str]) -> dict[str, object]:
    rules = load_rules(DEFAULT_RULES)
    return apply_rules_to_payload(payload, rules_by_current(rules))


class SemanticPhase2ADryRunTests(unittest.TestCase):
    def assert_legacy_preserved(self, before: dict[str, str], after: dict[str, object]) -> None:
        for field in ["Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad"]:
            self.assertEqual(after.get(field, ""), before.get(field, ""), field)

    def test_patient_human_context_is_evidence_gated(self) -> None:
        payload = {
            "Host_Health_State_SD": "patient",
            "Host_SD": "Homo sapiens",
            "Host_TaxID": "9606",
            "Sample_Type_SD": "clinical sample",
            "Isolation_Source_SD": "clinical/host-associated material",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Host_Health_State_SD"), "")
        self.assertEqual(after.get("Sampling_Context_SD"), "clinical subject")
        self.assertEqual(after.get("Host_Context_SD"), "human-associated")
        self.assertIn("Sampling_Context_SD", after.get(PROVENANCE_FIELD, {}))
        self.assertIn("Host_Context_SD", after.get(PROVENANCE_FIELD, {}))
        self.assert_legacy_preserved(payload, after)
        self.assertEqual(result["conflicts"], [])

    def test_patient_nonhuman_does_not_infer_human(self) -> None:
        payload = {
            "Host_Health_State_SD": "patient",
            "Host_SD": "Bos taurus",
            "Host_TaxID": "9913",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Host_Health_State_SD"), "")
        self.assertEqual(after.get("Sampling_Context_SD"), "clinical subject")
        self.assertNotEqual(after.get("Host_Context_SD"), "human-associated")
        self.assertTrue(any(event["rule_id"] == "PH2A-HHS-PATIENT-HUMAN-CONTEXT" for event in result["evidence_failures"]))

    def test_patient_without_host_evidence_remains_clinical_only(self) -> None:
        payload = {"Host_Health_State_SD": "patient"}
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Sampling_Context_SD"), "clinical subject")
        self.assertNotEqual(after.get("Host_Context_SD"), "human-associated")

    def test_disease_health_state_split_preserves_conflicting_destination(self) -> None:
        payload = {
            "Host_Disease_SD": "healthy/no disease reported",
            "Host_Health_State_SD": "symptomatic",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Host_Disease_SD"), "")
        self.assertEqual(after.get("Host_Health_State_SD"), "symptomatic")
        self.assertEqual(result["conflicts"], [])
        self.assertTrue(any("existing nonblank destination preserved" in event["detail"] for event in result["noops"]))

    def test_csf_sets_anatomical_material_only_with_host_evidence(self) -> None:
        with_host = {
            "Isolation_Site_SD": "cerebrospinal fluid",
            "Host_Context_SD": "clinical/host-associated material",
        }
        result = run_payload(with_host)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "cerebrospinal fluid")
        self.assertEqual(after.get("Host_Anatomical_Material_SD"), "cerebrospinal fluid")

        no_host = {"Isolation_Site_SD": "cerebrospinal fluid"}
        result = run_payload(no_host)
        after = result["after"]
        self.assertEqual(after.get("Sample_Material_SD"), "cerebrospinal fluid")
        self.assertNotEqual(after.get("Host_Anatomical_Material_SD"), "cerebrospinal fluid")
        self.assertTrue(any(event["rule_id"] == "PH2A-SITE-CSF-ANAT-MATERIAL" for event in result["evidence_failures"]))

    def test_pus_sets_anatomical_material_only_with_host_evidence(self) -> None:
        payload = {"Isolation_Site_SD": "pus", "Host_SD": "Homo sapiens", "Host_TaxID": "9606"}
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "pus/purulent material")
        self.assertEqual(after.get("Host_Anatomical_Material_SD"), "pus/purulent material")

    def test_catheter_requires_collection_device_evidence(self) -> None:
        payload = {"Isolation_Site_SD": "catheter", "Collection Device": "catheter"}
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Collection_Device_SD"), "catheter")

        unsupported = {"Isolation_Site_SD": "catheter"}
        result = run_payload(unsupported)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertNotEqual(after.get("Sample_Collection_Device_SD"), "catheter")
        self.assertTrue(any(event["rule_id"] == "PH2A-SITE-CATHETER-DEVICE" for event in result["evidence_failures"]))

    def test_plant_material_is_not_forced_without_material_evidence(self) -> None:
        payload = {"Isolation_Site_SD": "plant-associated material"}
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Host_Context_SD"), "plant-associated")
        self.assertEqual(after.get("Sampling_Context_SD"), "plant-associated")
        self.assertNotEqual(after.get("Sample_Material_SD"), "plant material")

        supported = {
            "Isolation_Site_SD": "plant-associated material",
            "BioSample Tissue": "leaf tissue from plant-associated material",
        }
        result = run_payload(supported)
        self.assertEqual(result["after"].get("Sample_Material_SD"), "plant material")

    def test_legacy_fields_are_preserved_for_material_site_correction(self) -> None:
        payload = {
            "Isolation_Site_SD": "manure",
            "Sample_Type_SD": "manure",
            "Sample_Type_SD_Broad": "organic material",
            "Isolation_Source_SD": "farm manure",
            "Isolation_Source_SD_Broad": "agricultural material",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "manure/fecal material")
        self.assertEqual(after.get("Environment_Medium_SD"), "agricultural organic material")
        self.assert_legacy_preserved(payload, after)
        self.assertEqual(result["legacy_changed"], [])

    def test_allowlist_has_no_legacy_destination_writes(self) -> None:
        rules = load_rules(DEFAULT_RULES)
        self.assertGreaterEqual(len(rules), 18)
        for rule in rules:
            self.assertNotIn(rule.destination_field, {"Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad"})
            self.assertEqual(rule.reviewer_status, "approved_phase2a_dry_run")


if __name__ == "__main__":
    unittest.main()
