from __future__ import annotations

import unittest

from tools.semantic_phase2a_dry_run import (
    DEFAULT_RULES,
    PROVENANCE_FIELD,
    REMOVAL_PROVENANCE_KEY,
    PROTECTED_FIELDS,
    RAW_EVIDENCE_FIELDS,
    Rule,
    apply_rules_to_payload,
    load_rules,
    present,
    rules_by_current,
    validate_rules,
)


def run_payload(payload: dict[str, str]) -> dict[str, object]:
    rules = load_rules(DEFAULT_RULES)
    return apply_rules_to_payload(payload, rules_by_current(rules))


class SemanticPhase2ADryRunTests(unittest.TestCase):
    def assert_legacy_preserved(self, before: dict[str, str], after: dict[str, object]) -> None:
        for field in ["Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad"]:
            self.assertEqual(after.get(field, ""), before.get(field, ""), field)

    def assert_has_removal_provenance(self, after: dict[str, object], field: str) -> None:
        provenance = after.get(PROVENANCE_FIELD, {})
        self.assertIn(REMOVAL_PROVENANCE_KEY, provenance)
        self.assertTrue(any(event.get("cleared_field") == field for event in provenance[REMOVAL_PROVENANCE_KEY]))

    def test_missing_token_absent_is_not_present(self) -> None:
        for value in ["absent", "not provided", "unavailable", "not available", "unknown", ""]:
            with self.subTest(value=value):
                self.assertFalse(present(value))

    def test_patient_human_clinical_specimen_gets_subject_and_human_context(self) -> None:
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
        self.assert_has_removal_provenance(after, "Host_Health_State_SD")
        self.assert_legacy_preserved(payload, after)
        self.assertEqual(result["conflicts"], [])

    def test_patient_nonhuman_veterinary_specimen_does_not_infer_human(self) -> None:
        payload = {
            "Host_Health_State_SD": "patient",
            "Host_SD": "Bos taurus",
            "Host_TaxID": "9913",
            "Sample_Type_SD": "clinical sample",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Host_Health_State_SD"), "")
        self.assertEqual(after.get("Sampling_Context_SD"), "clinical subject")
        self.assertNotEqual(after.get("Host_Context_SD"), "human-associated")
        self.assertTrue(any(event["rule_id"] == "PH2A-HHS-PATIENT-HUMAN-CONTEXT" for event in result["conditional_skips"]))

    def test_patient_without_clinical_subject_evidence_is_removal_only(self) -> None:
        for payload in [
            {"Host_Health_State_SD": "patient"},
            {"Host_Health_State_SD": "patient", "Host": "absent"},
            {"Host_Health_State_SD": "patient", "Isolation_Source_SD": "hospital wastewater", "Sample_Type_SD": "hospital wastewater"},
            {"Host_Health_State_SD": "patient", "Isolation_Source_SD": "sink drain surface"},
        ]:
            with self.subTest(payload=payload):
                result = run_payload(payload)
                after = result["after"]
                self.assertEqual(after.get("Host_Health_State_SD"), "")
                self.assertNotEqual(after.get("Sampling_Context_SD"), "clinical subject")
                self.assertNotEqual(after.get("Host_Context_SD"), "human-associated")
                self.assert_has_removal_provenance(after, "Host_Health_State_SD")
                self.assertTrue(any(event["rule_id"] == "PH2A-HHS-PATIENT-SAMPLING" for event in result["conditional_skips"]))

    def test_disease_health_state_split_records_same_and_existing_different(self) -> None:
        same = {"Host_Disease_SD": "healthy/no disease reported", "Host_Health_State_SD": "healthy"}
        result = run_payload(same)
        self.assertEqual(result["after"].get("Host_Disease_SD"), "")
        self.assertTrue(any(event["rule_id"] == "PH2A-HD-HEALTHY-NO-DISEASE" for event in result["already_same"]))

        different = {"Host_Disease_SD": "healthy/no disease reported", "Host_Health_State_SD": "symptomatic"}
        result = run_payload(different)
        self.assertEqual(result["after"].get("Host_Disease_SD"), "")
        self.assertEqual(result["after"].get("Host_Health_State_SD"), "symptomatic")
        self.assertEqual(result["conflicts"], [])
        self.assertTrue(any("existing nonblank destination preserved" in event["detail"] for event in result["existing_different"]))

    def test_csf_sets_anatomical_material_only_with_host_evidence(self) -> None:
        with_host = {"Isolation_Site_SD": "cerebrospinal fluid", "Host_Context_SD": "clinical/host-associated material"}
        result = run_payload(with_host)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "cerebrospinal fluid")
        self.assertEqual(after.get("Host_Anatomical_Material_SD"), "cerebrospinal fluid")
        self.assert_has_removal_provenance(after, "Isolation_Site_SD")

        no_host = {"Isolation_Site_SD": "cerebrospinal fluid"}
        result = run_payload(no_host)
        after = result["after"]
        self.assertEqual(after.get("Sample_Material_SD"), "cerebrospinal fluid")
        self.assertNotEqual(after.get("Host_Anatomical_Material_SD"), "cerebrospinal fluid")
        self.assertTrue(any(event["rule_id"] == "PH2A-SITE-CSF-ANAT-MATERIAL" for event in result["conditional_skips"]))

    def test_pus_sets_anatomical_material_only_with_host_evidence(self) -> None:
        payload = {"Isolation_Site_SD": "pus", "Host_SD": "Homo sapiens", "Host_TaxID": "9606"}
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "pus/purulent material")
        self.assertEqual(after.get("Host_Anatomical_Material_SD"), "pus/purulent material")

    def test_catheter_is_deferred_from_phase2a(self) -> None:
        for payload in [
            {"Isolation_Site_SD": "catheter", "Collection Device": "catheter"},
            {"Isolation_Site_SD": "catheter"},
        ]:
            with self.subTest(payload=payload):
                result = run_payload(payload)
                self.assertEqual(result["matched_rules"], [])
                self.assertEqual(result["changed_fields"], [])
                self.assertEqual(result["after"].get("Isolation_Site_SD"), "catheter")

    def test_plant_context_is_evidence_gated(self) -> None:
        supported_host = {"Isolation_Site_SD": "plant-associated material", "Host_SD": "Viridiplantae", "Host_TaxID": "33090"}
        result = run_payload(supported_host)
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Host_Context_SD"), "plant-associated")
        self.assertEqual(after.get("Sampling_Context_SD"), "plant-associated")
        self.assertNotEqual(after.get("Sample_Material_SD"), "plant material")

        supported_tissue = {"Isolation_Site_SD": "plant-associated material", "BioSample Tissue": "leaf tissue"}
        result = run_payload(supported_tissue)
        self.assertEqual(result["after"].get("Host_Context_SD"), "plant-associated")
        self.assertEqual(result["after"].get("Sample_Material_SD"), "plant material")

        for unsupported in [
            {"Isolation_Site_SD": "plant-associated material", "Host_SD": "Aves", "Host_TaxID": "8782"},
            {"Isolation_Site_SD": "plant-associated material", "Host_SD": "Bos taurus", "Host_TaxID": "9913"},
            {"Isolation_Site_SD": "plant-associated material", "Isolation_Source_SD": "drain water"},
            {"Isolation_Site_SD": "plant-associated material"},
        ]:
            with self.subTest(unsupported=unsupported):
                result = run_payload(unsupported)
                after = result["after"]
                self.assertEqual(after.get("Isolation_Site_SD"), "")
                self.assertNotEqual(after.get("Host_Context_SD"), "plant-associated")
                self.assertNotEqual(after.get("Sampling_Context_SD"), "plant-associated")
                self.assertNotEqual(after.get("Sample_Material_SD"), "plant material")
                self.assert_has_removal_provenance(after, "Isolation_Site_SD")

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


    def test_manure_medium_enrichment_only_when_blank(self) -> None:
        blank = {"Isolation_Site_SD": "manure"}
        result = run_payload(blank)
        self.assertEqual(result["after"].get("Environment_Medium_SD"), "agricultural organic material")

        for existing in ["manure", "agricultural organic material", "soil"]:
            payload = {"Isolation_Site_SD": "manure", "Environment_Medium_SD": existing}
            with self.subTest(existing=existing):
                result = run_payload(payload)
                self.assertEqual(result["after"].get("Environment_Medium_SD"), existing)
                self.assertTrue(any(event["rule_id"] == "PH2A-SITE-MANURE-MEDIUM" for event in result["conditional_skips"]))
                self.assertFalse(any(event["rule_id"] == "PH2A-SITE-MANURE-MEDIUM" for event in result["existing_different"]))

    def test_allowlist_has_no_legacy_or_protected_destination_writes(self) -> None:
        rules = load_rules(DEFAULT_RULES)
        self.assertGreaterEqual(len(rules), 18)
        blocked = set(PROTECTED_FIELDS) | set(RAW_EVIDENCE_FIELDS) | {"Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad"}
        for rule in rules:
            if rule.destination_field:
                self.assertNotIn(rule.destination_field, blocked)
            self.assertEqual(rule.reviewer_status, "approved_phase2a_dry_run")

    def test_validate_rules_rejects_protected_destination(self) -> None:
        with self.assertRaises(ValueError):
            validate_rules([
                Rule(
                    rule_id="bad",
                    current_field="Host_Health_State_SD",
                    current_value="patient",
                    clear_current_field=False,
                    destination_field="Country",
                    destination_value="Bangladesh",
                    destination_condition="always",
                    removal_confidence="high",
                    destination_confidence="high",
                    merge_policy="set_when_blank_or_same",
                    evidence_requirement="bad",
                    preserve_legacy_fields=True,
                    reviewer_status="approved_phase2a_dry_run",
                    rationale="bad",
                    source_audit_commit="test",
                )
            ])


if __name__ == "__main__":
    unittest.main()
