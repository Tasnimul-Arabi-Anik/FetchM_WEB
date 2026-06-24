from __future__ import annotations

import unittest

from tools.semantic_phase2a_dry_run import DEFAULT_RULES, PROVENANCE_FIELD, load_rules, rules_by_current
from tools.source_environment_semantic_completion import (
    apply_semantic_completion,
    apply_completion_rules,
    rules,
    validate_completion_rules,
)


def run_payload(payload: dict[str, str]) -> dict[str, object]:
    phase2a_lookup = rules_by_current(load_rules(DEFAULT_RULES))
    return apply_semantic_completion(payload, phase2a_lookup, rules())


class SourceEnvironmentSemanticCompletionTests(unittest.TestCase):
    def test_completion_rules_are_valid(self) -> None:
        validate_completion_rules(rules())

    def test_registers_phase2a_populated_fields_only(self) -> None:
        app_text = __import__("pathlib").Path("app.py").read_text()
        marker = "SEMANTIC_AXIS_STANDARDIZATION_COLUMNS = ["
        start = app_text.index(marker)
        end = app_text.index("]", start)
        column_block = app_text[start:end]
        SEMANTIC_AXIS_STANDARDIZATION_COLUMNS = [
            line.split("\"")[1]
            for line in column_block.splitlines()
            if line.strip().startswith("\"")
        ]

        self.assertEqual(
            set(SEMANTIC_AXIS_STANDARDIZATION_COLUMNS),
            {
                "Sample_Material_SD",
                "Sampling_Context_SD",
                "Host_Anatomical_Material_SD",
                "Host_Hospitalization_Status_SD",
                "Host_Vital_Status_SD",
                "Host_Colonization_Status_SD",
                "Host_Disease_Stage_SD",
                "Host_Exposure_Context_SD",
                "Semantic_Axis_Provenance",
            },
        )
        self.assertNotIn("Sample_Processing_SD", SEMANTIC_AXIS_STANDARDIZATION_COLUMNS)
        self.assertNotIn("Sample_Collection_Device_SD", SEMANTIC_AXIS_STANDARDIZATION_COLUMNS)
        self.assertNotIn("Host_Study_Group_SD", SEMANTIC_AXIS_STANDARDIZATION_COLUMNS)

    def test_phase2a_site_correction_composes_with_environment_completion(self) -> None:
        result = run_payload({"Isolation_Site_SD": "manure"})
        after = result["after"]
        self.assertEqual(after.get("Isolation_Site_SD"), "")
        self.assertEqual(after.get("Sample_Material_SD"), "manure/fecal material")
        self.assertEqual(after.get("Environment_Medium_SD"), "agricultural organic material")
        self.assertEqual(result["legacy_changed"], [])
        self.assertIn(PROVENANCE_FIELD, after)

    def test_catheter_remains_deferred(self) -> None:
        result = run_payload({"Isolation_Site_SD": "catheter", "Collection Device": "catheter"})
        self.assertEqual(result["after"].get("Isolation_Site_SD"), "catheter")
        self.assertEqual(result["changed_fields"], [])

    def test_environment_medium_addition_requires_blank_or_same_destination(self) -> None:
        blank = run_payload({"Sample_Type_SD": "water"})
        self.assertEqual(blank["after"].get("Environment_Medium_SD"), "water")

        existing = run_payload({"Sample_Type_SD": "water", "Environment_Medium_SD": "lake water"})
        self.assertEqual(existing["after"].get("Environment_Medium_SD"), "lake water")
        self.assertTrue(
            any(
                event["rule_id"] == "SEC-MEDIUM-WATER" and event["status"] == "preserved_conflict"
                for event in existing["completion_events"]
            )
        )

    def test_environmental_biofilm_does_not_capture_host_biofilm(self) -> None:
        environmental = run_payload({"Sample_Type_SD": "biofilm"})
        self.assertEqual(environmental["after"].get("Environment_Medium_SD"), "biofilm")

        dental = run_payload({"Sample_Type_SD": "biofilm", "Isolation_Source_SD": "dental plaque"})
        self.assertNotEqual(dental["after"].get("Environment_Medium_SD"), "biofilm")
        self.assertTrue(
            any(event["rule_id"] == "SEC-MEDIUM-BIOFILM" and event["status"] == "ambiguous" for event in dental["completion_events"])
        )

    def test_context_dependent_values_are_not_blanket_mapped(self) -> None:
        canal = run_payload({"Sample_Type_SD": "canal"})
        self.assertEqual(canal["after"].get("Environment_Local_Scale_SD", ""), "")
        self.assertEqual(canal["after"].get("Environment_Medium_SD", ""), "")

        drainage = run_payload({"Sample_Type_SD": "drainage"})
        self.assertEqual(drainage["after"].get("Environment_Local_Scale_SD", ""), "")

        surface = run_payload({"Sample_Type_SD": "surface"})
        self.assertEqual(surface["after"].get("Environment_Local_Scale_SD", ""), "")

    def test_local_and_broad_environment_additions(self) -> None:
        sink = run_payload({"Sample_Type_SD": "sink"})
        self.assertEqual(sink["after"].get("Environment_Local_Scale_SD"), "sink")

        farm = run_payload({"Sample_Type_SD": "dairy farm"})
        self.assertEqual(farm["after"].get("Environment_Local_Scale_SD"), "farm")
        self.assertEqual(farm["after"].get("Environment_Broad_Scale_SD"), "agricultural environment")

        terrestrial = run_payload({"Sample_Type_SD": "terrestrial environment"})
        self.assertEqual(terrestrial["after"].get("Environment_Broad_Scale_SD"), "terrestrial environment")

    def test_rhizosphere_adds_local_scale_and_plant_context(self) -> None:
        result = run_payload({"Sample_Type_SD": "rhizosphere"})
        after = result["after"]
        self.assertEqual(after.get("Environment_Local_Scale_SD"), "rhizosphere")
        self.assertEqual(after.get("Host_Context_SD"), "plant-associated")
        self.assertEqual(after.get("Environment_Medium_SD", ""), "")

    def test_material_completion_preserves_legacy_source(self) -> None:
        payload = {
            "Isolation_Source_SD": "blood",
            "Isolation_Source_SD_Broad": "clinical/host-associated material",
            "Host_SD": "Homo sapiens",
            "Host_TaxID": "9606",
        }
        result = run_payload(payload)
        after = result["after"]
        self.assertEqual(after.get("Sample_Material_SD"), "blood")
        self.assertEqual(after.get("Host_Anatomical_Material_SD"), "blood")
        self.assertEqual(after.get("Isolation_Source_SD"), "blood")
        self.assertEqual(result["legacy_changed"], [])

    def test_material_completion_does_not_overwrite_existing_material(self) -> None:
        result = run_payload({"Isolation_Source_SD": "blood", "Sample_Material_SD": "urine"})
        self.assertEqual(result["after"].get("Sample_Material_SD"), "urine")
        self.assertTrue(
            any(
                event["rule_id"] == "SEC-MATERIAL-BLOOD" and event["status"] == "preserved_conflict"
                for event in result["completion_events"]
            )
        )

    def test_raw_substring_does_not_create_material_false_positive(self) -> None:
        result = run_payload({"Isolation Source": "banana blood disease"})
        self.assertNotEqual(result["after"].get("Sample_Material_SD"), "blood")

    def test_apply_completion_rules_is_idempotent(self) -> None:
        first = run_payload({"Sample_Type_SD": "stream"})["after"]
        second = apply_completion_rules(first, rules())["after"]
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
