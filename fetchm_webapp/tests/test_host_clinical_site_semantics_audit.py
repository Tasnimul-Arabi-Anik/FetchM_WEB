from __future__ import annotations

import unittest

from tools.audit_host_clinical_site_semantics import (
    additive_destinations,
    action_class,
    classify_value,
    expected_classes,
    field_compatibility,
    incompatibility_rows,
    remove_from_current_field,
)


def classified_row(field: str, value: str, row_count: int = 7) -> dict[str, str | int]:
    classified = classify_value(value, field=field)
    return {
        "field": field,
        "standardized_value": value,
        "primary_semantic_class": classified.primary_semantic_class,
        "secondary_semantic_classes": "|".join(classified.secondary_semantic_classes),
        "semantic_components": "|".join(classified.semantic_components),
        "confidence": classified.confidence,
        "field_compatibility": field_compatibility(field, classified),
        "additive_destinations": "|".join(additive_destinations(classified, field)),
        "remove_from_current_field": str(remove_from_current_field(field, classified)).lower(),
        "review_required": str(action_class(field, classified) in {"manual_review", "classifier_uncertain", "composite_requires_split", "confirmed_high_confidence_fix"}).lower(),
        "action_class": action_class(field, classified),
        "row_count": row_count,
        "reason": classified.reason,
        "expected_semantic_classes": "|".join(sorted(expected_classes(field))),
    }


class HostClinicalSiteSemanticsAuditTests(unittest.TestCase):
    def assert_has_classes(self, value: str, expected: set[str], field: str = "") -> None:
        classified = classify_value(value, field=field)
        self.assertTrue(
            expected <= set(classified.all_classes),
            f"{value!r} classified as {classified.all_classes}, missing {expected}",
        )

    def assert_lacks_classes(self, value: str, rejected: set[str], field: str = "") -> None:
        classified = classify_value(value, field=field)
        self.assertFalse(
            rejected & set(classified.all_classes),
            f"{value!r} unexpectedly classified as {classified.all_classes}",
        )

    def test_classifies_known_axis_splits(self) -> None:
        expectations = {
            "patient": {"host_context"},
            "hospitalized": {"hospitalization_status"},
            "control": {"study_group"},
            "carrier": {"colonization_status"},
            "dead": {"vital_status"},
            "convalescent": {"disease_stage"},
            "pus": {"sample_material", "anatomical_material"},
            "sink": {"environment_local_scale"},
            "DNA extract": {"sample_material", "sample_processing"},
            "clinical/host-associated material": {"source_context"},
        }
        for value, expected in expectations.items():
            with self.subTest(value=value):
                self.assert_has_classes(value, expected)

    def test_compound_sample_phrases_are_not_generic_descriptors(self) -> None:
        for value in ["environmental sample", "respiratory sample", "cloacal sample"]:
            with self.subTest(value=value):
                classified = classify_value(value)
                self.assertNotEqual(classified.primary_semantic_class, "metadata_descriptor")
                self.assertIn("sampling_context", classified.all_classes)
        self.assert_has_classes("respiratory sample", {"sampling_context", "anatomical_site"})
        self.assert_has_classes("cloacal sample", {"sampling_context", "anatomical_site"})

    def test_food_contact_surface_is_not_study_group(self) -> None:
        self.assert_has_classes("food contact surface", {"food_commodity", "environment_local_scale"})
        self.assert_lacks_classes("food contact surface", {"study_group"})

    def test_care_facility_terms_are_not_disease_stage(self) -> None:
        self.assert_has_classes("acute care hospital", {"care_setting", "environment_local_scale"})
        self.assert_lacks_classes("acute care hospital", {"disease_stage"})
        self.assert_has_classes("chronic care facility", {"care_setting", "environment_local_scale"})
        self.assert_lacks_classes("chronic care facility", {"disease_stage"})

    def test_food_phrase_protects_against_environment_only_classification(self) -> None:
        classified = classify_value("freshwater fish product")
        self.assertIn("food_commodity", classified.all_classes)
        self.assertNotIn("environment_medium", classified.all_classes)

    def test_compositional_values_keep_multiple_axes(self) -> None:
        cases = {
            "healthy control": {"health_state", "study_group"},
            "rectal swab": {"sample_material", "collection_device", "anatomical_site"},
            "sink biofilm": {"environment_local_scale", "environment_medium"},
            "gastric biopsy": {"sample_material", "collection_method", "anatomical_site"},
            "manure": {"sample_material", "environment_medium"},
            "abscess": {"sample_material", "anatomical_site", "disease"},
            "cell culture": {"sample_entity", "sample_processing"},
            "specific pathogen free": {"production_context"},
            "patient sample": {"host_context", "sampling_context"},
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assert_has_classes(value, expected)
        self.assert_lacks_classes("patient sample", {"health_state"})


    def test_residual_false_positive_protections(self) -> None:
        self.assert_has_classes("skin/body-surface swab", {"sample_material", "collection_device", "anatomical_site"})
        self.assert_lacks_classes("skin/body-surface swab", {"environment_local_scale"})
        self.assert_has_classes("dairy farm", {"production_context", "host_context", "environment_local_scale"})
        self.assert_lacks_classes("dairy farm", {"food_commodity"})
        self.assert_has_classes("non-food-contact surface", {"environment_local_scale"})
        self.assert_lacks_classes("non-food-contact surface", {"food_commodity"})
        self.assert_has_classes("laboratory culture", {"sample_entity", "sample_processing", "sampling_context"})
        self.assert_lacks_classes("laboratory culture", {"environment_local_scale"})

    def test_plant_niches_and_plant_material_are_ecological_not_simple_sites(self) -> None:
        for value in ["rhizosphere", "phyllosphere", "endosphere"]:
            with self.subTest(value=value):
                self.assert_has_classes(value, {"environment_local_scale", "host_context"})
                self.assert_lacks_classes(value, {"anatomical_site"})
        self.assert_has_classes("plant-associated material", {"source_context", "host_context", "sampling_context", "sample_material"})

    def test_exposure_and_outcome_axes(self) -> None:
        self.assert_has_classes("exposure/contact context", {"exposure_context"})
        self.assert_has_classes("household contact", {"exposure_context"})
        self.assert_has_classes("recovered", {"disease_outcome"})
        self.assert_has_classes("fatal outcome", {"disease_outcome"})

    def test_additional_deterministic_terms(self) -> None:
        cases = {
            "ascites": {"sample_material", "anatomical_material"},
            "dental plaque": {"sample_material", "environment_medium", "anatomical_site"},
            "gill": {"anatomical_site"},
            "bloodstream": {"anatomical_site"},
            "secretion": {"sample_material", "anatomical_material"},
            "cold seep": {"environment_local_scale"},
            "estuary": {"environment_local_scale"},
            "glacier": {"environment_local_scale"},
            "anaerobic digester": {"environment_local_scale"},
            "deciduous forest": {"environment_broad_scale", "environment_local_scale"},
            "produced fluids from fractured shale": {"environment_medium", "environment_local_scale"},
            "ice cream": {"food_commodity"},
            "ready-to-eat product": {"food_commodity"},
            "kimchi": {"food_commodity"},
            "prawn product": {"food_commodity"},
            "catfish product": {"food_commodity"},
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assert_has_classes(value, expected)

    def test_metagenomic_assembly_is_data_product_not_sample_entity(self) -> None:
        self.assert_has_classes("metagenomic assembly", {"data_product", "sample_processing"})
        self.assert_lacks_classes("metagenomic assembly", {"sample_entity"})

    def test_raw_attribute_can_add_context(self) -> None:
        classified = classify_value("contact", raw_attribute="host exposure")
        self.assertIn("exposure_context", classified.all_classes)
        classified = classify_value("recovered", raw_attribute="host disease outcome")
        self.assertIn("disease_outcome", classified.all_classes)

    def test_compound_disease_name_does_not_become_blood_material(self) -> None:
        classified = classify_value("banana blood disease")
        self.assertEqual(classified.primary_semantic_class, "disease")
        self.assertNotIn("sample_material", classified.all_classes)

    def test_site_classes_are_not_flagged_as_non_sites(self) -> None:
        rows = [
            classified_row("Isolation_Site_SD", "sterile body site", 10),
            classified_row("Isolation_Site_SD", "organ/tissue site", 5),
        ]
        flags = incompatibility_rows(rows)
        self.assertEqual(flags["non_site_values_in_isolation_site"], [])

    def test_patient_in_health_state_is_flagged_for_host_context(self) -> None:
        flags = incompatibility_rows([classified_row("Host_Health_State_SD", "patient")])
        flagged = flags["non_health_values_in_host_health_state"][0]
        self.assertIn("Host_Context_SD", flagged["proposed_destination"])
        self.assertEqual(flagged["remove_from_current_field"], "true")


    def test_confirmed_fix_scope_is_limited_to_reviewed_strict_fields(self) -> None:
        patient_row = classified_row("Host_Health_State_SD", "patient")
        self.assertEqual(patient_row["action_class"], "confirmed_high_confidence_fix")
        csf_row = classified_row("Isolation_Site_SD", "cerebrospinal fluid")
        self.assertEqual(csf_row["action_class"], "confirmed_high_confidence_fix")
        environment_row = classified_row("Environment_Broad_Scale_SD", "food-associated environment")
        self.assertNotEqual(environment_row["action_class"], "confirmed_high_confidence_fix")
        self.assertEqual(environment_row["action_class"], "manual_review")

    def test_swab_in_sample_type_is_additive_not_removed(self) -> None:
        row = classified_row("Sample_Type_SD", "rectal swab")
        self.assertEqual(row["field_compatibility"], "compatible_composite")
        self.assertEqual(row["remove_from_current_field"], "false")
        self.assertIn("Sample_Collection_Device_SD", row["additive_destinations"])

    def test_material_in_isolation_source_is_decomposition_candidate_not_hard_error(self) -> None:
        row = classified_row("Isolation_Source_SD", "blood")
        self.assertIn(row["field_compatibility"], {"compatible", "compatible_composite"})
        self.assertEqual(row["remove_from_current_field"], "false")


if __name__ == "__main__":
    unittest.main()
