from __future__ import annotations

import unittest

from app import (
    broad_standardization_category,
    ensure_managed_metadata_schema,
    extract_country,
    standardize_host_metadata,
)


class MetadataStandardizationRegressionTests(unittest.TestCase):
    def test_geography_false_positive_guards(self) -> None:
        self.assertIsNone(extract_country("Hospital"))
        self.assertIsNone(extract_country("St Margaret's Hospital"))
        self.assertIsNone(extract_country("Outpatient"))
        self.assertIsNone(extract_country("ground turkey"))
        self.assertIsNone(extract_country("Guinea pig"))
        self.assertIsNone(extract_country("Norway rat"))
        self.assertIsNone(extract_country("Aspergillus niger"))

    def test_valid_geography_still_maps(self) -> None:
        bangladesh = ensure_managed_metadata_schema({"Geographic Location": "Bangladesh: Dhaka"})
        self.assertEqual(bangladesh["Country"], "Bangladesh")
        self.assertEqual(bangladesh["Continent"], "Asia")
        united_states = ensure_managed_metadata_schema({"Geographic Location": "United States: California"})
        self.assertEqual(united_states["Country"], "United States")
        self.assertEqual(united_states["Continent"], "North America")

    def test_host_sample_source_separation(self) -> None:
        human = ensure_managed_metadata_schema({"Host": "", "Sample Type": "human"})
        self.assertEqual(human["Sample_Type_SD"], "")

        human_blood = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human blood"})
        self.assertEqual(human_blood["Host_SD"], "Homo sapiens")
        self.assertEqual(human_blood["Sample_Type_SD"], "blood")

        human_feces = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human feces"})
        self.assertEqual(human_feces["Host_SD"], "Homo sapiens")
        self.assertEqual(human_feces["Sample_Type_SD"], "feces/stool")
        self.assertNotEqual(human_feces["Environment_Medium_SD"], "feces/stool")

        bacteria_culture = standardize_host_metadata("bacteria culture")
        self.assertEqual(bacteria_culture["Host_SD"], "")
        self.assertEqual(bacteria_culture["Host_TaxID"], "")

        dh5a = standardize_host_metadata("DH5a")
        self.assertEqual(dh5a["Host_SD"], "")
        self.assertEqual(dh5a["Host_TaxID"], "")

        xl10 = standardize_host_metadata("XL10-gold")
        self.assertEqual(xl10["Host_SD"], "")
        self.assertEqual(xl10["Host_TaxID"], "")

        patient = ensure_managed_metadata_schema({"Host": "", "Sample Type": "patient"})
        self.assertEqual(patient["Sample_Type_SD"], "")

        cattle_feces = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "cattle feces"})
        self.assertEqual(cattle_feces["Host_SD"], "Bos taurus")
        self.assertEqual(cattle_feces["Sample_Type_SD"], "feces/stool")

        water_deer = ensure_managed_metadata_schema({"Host": "water deer", "Isolation Source": ""})
        self.assertEqual(water_deer["Host_SD"], "Hydropotes inermis")
        self.assertNotEqual(water_deer["Environment_Medium_SD"], "water")

        water_buffalo = ensure_managed_metadata_schema({"Host": "water buffalo", "Isolation Source": ""})
        self.assertEqual(water_buffalo["Host_SD"], "Bubalus bubalis")
        self.assertNotEqual(water_buffalo["Environment_Medium_SD"], "water")

    def test_environment_medium_examples(self) -> None:
        feces = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "feces/stool"})
        self.assertNotEqual(feces["Environment_Medium_SD"], "feces/stool")

        soil = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "soil"})
        self.assertEqual(soil["Environment_Medium_SD"], "soil")

        wastewater = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "wastewater"})
        self.assertEqual(wastewater["Environment_Medium_SD"], "wastewater")

        seawater = ensure_managed_metadata_schema({"Host": "", "Environment Medium": "seawater"})
        self.assertEqual(seawater["Environment_Medium_SD"], "seawater")

    def test_broad_categories_do_not_leak_raw_values(self) -> None:
        self.assertEqual(broad_standardization_category("Marmota himalayana"), "")
        self.assertEqual(broad_standardization_category("Nottingham"), "")
        self.assertEqual(broad_standardization_category("Osteomyelitis"), "")
        self.assertEqual(broad_standardization_category("L_cheek"), "")
        self.assertEqual(broad_standardization_category("#REF!"), "")
        self.assertEqual(broad_standardization_category("whole organism"), "host-associated context")
        self.assertEqual(broad_standardization_category("poultry"), "host-associated context")
        self.assertEqual(broad_standardization_category("host-associated organism"), "host-associated context")
        self.assertEqual(
            broad_standardization_category("urogenital/gastrointestinal site"),
            "clinical/host-associated material",
        )
        self.assertEqual(broad_standardization_category("Klíčava reservoir"), "water")
        self.assertEqual(broad_standardization_category("hydrothermal vent"), "environmental/geologic material")
        self.assertEqual(broad_standardization_category("Wall biofilm"), "biofilm")
        self.assertEqual(broad_standardization_category("rectal swab"), "swab")
        self.assertEqual(broad_standardization_category("urogenital/reproductive swab"), "swab")
        self.assertEqual(broad_standardization_category("river water"), "water")

    def test_body_site_sample_source_separation(self) -> None:
        rectal_swab = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "rectal swab"})
        self.assertEqual(rectal_swab["Sample_Type_SD"], "rectal swab")
        self.assertEqual(rectal_swab["Isolation_Site_SD"], "rectum/perianal region")
        self.assertEqual(rectal_swab["Host_Anatomical_Site_SD"], "rectum/perianal region")
        self.assertEqual(rectal_swab["Isolation_Source_SD"], "clinical/host-associated material")

        nasal_swab = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "nasal swab"})
        self.assertEqual(nasal_swab["Sample_Type_SD"], "nasal swab")
        self.assertEqual(nasal_swab["Isolation_Site_SD"], "nasal cavity/sinus/upper respiratory tract")

        bronchial_lavage = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "bronchial lavage"})
        self.assertEqual(bronchial_lavage["Sample_Type_SD"], "bronchial wash/lavage")
        self.assertEqual(bronchial_lavage["Isolation_Site_SD"], "lower respiratory tract/bronch/pleural cavity")

        pleural_fluid = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "pleural fluid"})
        self.assertEqual(pleural_fluid["Sample_Type_SD"], "pleural fluid")
        self.assertEqual(pleural_fluid["Isolation_Source_SD"], "clinical fluid/material")

        dental_plaque = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "dental plaque"})
        self.assertEqual(dental_plaque["Sample_Type_SD"], "dental plaque")
        self.assertEqual(dental_plaque["Sample_Type_SD_Broad"], "clinical/host-associated material")
        self.assertEqual(dental_plaque["Isolation_Site_SD"], "oral cavity")

        perineum = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Perineum"})
        self.assertEqual(perineum["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(perineum["Isolation_Site_SD"], "skin/body surface")

        nasal_context = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Healthcare worker (nasal)"})
        self.assertEqual(nasal_context["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(nasal_context["Isolation_Site_SD"], "nasal cavity/sinus/upper respiratory tract")

        conjunctiva = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Right conjunctiva of a child"})
        self.assertEqual(conjunctiva["Isolation_Source_SD"], "clinical/host-associated material")
        self.assertEqual(conjunctiva["Isolation_Site_SD"], "organ/tissue site")

    def test_food_cut_terms_are_not_clinical_anatomy(self) -> None:
        retail_breast = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Retail Breast"})
        self.assertEqual(retail_breast["Sample_Type_SD"], "poultry meat")
        self.assertEqual(retail_breast["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(retail_breast["Isolation_Site_SD"], "")

        turkey_sandwich = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "turkey breast sandwich"})
        self.assertEqual(turkey_sandwich["Sample_Type_SD"], "poultry meat")
        self.assertEqual(turkey_sandwich["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(turkey_sandwich["Isolation_Site_SD"], "")

        ground_breast = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Ground (breast)"})
        self.assertEqual(ground_breast["Sample_Type_SD"], "poultry meat")
        self.assertEqual(ground_breast["Isolation_Source_SD_Broad"], "food/meat")
        self.assertEqual(ground_breast["Isolation_Site_SD"], "")

        human_breast_milk = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "human breast milk"})
        self.assertEqual(human_breast_milk["Host_SD"], "Homo sapiens")
        self.assertEqual(human_breast_milk["Sample_Type_SD"], "milk")
        self.assertEqual(human_breast_milk["Host_Anatomical_Site_SD"], "breast")

    def test_disease_and_lab_artifacts_do_not_leak_as_source(self) -> None:
        osteomyelitis = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Osteomyelitis"})
        self.assertEqual(osteomyelitis["Host_Disease_SD"], "osteomyelitis")
        self.assertEqual(osteomyelitis["Host_Health_State_SD"], "diseased")
        self.assertEqual(osteomyelitis["Isolation_Source_SD"], "clinical/host-associated material")

        aborted = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "aborted uteroplacental unit"})
        self.assertEqual(aborted["Host_Disease_SD"], "abortion/reproductive disorder")
        self.assertEqual(aborted["Host_Health_State_SD"], "diseased")
        self.assertEqual(aborted["Isolation_Source_SD"], "clinical/host-associated material")

        leukemia = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Leukemia cell line (SEM)"})
        self.assertEqual(leukemia["Host_Disease_SD"], "leukemia")
        self.assertEqual(leukemia["Isolation_Source_SD"], "clinical/host-associated material")

        derived_strain = ensure_managed_metadata_schema(
            {"Host": "", "Isolation Source": "derived from the strain Pseudomonas aeruginosa ATCC 27853"}
        )
        self.assertEqual(derived_strain["Isolation_Source_SD"], "culture")
        self.assertEqual(derived_strain["Host_SD"], "")

        spreadsheet_error = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "#REF!"})
        self.assertEqual(spreadsheet_error["Isolation_Source_SD"], "")
        self.assertEqual(spreadsheet_error["Sample_Type_SD"], "")

        facility = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "Facility 4"})
        self.assertEqual(facility["Isolation_Source_SD"], "healthcare-associated environment")

        raw_code = ensure_managed_metadata_schema({"Host": "", "Isolation Source": "cxwnd"})
        self.assertEqual(raw_code["Isolation_Source_SD"], "metadata descriptor / non-source")


if __name__ == "__main__":
    unittest.main()
