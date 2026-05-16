from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import app as fetchm_app
from app import (
    apply_pass_fail_decision_mode,
    apply_quality_post_filters,
    apply_sequence_filters,
    broad_standardization_category,
    build_qc_decision_preview,
    build_quality_config,
    ensure_managed_metadata_schema,
    extract_country,
    import_nextflow_qc_outputs,
    dedupe_reason_text,
    run_sequence_quality_checks,
    should_expose_output_file,
    standardize_host_metadata,
)
from external_tools.quality_check.runner import validate_quality_runtime
from global_insights.generator import generate_demo_snapshot, generate_global_insights_snapshot, run_standardization_simulator


class MetadataStandardizationRegressionTests(unittest.TestCase):
    def test_dataset_pipeline_derives_species_after_genus_standardization(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    db = fetchm_app.get_db()
                    self.assertEqual(
                        fetchm_app.dataset_pipeline_steps_for_start("metadata", db),
                        ["metadata", "standardization", "derive_species", "verify"],
                    )
                    self.assertLess(
                        fetchm_app.dataset_pipeline_step_keys().index("standardization"),
                        fetchm_app.dataset_pipeline_step_keys().index("derive_species"),
                    )
                    self.assertIn("genus", fetchm_app.DATASET_PIPELINE_STEP_COPY["standardization"]["short"].lower())
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_standardization_reuses_unchanged_rows_and_forces_when_requested(self) -> None:
        row = {
            "Assembly Accession": "GCA_000001.1",
            "Assembly Name": "ASM1",
            "Organism Name": "Escherichia coli strain test",
            "Host": "human",
            "Isolation Source": "stool",
            "Geographic Location": "USA",
            "Collection Date": "2024",
        }
        first = ensure_managed_metadata_schema(dict(row))
        fingerprint_column = fetchm_app.METADATA_STANDARDIZATION_INPUT_FINGERPRINT_COLUMN
        self.assertTrue(first.get(fingerprint_column))

        legacy_reusable = dict(first)
        legacy_reusable.pop(fingerprint_column, None)
        legacy_reusable.pop(fetchm_app.METADATA_STANDARDIZATION_UPDATED_AT_COLUMN, None)
        legacy_reusable["Host_SD"] = "Existing standardized host"
        seeded = ensure_managed_metadata_schema(legacy_reusable)
        self.assertEqual(seeded["Host_SD"], "Existing standardized host")
        self.assertTrue(seeded.get(fingerprint_column))

        changed_same_input = dict(seeded)
        changed_same_input["Host_SD"] = "Preserved standardized host"
        reused = ensure_managed_metadata_schema(changed_same_input)
        self.assertEqual(reused["Host_SD"], "Preserved standardized host")

        forced = ensure_managed_metadata_schema(changed_same_input, force_standardization=True)
        self.assertNotEqual(forced["Host_SD"], "Preserved standardized host")

        changed_raw_input = dict(reused)
        changed_raw_input["Host"] = "mouse"
        refreshed = ensure_managed_metadata_schema(changed_raw_input)
        self.assertNotEqual(refreshed["Host_SD"], "Preserved standardized host")

    def test_advanced_quality_job_detail_loads_parent_owner_field(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    parent = fetchm_app.JobRecord(
                        id="parent-standard",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="parent.csv",
                        input_path=str(root / "parent.csv"),
                        output_dir=str(root / "parent_outputs"),
                        log_path=str(root / "parent.log"),
                        command=[],
                        owner_user_id=42,
                        filters={},
                    )
                    advanced = fetchm_app.JobRecord(
                        id="advanced-child",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="advanced.csv",
                        input_path=str(root / "advanced.csv"),
                        output_dir=str(root / "advanced_outputs"),
                        log_path=str(root / "advanced.log"),
                        command=[],
                        owner_user_id=42,
                        filters={"advanced_qc": True, "parent_quality_job_id": parent.id},
                    )
                    fetchm_app.save_job(parent)
                    fetchm_app.save_job(advanced)

                    with fetchm_app.app.test_request_context("/jobs/advanced-child"):
                        loaded = fetchm_app.load_job("advanced-child")
                        parent_quality_job = None
                        parent_quality_job_id = str(loaded.filters.get("parent_quality_job_id") or "").strip()
                        if parent_quality_job_id:
                            candidate_parent = fetchm_app.load_job(parent_quality_job_id)
                            if candidate_parent.owner_user_id == loaded.owner_user_id:
                                parent_quality_job = candidate_parent

                    self.assertIsNotNone(parent_quality_job)
                    self.assertEqual(parent_quality_job.id, "parent-standard")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_qc_preview_does_not_display_numeric_ani_closest_as_genome(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_000001.1",
                    "Sequence_QC_Status": "review",
                    "ANI_Closest_ANI": "87.79",
                    "ANI_Closest_Genome": "97.69",
                }
            ]
        )

        preview = build_qc_decision_preview(frame)

        self.assertEqual(preview[0]["ani"], "87.79")
        self.assertEqual(preview[0]["ani_closest"], "")

    def test_qc_preview_treats_zero_ani_as_unresolved(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_000001.1",
                    "Sequence_QC_Status": "review",
                    "Sequence_QC_Review_Reasons": "ani_species_warning:0<95",
                    "ANI_Closest_ANI": "0.0",
                    "ANI_Closest_Genome": "0.00",
                    "ANI_Species_Consistency_Status": "WARN",
                }
            ]
        )

        preview = build_qc_decision_preview(frame)

        self.assertEqual(preview[0]["ani"], "")
        self.assertEqual(preview[0]["ani_closest"], "")
        self.assertEqual(preview[0]["ani_status"], "WARN")
        self.assertEqual(preview[0]["reasons"], "NO_VALID_ANI_RESULT")

    def test_quality_submission_blockers_guard_low_memory_and_active_qc(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            old_reader = fetchm_app.read_memory_usage
            old_min_memory = fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES
            old_max_active = fetchm_app.MAX_ACTIVE_QUALITY_JOBS
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES = 12 * 1024**3
            fetchm_app.MAX_ACTIVE_QUALITY_JOBS = 1
            fetchm_app.read_memory_usage = lambda: {
                "total_bytes": 128 * 1024**3,
                "used_bytes": 124 * 1024**3,
                "available_bytes": 4 * 1024**3,
                "used_percent": 96.9,
                "total_label": "128 GiB",
                "used_label": "124 GiB",
                "available_label": "4 GiB",
            }
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    fetchm_app.save_job(
                        fetchm_app.JobRecord(
                            id="active-qc",
                            mode="qc",
                            status="running",
                            created_at=fetchm_app.utc_now(),
                            updated_at=fetchm_app.utc_now(),
                            input_name="input.csv",
                            input_path=str(root / "input.csv"),
                            output_dir=str(root / "outputs"),
                            log_path=str(root / "job.log"),
                            command=[],
                            owner_user_id=1,
                        )
                    )

                    blockers = fetchm_app.quality_submission_blockers()

                    self.assertTrue(any("low on available memory" in item for item in blockers))
                    self.assertTrue(any("memory load is too high" in item for item in blockers))
                    self.assertTrue(any("quality job(s) are already queued or running" in item for item in blockers))
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths
                fetchm_app.read_memory_usage = old_reader
                fetchm_app.MIN_AVAILABLE_MEMORY_FOR_QUALITY_BYTES = old_min_memory
                fetchm_app.MAX_ACTIVE_QUALITY_JOBS = old_max_active

    def test_qc_reason_text_is_deduplicated(self) -> None:
        self.assertEqual(
            dedupe_reason_text(
                "min_completeness:87.48<90;min_completeness:87.48<90",
                " min_completeness:87.48<90 ",
            ),
            "min_completeness:87.48<90",
        )

    def test_cancelled_running_job_reconciles_when_worker_claim_is_gone(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    job = fetchm_app.JobRecord(
                        id="stale-cancel",
                        mode="qc",
                        status="running",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="input.csv",
                        input_path=str(root / "input.csv"),
                        output_dir=str(root / "outputs"),
                        log_path=str(root / "data" / "jobs" / "stale-cancel" / "job.log"),
                        command=[],
                        return_code=None,
                        cancel_requested=True,
                        claimed_by="dead-worker:123",
                        claimed_at=fetchm_app.utc_now(),
                    )
                    fetchm_app.save_job(job)

                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(), 1)
                    updated = fetchm_app.load_job("stale-cancel")

                    self.assertEqual(updated.status, "cancelled")
                    self.assertEqual(updated.return_code, 1)
                    self.assertIsNone(updated.claimed_by)
                    self.assertIn("no live worker claim", updated.error or "")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_worker_reconciles_own_cancelled_job_after_returning_to_queue(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    worker_name = "live-worker:456"
                    fetchm_app.touch_worker_heartbeat(worker_name)
                    job = fetchm_app.JobRecord(
                        id="own-stale-cancel",
                        mode="qc",
                        status="running",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="input.csv",
                        input_path=str(root / "input.csv"),
                        output_dir=str(root / "outputs"),
                        log_path=str(root / "data" / "jobs" / "own-stale-cancel" / "job.log"),
                        command=[],
                        return_code=None,
                        cancel_requested=True,
                        claimed_by=worker_name,
                        claimed_at=fetchm_app.utc_now(),
                    )
                    fetchm_app.save_job(job)

                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(), 0)
                    self.assertEqual(fetchm_app.reconcile_cancelled_running_jobs(worker_name), 1)
                    updated = fetchm_app.load_job("own-stale-cancel")

                    self.assertEqual(updated.status, "cancelled")
                    self.assertIsNone(updated.claimed_by)
                    self.assertIn("claiming worker returned", updated.error or "")
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

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

    def test_external_nextflow_qc_master_imports_as_canonical_qc_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.csv"
            output_dir = root / "outputs"
            qc_dir = output_dir / "sequence_qc"
            master_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "qc"
            ani_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "ani" / "analysis"
            mash_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "mash" / "analysis"
            gtdbtk_dir = output_dir / "nextflow_qc" / "fetchm_web_qc" / "gtdbtk"
            qc_dir.mkdir(parents=True)
            master_dir.mkdir(parents=True)
            ani_dir.mkdir(parents=True)
            mash_dir.mkdir(parents=True)
            gtdbtk_dir.mkdir(parents=True)

            input_path.write_text(
                "\n".join(
                    [
                        "Assembly Accession,Assembly Name,Organism Name",
                        "GCF_000001.1,ASM1,Klebsiella pneumoniae",
                        "GCF_000002.1,ASM2,Klebsiella pneumoniae",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (master_dir / "qc_master_report.csv").write_text(
                "\n".join(
                    [
                        "Assembly Accession,Assembly Name,sequence_file,sequence_total_length,sequence_num_contigs,sequence_n50,sequence_gc_percent,sequence_ambiguous_bases,checkm2_completeness,checkm2_contamination,ani_closest_ani,ani_species_consistency_status,ani_cluster,gtdbtk_qc_status,gtdbtk_genus,gtdbtk_species,gtdbtk_qc_fail_reasons,qc_master_status,qc_master_fail_reasons,qc_master_warning_reasons",
                        "GCF_000001.1,ASM1,GCF_000001.1_ASM1_genomic.fna,5200000,81,120000,57.3,0,98.4,0.8,99.98,PASS,ANI_CLUSTER_0001,PASS,Klebsiella,Klebsiella pneumoniae,,PASS,,",
                        "GCF_000002.1,ASM2,GCF_000002.1_ASM2_genomic.fna,4100000,300,5000,56.9,10,72.0,8.5,0.0,WARN,ANI_CLUSTER_0002,FAIL,Enterobacter,Enterobacter cloacae,GENUS_MISMATCH,FAIL,CheckM2 completeness below threshold,ani_species_warning:0<95",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (ani_dir / "panr2_ani_summary.csv").write_text(
                "\n".join(
                    [
                        "sample_id,assembly_accession,database,feature_id,feature_category,presence,tool",
                        "GCF_000001.1_ASM1,GCF_000001.1_ASM1,ani,ANI_CLUSTER_0001,ani_cluster,1,skani",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (ani_dir / "ani_run_status.tsv").write_text(
                "tool\tgenome_count\testimated_comparisons\tstrategy\tstatus\tmessage\n"
                "skani\t2\t4\tauto\tPASS\tRunning all-vs-all ANI.\n",
                encoding="utf-8",
            )
            (mash_dir / "closest_mash_neighbor.csv").write_text(
                "\n".join(
                    [
                        "query,reference,mash_distance,p_value,matching_hashes",
                        "GCF_000001.1_ASM1_genomic,GCF_000002.1_ASM2_genomic,0.001,0,950/1000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (mash_dir / "mash_distance_long.csv").write_text(
                "\n".join(
                    [
                        "query,reference,mash_distance,p_value,matching_hashes",
                        "GCF_000001.1_ASM1_genomic,GCF_000001.1_ASM1_genomic,0,0,1000/1000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (gtdbtk_dir / "gtdbtk.bac120.summary.tsv").write_text(
                "user_genome\tclassification\n"
                "GCF_000001.1_ASM1_genomic\td__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Klebsiella;s__Klebsiella pneumoniae\n",
                encoding="utf-8",
            )

            result = import_nextflow_qc_outputs(input_path, output_dir, qc_dir)
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["pass"], 1)
            self.assertEqual(result["fail"], 1)
            self.assertTrue((qc_dir / "external_qc_master_report.csv").exists())
            self.assertTrue((qc_dir / "qc_all_metadata.csv").exists())
            self.assertTrue((qc_dir / "external_ani_summary.csv").exists())
            self.assertTrue((qc_dir / "external_ani_run_status.tsv").exists())
            self.assertTrue((qc_dir / "external_mash_closest_neighbors.csv").exists())
            self.assertTrue((qc_dir / "external_mash_distance_long.csv").exists())
            self.assertTrue((qc_dir / "external_gtdbtk_bac120_summary.tsv").exists())
            decisions = (qc_dir / "qc_decisions.csv").read_text(encoding="utf-8")
            self.assertIn("ANI_Closest_ANI", decisions)
            self.assertIn("Mash_Distance", decisions)
            self.assertIn("GTDBTK_QC_Status", decisions)
            self.assertIn("Klebsiella pneumoniae", decisions)
            self.assertIn("0.001", decisions)
            self.assertIn("NO_VALID_ANI_RESULT", decisions)
            self.assertNotIn("ani_species_warning:0<95", decisions)
            self.assertIn("GCF_000001.1", (qc_dir / "qc_pass_metadata.csv").read_text(encoding="utf-8"))
            self.assertIn("CheckM2 completeness below threshold", (qc_dir / "qc_failed_metadata.csv").read_text(encoding="utf-8"))

    def test_gtdbtk_selection_requires_reference_data(self) -> None:
        errors = validate_quality_runtime(
            {"run_mode": "nextflow", "selected_modules": ["quick_fasta", "gtdbtk"]},
            {
                "nextflow_enabled": True,
                "nextflow_available": True,
                "conda_available": True,
                "nextflow_config_exists": True,
                "nextflow_workflow_exists": True,
                "available_tools": {"gtdbtk": False},
                "gtdbtk_data_path_exists": False,
                "gtdbtk_data_ready": False,
                "nextflow_managed_tools": {"gtdbtk": False},
            },
        )
        self.assertTrue(any("GTDB-Tk" in error for error in errors))

    def test_public_auth_pages_use_refreshed_fetchm_web_copy(self) -> None:
        client = fetchm_app.app.test_client()
        health = client.get("/healthz")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.get_json()["status"], "ok")

        home = client.get("/")
        self.assertEqual(home.status_code, 200)
        home_html = home.data.decode("utf-8")
        self.assertIn("Select your target species or genus", home_html)
        self.assertIn("FetchM automatically standardizes genome metadata", home_html)
        self.assertIn("10.1093/bioadv/vbag124", home_html)
        self.assertNotIn("Start with the managed catalog", home_html)
        self.assertNotIn("Standardized metadata</strong>", home_html)

        login = client.get("/login")
        self.assertEqual(login.status_code, 200)
        login_html = login.data.decode("utf-8")
        self.assertIn("FetchM Web", login_html)
        self.assertIn("Run metadata analyses, launch sequence downloads", login_html)
        self.assertNotIn("FetckM", login_html)
        self.assertNotIn("FetchM WEB", login_html)
        self.assertIn("Create an account", login_html)
        self.assertIn("Forgot password?", login_html)

        register = client.get("/register")
        self.assertEqual(register.status_code, 200)
        register_html = register.data.decode("utf-8")
        self.assertIn("Register for FetchM Web", register_html)
        self.assertIn("private workspace", register_html)

        forgot = client.get("/forgot-password")
        self.assertEqual(forgot.status_code, 200)
        self.assertIn("Forgot your password?", forgot.data.decode("utf-8"))

    def test_theme_assets_include_design_tokens_and_cache_bust(self) -> None:
        css = Path(fetchm_app.app.root_path, "static", "styles.css").read_text(encoding="utf-8")
        self.assertIn("--accent-bright", css)
        self.assertIn("--radius-xl", css)
        self.assertIn(".auth-unified-panel::after", css)
        self.assertIn("Aptos Display", css)

        base = Path(fetchm_app.app.root_path, "templates", "base.html").read_text(encoding="utf-8")
        self.assertIn("20260512-saas-theme", base)
        self.assertIn(">FetchM Web<", base)

    def test_sequence_pages_require_login_but_metadata_routes_are_public(self) -> None:
        client = fetchm_app.app.test_client()
        self.assertEqual(client.get("/api/taxa/search?q=Klebsiella").status_code, 200)
        sequence = client.get("/taxa/1/sequences", follow_redirects=False)
        self.assertEqual(sequence.status_code, 302)
        self.assertIn("/login", sequence.headers.get("Location", ""))

    def test_external_profile_without_javascript_does_not_fall_back_to_quick_mode(self) -> None:
        class Form:
            def get(self, key: str, default=None):
                values = {"quality_profile": "standard", "quality_run_mode": "quick"}
                return values.get(key, default)

            def getlist(self, key: str):
                if key == "quality_module":
                    return ["quick_fasta"]
                return []

        config = build_quality_config(Form())
        self.assertEqual(config["run_mode"], "handoff")
        self.assertIn("checkm2", config["selected_modules"])
        self.assertIn("quast", config["selected_modules"])

    def test_quality_profiles_use_profile_specific_threshold_defaults(self) -> None:
        class Form:
            def __init__(self, values: dict[str, str]):
                self.values = values

            def get(self, key: str, default=None):
                return self.values.get(key, default)

            def getlist(self, key: str):
                return []

        quick = build_quality_config(Form({"quality_profile": "quick"}))
        self.assertEqual(quick["run_mode"], "quick")
        self.assertEqual(quick["selected_modules"], ["quick_fasta"])
        self.assertIsNone(quick["thresholds"]["min_completeness"])
        self.assertIsNone(quick["thresholds"]["max_contamination"])
        self.assertIsNone(quick["thresholds"]["min_ani_percent"])
        self.assertEqual(quick["thresholds"]["max_n_percent"], 5.0)

        quick_with_checkm = build_quality_config(
            Form(
                {
                    "quality_profile": "quick",
                    "qc_use_existing_checkm": "1",
                    "qc_min_completeness": "85",
                    "qc_max_contamination": "10",
                }
            )
        )
        self.assertTrue(quick_with_checkm["use_existing_checkm"])
        self.assertEqual(quick_with_checkm["thresholds"]["min_completeness"], 85.0)
        self.assertEqual(quick_with_checkm["thresholds"]["max_contamination"], 10.0)

        standard = build_quality_config(Form({"quality_profile": "standard"}))
        self.assertEqual(standard["run_mode"], "handoff")
        self.assertEqual(standard["thresholds"]["min_completeness"], 90.0)
        self.assertEqual(standard["thresholds"]["max_contamination"], 5.0)
        self.assertIsNone(standard["thresholds"]["min_ani_percent"])
        self.assertIn("checkm2", standard["selected_modules"])
        self.assertIn("quast", standard["selected_modules"])

        advanced = build_quality_config(Form({"quality_profile": "advanced"}))
        self.assertEqual(advanced["thresholds"]["min_ani_percent"], 95.0)
        self.assertIn("ani", advanced["selected_modules"])
        self.assertNotIn("checkm2", advanced["selected_modules"])
        self.assertNotIn("quast", advanced["selected_modules"])
        self.assertNotIn("mash", advanced["selected_modules"])
        self.assertNotIn("gtdbtk", advanced["selected_modules"])

    def test_quick_qc_missing_checkm_only_matters_when_thresholds_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.csv"
            output_dir = root / "outputs"
            output_dir.mkdir()
            input_path.write_text(
                "Assembly Accession,Assembly Name,CheckM completeness,CheckM contamination\n"
                "GCA_000001.1,ASM1,,\n",
                encoding="utf-8",
            )
            (output_dir / "GCA_000001.1_ASM1_genomic.fna").write_text(">contig1\nACGTACGTACGT\n", encoding="utf-8")
            job = fetchm_app.JobRecord(
                id="quick-checkm",
                mode="qc",
                status="running",
                created_at=fetchm_app.utc_now(),
                updated_at=fetchm_app.utc_now(),
                input_name="input.csv",
                input_path=str(input_path),
                output_dir=str(output_dir),
                log_path=str(root / "job.log"),
                command=[],
                owner_user_id=1,
            )

            quick = build_quality_config(type("Form", (), {"get": lambda self, key, default=None: {"quality_profile": "quick"}.get(key, default), "getlist": lambda self, key: []})())
            run_sequence_quality_checks(job, quick["thresholds"], quick)
            decisions = fetchm_app.pd.read_csv(output_dir / "sequence_qc" / "qc_decisions.csv", dtype=str).fillna("")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Status"], "pass")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Review_Reasons"], "")

            strict = build_quality_config(
                type(
                    "Form",
                    (),
                    {
                        "get": lambda self, key, default=None: {
                            "quality_profile": "quick",
                            "qc_use_existing_checkm": "1",
                            "qc_min_completeness": "90",
                            "qc_max_contamination": "5",
                        }.get(key, default),
                        "getlist": lambda self, key: [],
                    },
                )()
            )
            run_sequence_quality_checks(job, strict["thresholds"], strict)
            decisions = fetchm_app.pd.read_csv(output_dir / "sequence_qc" / "qc_decisions.csv", dtype=str).fillna("")
            self.assertEqual(decisions.loc[0, "Sequence_QC_Status"], "fail")
            self.assertIn("CheckM completeness missing", decisions.loc[0, "Sequence_QC_Failure_Reasons"])
            self.assertIn("CheckM contamination missing", decisions.loc[0, "Sequence_QC_Failure_Reasons"])

    def test_sequence_filter_and_or_logic(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {"Assembly Accession": "GCA_1", "Country": "India", "Host_SD": "Homo sapiens"},
                {"Assembly Accession": "GCA_2", "Country": "India", "Host_SD": "Sus scrofa"},
                {"Assembly Accession": "GCA_3", "Country": "Italy", "Host_SD": "Homo sapiens"},
            ]
        )
        filters = {"country": ["India"], "host_sd": ["Homo sapiens"], "filter_logic": "and"}
        self.assertEqual(apply_sequence_filters(frame, filters)["Assembly Accession"].tolist(), ["GCA_1"])
        filters["filter_logic"] = "or"
        self.assertEqual(
            apply_sequence_filters(frame, filters)["Assembly Accession"].tolist(),
            ["GCA_1", "GCA_2", "GCA_3"],
        )

    def test_pass_fail_decision_mode_collapses_review_into_fail(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_1",
                    "Sequence_QC_Status": "review",
                    "Sequence_QC_Pass": False,
                    "Sequence_QC_Failure_Reasons": "",
                    "Sequence_QC_Review_Reasons": "CheckM completeness missing",
                },
                {
                    "Assembly Accession": "GCA_2",
                    "Sequence_QC_Status": "pass",
                    "Sequence_QC_Pass": True,
                    "Sequence_QC_Failure_Reasons": "",
                    "Sequence_QC_Review_Reasons": "",
                },
            ]
        )
        collapsed = apply_pass_fail_decision_mode(frame)
        self.assertEqual(collapsed["Sequence_QC_Status"].tolist(), ["fail", "pass"])
        self.assertEqual(collapsed.loc[0, "Sequence_QC_Failure_Reasons"], "CheckM completeness missing")
        self.assertEqual(collapsed.loc[0, "Sequence_QC_Review_Reasons"], "")

    def test_post_qc_filters_support_comprehensive_metrics(self) -> None:
        frame = fetchm_app.pd.DataFrame(
            [
                {
                    "Assembly Accession": "GCA_1",
                    "Sequence_QC_Status": "pass",
                    "CheckM completeness": "96",
                    "CheckM contamination": "1",
                    "QC_total_bp": "3000000",
                    "QC_gc_percent": "50",
                    "QC_ambiguous_n_percent": "0.1",
                    "ANI_Closest_ANI": "96.2",
                    "ANI_Species_Consistency_Status": "PASS",
                    "Mash_Distance": "0.01",
                    "GTDBTK_QC_Status": "PASS",
                    "GTDBTK_Match_Rank": "species",
                },
                {
                    "Assembly Accession": "GCA_2",
                    "Sequence_QC_Status": "review",
                    "CheckM completeness": "91",
                    "CheckM contamination": "2",
                    "QC_total_bp": "4500000",
                    "QC_gc_percent": "63",
                    "QC_ambiguous_n_percent": "0.2",
                    "ANI_Closest_ANI": "89",
                    "ANI_Species_Consistency_Status": "WARN",
                    "Mash_Distance": "0.08",
                    "GTDBTK_QC_Status": "WARN",
                    "GTDBTK_Match_Rank": "genus",
                },
            ]
        )

        class Query:
            values = {
                "qc_status": ["pass", "review"],
                "min_ani_percent": "95",
                "max_mash_distance": "0.02",
                "gtdbtk_rank": ["species"],
            }

            def get(self, key: str, default=None):
                value = self.values.get(key, default)
                if isinstance(value, list):
                    return value[0] if value else default
                return value

            def getlist(self, key: str):
                value = self.values.get(key, [])
                return value if isinstance(value, list) else [value]

            def keys(self):
                return self.values.keys()

        filtered, applied = apply_quality_post_filters(frame, Query())
        self.assertEqual(filtered["Assembly Accession"].tolist(), ["GCA_1"])
        self.assertEqual(applied["min_ani_percent"], 95.0)
        self.assertEqual(applied["max_mash_distance"], 0.02)
        self.assertEqual(applied["gtdbtk_rank"], ["species"])

    def test_post_qc_filtered_download_sequence_job_and_owner_protection(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("owner", "owner@example.com", "long-password-1")
                    other = fetchm_app.create_user("other", "other@example.com", "long-password-2")
                    output_dir = fetchm_app.JOBS_DIR / "qc-owner" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    qc_dir.joinpath("qc_enriched_metadata.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination\n"
                        "GCA_1,pass,99,0.1\n"
                        "GCA_2,fail,80,6\n",
                        encoding="utf-8",
                    )
                    qc_dir.joinpath("qc_decisions.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status\nGCA_1,pass\nGCA_2,fail\n",
                        encoding="utf-8",
                    )
                    job = fetchm_app.JobRecord(
                        id="qc-owner",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="qc.csv",
                        input_path=str(root / "qc.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "qc.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"taxon_id": 1, "taxon_name": "Klebsiella", "taxon_rank": "genus"},
                    )
                    fetchm_app.save_job(job)

                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                        session["_csrf_token"] = "token"

                    response = client.get("/jobs/qc-owner/quality-filtered-metadata.csv?qc_status=pass")
                    self.assertEqual(response.status_code, 200)
                    body = response.data.decode("utf-8")
                    self.assertIn("GCA_1", body)
                    self.assertNotIn("GCA_2", body)

                    response = client.post(
                        "/jobs/qc-owner/quality-filtered-sequence-job",
                        data={"_csrf_token": "token", "qc_status": "pass"},
                        follow_redirects=False,
                    )
                    self.assertEqual(response.status_code, 302)
                    created = fetchm_app.get_db().execute(
                        "SELECT * FROM jobs WHERE mode='seq' AND json_extract(filters_json, '$.parent_quality_job_id') = ?",
                        ("qc-owner",),
                    ).fetchone()
                    self.assertIsNotNone(created)
                    self.assertEqual(fetchm_app.load_job(str(created["id"])).filters["matched_row_total"], 1)

                    with client.session_transaction() as session:
                        session["user_id"] = int(other["id"])
                    denied = client.get("/jobs/qc-owner/files/sequence_qc/qc_all_metadata.csv")
                    self.assertEqual(denied.status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_quality_summary_route_generates_report_with_figures_and_links(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("summary-owner", "summary-owner@example.com", "long-password-1")
                    other = fetchm_app.create_user("summary-other", "summary-other@example.com", "long-password-2")
                    output_dir = fetchm_app.JOBS_DIR / "qc-summary" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    (output_dir / "quality_check_bundle.zip").write_bytes(b"zip")
                    qc_dir.joinpath("qc_decisions.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination,"
                        "QC_total_bp,QC_contig_count,QC_n50,QC_gc_percent,QC_ambiguous_n_percent,"
                        "ANI_Closest_ANI,ANI_Species_Consistency_Status,Mash_Distance,Mash_Closest_Genome,"
                        "GTDBTK_QC_Status,GTDBTK_Match_Rank,GTDBTK_Species,GTDBTK_FastANI,"
                        "Sequence_QC_Failure_Reasons,Sequence_QC_Review_Reasons\n"
                        "GCA_1,pass,99.1,0.1,3200000,120,51000,45.2,0.0,97.4,PASS,0.01,GCA_2,PASS,species,Prevotella copri,98.8,,\n"
                        "GCA_2,fail,82.0,6.2,2500000,450,9000,43.1,0.2,88.1,WARN,0.08,GCA_1,FAIL,genus,Prevotella sp,,min_completeness:82<90,ani_species_warning:88.1<95\n",
                        encoding="utf-8",
                    )
                    for name in [
                        "qc_all_metadata.csv",
                        "qc_pass_metadata.csv",
                        "qc_review_metadata.csv",
                        "qc_failed_metadata.csv",
                        "external_qc_master_report.csv",
                    ]:
                        qc_dir.joinpath(name).write_text("Assembly Accession\nGCA_1\n", encoding="utf-8")
                    qc_dir.joinpath("quality_check_report.md").write_text("# report\n", encoding="utf-8")
                    qc_dir.joinpath("quality_check_summary.json").write_text(
                        json.dumps(
                            {
                                "total": 2,
                                "pass": 1,
                                "review": 0,
                                "fail": 1,
                                "quality_profile": "Standard QC",
                                "decision_mode": "pass_fail",
                                "run_mode": "nextflow",
                                "qc_decision_source": "nextflow",
                                "selected_modules": ["quick_fasta", "checkm2", "quast", "ani", "mash", "gtdbtk"],
                                "thresholds": {
                                    "min_completeness": 90.0,
                                    "max_contamination": 5.0,
                                    "min_ani_percent": 95.0,
                                },
                            }
                        ),
                        encoding="utf-8",
                    )
                    job = fetchm_app.JobRecord(
                        id="qc-summary",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="qc.csv",
                        input_path=str(root / "qc.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "qc.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"sequence_filter_sentence": "Matching genomes where Country is India."},
                    )
                    fetchm_app.save_job(job)

                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                    response = client.get("/jobs/qc-summary/quality-summary")
                    self.assertEqual(response.status_code, 200)
                    html = response.data.decode("utf-8")
                    response.close()
                    self.assertIn("QC Figures", html)
                    self.assertIn("Important Files", html)
                    self.assertIn("ANI, Mash, and GTDB-Tk Summary", html)
                    self.assertIn("Download QC ZIP", html)
                    self.assertIn("Passed metadata", html)
                    self.assertIn("GTDB reference ANI", html)
                    self.assertIn("GCA_1", html)
                    self.assertTrue(qc_dir.joinpath("quality_check_summary.html").exists())

                    with client.session_transaction() as session:
                        session["user_id"] = int(other["id"])
                    denied = client.get("/jobs/qc-summary/quality-summary")
                    self.assertEqual(denied.status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths

    def test_advanced_qc_job_uses_current_post_qc_filtered_subset(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            old_blockers = fetchm_app.quality_submission_blockers
            old_validator = fetchm_app.validate_quality_runtime
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.quality_submission_blockers = lambda: []
            fetchm_app.validate_quality_runtime = lambda quality_config, tool_status: []
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    owner = fetchm_app.create_user("owner", "owner@example.com", "long-password-1")
                    output_dir = fetchm_app.JOBS_DIR / "standard-parent" / "outputs"
                    qc_dir = output_dir / "sequence_qc"
                    qc_dir.mkdir(parents=True)
                    qc_dir.joinpath("qc_enriched_metadata.csv").write_text(
                        "Assembly Accession,Sequence_QC_Status,CheckM completeness,CheckM contamination\n"
                        "GCA_1,pass,99,0.1\n"
                        "GCA_2,fail,80,6\n",
                        encoding="utf-8",
                    )
                    parent = fetchm_app.JobRecord(
                        id="standard-parent",
                        mode="qc",
                        status="completed",
                        created_at=fetchm_app.utc_now(),
                        updated_at=fetchm_app.utc_now(),
                        input_name="standard.csv",
                        input_path=str(root / "standard.csv"),
                        output_dir=str(output_dir),
                        log_path=str(root / "standard.log"),
                        command=[],
                        owner_user_id=int(owner["id"]),
                        filters={"quality_config": {"profile": {"key": "standard"}}},
                    )
                    fetchm_app.save_job(parent)
                    client = fetchm_app.app.test_client()
                    with client.session_transaction() as session:
                        session["user_id"] = int(owner["id"])
                        session["_csrf_token"] = "token"

                    response = client.post(
                        "/jobs/standard-parent/advanced-quality-job",
                        data={
                            "_csrf_token": "token",
                            "qc_status": "pass",
                            "advanced_module_mash": "1",
                            "advanced_min_ani_percent": "96",
                        },
                        follow_redirects=False,
                    )
                    self.assertEqual(response.status_code, 302)
                    child = fetchm_app.get_db().execute(
                        "SELECT * FROM jobs WHERE mode='qc' AND id != ? ORDER BY created_at DESC LIMIT 1",
                        ("standard-parent",),
                    ).fetchone()
                    self.assertIsNotNone(child)
                    child_job = fetchm_app.load_job(str(child["id"]))
                    self.assertEqual(child_job.filters["parent_quality_job_id"], "standard-parent")
                    self.assertTrue(child_job.filters["advanced_qc"])
                    self.assertEqual(child_job.filters["matched_row_total"], 1)
                    self.assertEqual(child_job.filters["post_qc_filters"]["qc_status"], ["pass"])
                    self.assertIn("ani", child_job.filters["quality_config"]["selected_modules"])
                    self.assertIn("mash", child_job.filters["quality_config"]["selected_modules"])
                    self.assertNotIn("checkm2", child_job.filters["quality_config"]["selected_modules"])
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths
                fetchm_app.quality_submission_blockers = old_blockers
                fetchm_app.validate_quality_runtime = old_validator

    def test_internal_nextflow_work_files_are_hidden_from_user_outputs(self) -> None:
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/nextflow_work/aa/bb/.command.sh")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/local_samples/fetchm_web_qc/sequence/example.fna")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/.nextflow.log")))
        self.assertTrue(should_expose_output_file(Path("external_tools/quality_check/nextflow_execution.log")))
        self.assertTrue(should_expose_output_file(Path("sequence_qc/qc_decisions.csv")))

    def test_global_insights_snapshot_prefers_species_rows_and_writes_simulator_records(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            species_csv = root / "species.csv"
            genus_csv = root / "genus.csv"
            species_csv.write_text(
                "Assembly Accession,Organism Name,Geographic Location,Country,Host,Host_SD,"
                "Isolation Source,Isolation_Source_SD,Collection Date,Assembly Release Date,"
                "Assembly Level,Assembly BioProject Accession,CheckM completeness,Country_Confidence\n"
                "GCA_000001.1,Escherichia coli,USA,United States,Homo sapiens,Human,"
                "human stool,feces/stool,2020,2021-01-02,Scaffold,PRJNA1,99,exact\n",
                encoding="utf-8",
            )
            genus_csv.write_text(
                "Assembly Accession,Organism Name,Geographic Location,Country,Host,Host_SD,"
                "Isolation Source,Isolation_Source_SD,Collection Date,Assembly Release Date,"
                "Assembly Level,Assembly BioProject Accession,CheckM completeness,Country_Confidence\n"
                "GCA_000001.1,Escherichia coli duplicate,USA,United States,Homo sapiens,Human,"
                "human stool,feces/stool,2020,2021-01-02,Scaffold,PRJNA1,99,exact\n"
                "GCA_000002.1,Escherichia albertii,Bangladesh: Dhaka,Bangladesh,duck,Anas platyrhynchos,"
                "pond water,wastewater,2021,2022-04-05,Contig,PRJNA2,96,synonym\n",
                encoding="utf-8",
            )

            summary = generate_global_insights_snapshot(
                [
                    {
                        "id": 1,
                        "species_name": "Escherichia",
                        "taxon_rank": "genus",
                        "genome_count": 2,
                        "metadata_clean_path": str(genus_csv),
                        "last_synced_at": "2026-05-14T01:00:00+00:00",
                    },
                    {
                        "id": 2,
                        "species_name": "Escherichia coli",
                        "taxon_rank": "species",
                        "genome_count": 1,
                        "metadata_clean_path": str(species_csv),
                        "last_synced_at": "2026-05-14T00:00:00+00:00",
                    },
                ],
                root / "global_insights",
                app_version="test",
                app_commit="unit",
                snapshot_id="unit_global_insights",
            )

            self.assertEqual(summary["overview"]["unique_assemblies"], 2)
            self.assertEqual(summary["overview"]["duplicate_rows_skipped"], 1)
            self.assertEqual(summary["overview"]["metadata_files_scanned"], 2)
            self.assertEqual(summary["taxonomic_landscape"]["top_genera"][0]["label"], "Escherichia")
            self.assertEqual(summary["taxonomic_landscape"]["top_genera"][0]["count"], 2)
            corrections = summary["standardization_impact"]["top_corrections"]
            self.assertTrue(any(row["field"] == "Country" and row["raw_value"] == "USA" for row in corrections))
            self.assertTrue((root / "global_insights" / "snapshots" / "unit_global_insights" / "summary.json").exists())

            simulator = run_standardization_simulator(
                root / "global_insights" / "snapshots" / "unit_global_insights" / "tables" / "simulator_records.csv",
                {"taxon": "Escherichia", "country": "United States", "host": "Human"},
            )
            self.assertTrue(simulator["available"])
            self.assertEqual(simulator["raw_count"], 0)
            self.assertEqual(simulator["standardized_count"], 1)
            self.assertEqual(simulator["rescued_count"], 1)
            self.assertEqual(simulator["examples"][0]["assembly_accession"], "GCA_000001.1")

    def test_global_insights_page_and_summary_download_are_public(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH)
            fetchm_app.DATA_DIR = root / "data"
            fetchm_app.JOBS_DIR = fetchm_app.DATA_DIR / "jobs"
            fetchm_app.LOCKS_DIR = fetchm_app.DATA_DIR / "locks"
            fetchm_app.DB_PATH = fetchm_app.DATA_DIR / "fetchm_webapp.db"
            fetchm_app.DATA_DIR.mkdir(parents=True, exist_ok=True)
            try:
                with fetchm_app.app.app_context():
                    fetchm_app.init_db()
                    generate_demo_snapshot(
                        fetchm_app.global_insights_root(),
                        app_version="test",
                        app_commit="unit",
                        snapshot_id="demo_global_insights",
                    )
                    client = fetchm_app.app.test_client()
                    page = client.get("/global-insights")
                    self.assertEqual(page.status_code, 200)
                    html = page.data.decode("utf-8")
                    self.assertIn("Global Metadata Insights", html)
                    self.assertIn("DEMO DATA - NOT REAL RESULTS", html)
                    self.assertIn("Methods & Reproducibility", html)
                    self.assertIn("Standardization Impact Simulator", html)

                    download = client.get("/global-insights/download/summary.json")
                    self.assertEqual(download.status_code, 200)
                    self.assertEqual(download.get_json()["snapshot_id"], "demo_global_insights")
                    self.assertEqual(client.get("/global-insights/download/../fetchm_webapp.db").status_code, 404)
            finally:
                fetchm_app.DATA_DIR, fetchm_app.JOBS_DIR, fetchm_app.LOCKS_DIR, fetchm_app.DB_PATH = old_paths


if __name__ == "__main__":
    unittest.main()
