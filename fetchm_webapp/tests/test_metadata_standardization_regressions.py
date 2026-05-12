from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import app as fetchm_app
from app import (
    broad_standardization_category,
    build_quality_config,
    ensure_managed_metadata_schema,
    extract_country,
    import_nextflow_qc_outputs,
    dedupe_reason_text,
    should_expose_output_file,
    standardize_host_metadata,
)
from external_tools.quality_check.runner import validate_quality_runtime


class MetadataStandardizationRegressionTests(unittest.TestCase):
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
                        "GCF_000002.1,ASM2,GCF_000002.1_ASM2_genomic.fna,4100000,300,5000,56.9,10,72.0,8.5,94.1,WARN,ANI_CLUSTER_0002,FAIL,Enterobacter,Enterobacter cloacae,GENUS_MISMATCH,FAIL,CheckM2 completeness below threshold,",
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

    def test_internal_nextflow_work_files_are_hidden_from_user_outputs(self) -> None:
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/nextflow_work/aa/bb/.command.sh")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/local_samples/fetchm_web_qc/sequence/example.fna")))
        self.assertFalse(should_expose_output_file(Path("external_tools/quality_check/.nextflow.log")))
        self.assertTrue(should_expose_output_file(Path("external_tools/quality_check/nextflow_execution.log")))
        self.assertTrue(should_expose_output_file(Path("sequence_qc/qc_decisions.csv")))


if __name__ == "__main__":
    unittest.main()
