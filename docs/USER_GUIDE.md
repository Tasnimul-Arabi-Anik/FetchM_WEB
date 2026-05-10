# FetchM Web User Guide

Version: 2026.05-genus-v1.1

## What FetchM Web Does

FetchM Web helps you explore microbial genome metadata, review standardized metadata fields, and launch sequence-download jobs from a browser. It keeps your jobs, logs, filtered metadata, and downloaded outputs under your own account.

## Account Access

1. Open `https://fetchm.dulab206.xyz/login`.
2. Sign in with your username and password.
3. If you are new, use `Create an account`.
4. If you forgot your password, use `Forgot password?`.

Accounts are used to keep user inputs and outputs private. Admins can monitor queue health and failures, but passwords are never displayed.

## Finding a Taxon

1. Go to the home page.
2. Search for a species or genus in the taxon search box.
3. Select the matching taxon.
4. If the taxon is already prepared, links appear for metadata analysis and sequence download.
5. If metadata must be prepared first, FetchM Web queues the build and shows the pending state.

## Metadata Analysis

The metadata page summarizes standardized and raw metadata for the selected taxon. Common sections include:

- Metadata completeness.
- Country/geography distributions.
- Collection-year summaries.
- Standardized and raw host distributions.
- Host disease and host health-state summaries when annotations exist.
- Host taxonomic structure from lineage columns.
- Isolation source, isolation site, sample type, and environment-medium distributions.
- Standardization QA fields such as review status and confidence.

Original source metadata is preserved. Standardized values are written into derived fields such as:

- `Country`, `Continent`, `Subcontinent`
- `Collection_Year`
- `Host_SD`, `Host_TaxID`, `Host_Rank`, lineage columns
- `Sample_Type_SD`
- `Isolation_Source_SD`
- `Isolation_Site_SD`
- `Environment_Medium_SD`
- `Host_Disease_SD`
- `Host_Health_State_SD`

## Sequence Download

The sequence page lets you filter genomes before downloading sequences.

Useful filters include:

- Country, continent, and subcontinent.
- Host and host taxonomy.
- Isolation source and sample type.
- Collection year.
- Assembly quality fields such as CheckM completeness/contamination.
- Assembly length and contig/scaffold count.

After filtering:

1. Review how many genomes match.
2. Download filtered metadata if needed.
3. Launch a sequence-download job.
4. Optionally open the `Quality Check` workspace for the same filtered subset.
5. Track the job from `Track Jobs`.
6. Download generated outputs from the job detail page.

The quality-check workspace supports quick built-in QC and external-tool handoff for PanResistome-style checks. Quick QC downloads the selected FASTA files, computes assembly statistics, merges available CheckM completeness/contamination metadata, and writes pass/review/fail tables. External modules such as CheckM2, QUAST, ANI/skani, Mash, and GTDB-Tk are selected from the same page and recorded as a Nextflow handoff unless server-side Nextflow execution is explicitly enabled.

The job detail page shows each stage as it finishes and provides:

- `quality_check_report.md`
- `qc_decisions.csv`
- `qc_enriched_metadata.csv`
- `qc_pass_metadata.csv`
- `external_tools/quality_check/quality_check_manifest.json`
- `external_tools/quality_check/nextflow_command.sh`
- `quality_check_bundle.zip`

Use `qc_pass_metadata.csv` when you want a quality-filtered subset for downstream analysis. From a completed quality-check job, click `Create sequence job` to download only QC-passed genomes.

## Job Tracking

The `Track Jobs` page shows your queued, running, completed, failed, and cancelled jobs. Each job detail page includes:

- Current status.
- Command and filters.
- Log output.
- Downloadable output files.
- Sequence bundles, filtered metadata, or grouped outputs when available.

Queued or running jobs can be cancelled from the job detail page.

## Understanding Standardization

FetchM Web uses deterministic, auditable rules rather than direct AI-generated production mappings. The standardization workflow:

1. Preserves raw metadata.
2. Normalizes obvious spelling and formatting variation.
3. Applies reviewed rules and taxonomy-backed host mappings.
4. Routes source/sample/environment terms into the correct standardized fields.
5. Leaves ambiguous long-tail values for review rather than forcing low-confidence assignments.

The final genus-level production audit reports:

- 5,066 genus metadata files scanned.
- 2,598,486 rows scanned.
- 0 file errors.
- 0 country-continent/subcontinent mismatches.
- 0 non-country values in standardized `Country`.
- 0 noisy broad-source leakage values.
- 484 low-frequency host values still requiring long-tail review.

## Privacy and Security Notes

- Sessions expire after the configured server lifetime.
- POST forms use CSRF protection.
- New passwords require at least 10 characters with at least one letter and one number.
- Your job files are scoped to your account.
- Do not upload private data unless you are authorized to process it on this server.

## Getting Help

Use `Feedback / Report` from the navigation bar to report failed jobs, missing taxa, metadata issues, or usability problems.
