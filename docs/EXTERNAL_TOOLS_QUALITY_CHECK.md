# External Tools: Quality Check Architecture

FetchM Web keeps heavy quality-control tools outside the Flask application core.
The web app owns user workflow, job tracking, metadata filtering, and result
packaging. External tools are isolated behind adapters under:

```text
fetchm_webapp/external_tools/quality_check/
```

## Execution Modes

- `quick`: built-in FASTA statistics plus available metadata thresholds.
- `handoff`: quick QC plus a reproducible Nextflow command and manifest.
- `nextflow`: execute the configured Nextflow workflow from the job worker.

`nextflow` mode uses Java, Nextflow, and Conda inside the Docker image. Conda
environments and databases are cached under `fetchm_webapp/data/external_tools/`
so they persist across container rebuilds.

## Supported Module Families

- Quick FASTA statistics: built in.
- CheckM2 completeness/contamination: external.
- QUAST assembly metrics: external.
- ANI/skani species consistency: external.
- Mash duplicate/outlier pre-screen: external.
- GTDB-Tk taxonomy check: external and heavy.

## Runtime Output Contract

Each quality-check job writes:

```text
data/jobs/<job_id>/outputs/
  quality_check_bundle.zip
  sequence_qc/
    assembly_stats.csv
    qc_decisions.csv
    qc_enriched_metadata.csv
    qc_pass_metadata.csv
    qc_review_metadata.csv
    qc_failed_metadata.csv
    quality_check_report.md
    quality_check_summary.json
  external_tools/quality_check/
    quality_check_manifest.json
    nextflow_command.sh
    README.md
```

`qc_pass_metadata.csv` is the stable handoff for downloading only genomes that
passed the selected quality checks.

## Server Configuration

Environment variables:

```text
FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED=1
FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/app/fetchm_webapp/data/external_tools/workflows/PanResistome
FETCHM_WEBAPP_QUALITY_NEXTFLOW_CONFIG=/app/fetchm_webapp/external_tools/quality_check/panresistome_qc/fetchm_web_qc.config
FETCHM_WEBAPP_QUALITY_NEXTFLOW_PROFILE=conda,lowmem
FETCHM_WEBAPP_QUALITY_CHECKM2_DB_DIR=/app/fetchm_webapp/data/external_tools/databases/checkm2
NXF_HOME=/app/fetchm_webapp/data/external_tools/nextflow/home
NXF_CONDA_CACHEDIR=/app/fetchm_webapp/data/external_tools/conda/envs
CONDA_PKGS_DIRS=/app/fetchm_webapp/data/external_tools/conda/pkgs
NXF_SYNTAX_PARSER=v1
```

The production Compose file enables these values for the web service and job
workers. The PanResistome workflow should be cloned to:

```text
fetchm_webapp/data/external_tools/workflows/PanResistome
```

`FETCHM_WEBAPP_QUALITY_NEXTFLOW_CONFIG` points to FetchM Web's adapter config.
That config keeps PanResistome in local-sample, QC-only mode. FetchM Web passes
the filtered FASTA files and metadata through `--local_samples`, so the external
pipeline does not refetch metadata or sequences.

CheckM2 can auto-download its database to the configured database directory on
first run. GTDB-Tk is intentionally optional because its reference database is
large; configure `FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH` only after installing
the GTDB release separately.

`NXF_SYNTAX_PARSER=v1` is required for compatibility with the current
PanResistome Nextflow syntax when using Nextflow 26+.

## Design Rule

Do not paste heavy external-tool logic into `app.py`. Add new tools through the
external adapter layer, write a manifest, parse outputs into stable CSV files,
and expose only summarized pass/review/fail results through the web UI.
