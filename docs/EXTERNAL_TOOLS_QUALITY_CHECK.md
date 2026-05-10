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

`nextflow` mode is disabled by default. Enable it only after Nextflow, Conda or
Mamba, CheckM2 database paths, and any optional tool databases are configured on
the server.

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
FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/path/to/PanResistome
```

If `FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW` is not set, the handoff command
uses `Tasnimul-Arabi-Anik/PanResistome`.

## Design Rule

Do not paste heavy external-tool logic into `app.py`. Add new tools through the
external adapter layer, write a manifest, parse outputs into stable CSV files,
and expose only summarized pass/review/fail results through the web UI.
