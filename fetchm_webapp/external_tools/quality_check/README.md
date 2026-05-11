# FetchM Web Quality-Check External Tools

This directory isolates comprehensive sequence quality-check integrations from the
Flask application core.

FetchM Web supports three execution modes:

- `quick`: built-in FASTA statistics plus available metadata thresholds.
- `handoff`: quick QC plus a Nextflow command/manifest for external execution.
- `nextflow`: execute the configured Nextflow QC workflow from the web worker.

`nextflow` execution should be enabled only after the server has Nextflow,
Conda/Mamba, and required databases such as CheckM2/GTDB-Tk installed.

Required environment variables:

- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED=1`
- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/path/to/workflow` or `Tasnimul-Arabi-Anik/PanResistome`
- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_CONFIG=/path/to/fetchm_web_qc.config`
- `FETCHM_WEBAPP_QUALITY_CHECKM2_DB=/path/to/uniref100.KO.1.dmnd`
- `NXF_HOME=/app/fetchm_webapp/data/external_tools/nextflow/home`
- `NXF_CONDA_CACHEDIR=/app/fetchm_webapp/data/external_tools/conda/envs`
- `CONDA_PKGS_DIRS=/app/fetchm_webapp/data/external_tools/conda/pkgs`

## Persistent caches and databases

FetchM Web should never download the same QC database for every user run. The
deployment keeps heavy runtime assets under the mounted data directory:

```text
data/external_tools/
├── conda/                 # shared Nextflow Conda env/package cache
├── databases/checkm2/     # prefetched CheckM2 database
├── nextflow/              # Nextflow home/cache
└── workflows/PanResistome # local QC workflow clone
```

For CheckM2, prefer `FETCHM_WEBAPP_QUALITY_CHECKM2_DB` pointing directly at the
prefetched `.dmnd` file. This makes PanResistome run with
`--checkm2_auto_download_db false`, so it reuses the local database instead of
checking/downloading during every QC job.

GTDB-Tk remains manual because its reference data is large. Install the GTDB-Tk
database under `data/external_tools/databases/gtdbtk/` and set
`FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH` only when taxonomy QC is intentionally
enabled.

## Performance defaults

The current deployment is tuned for a 128-core / 128-GB server:

- `FETCHM_WEBAPP_QUALITY_THREADS=32`
- `FETCHM_WEBAPP_QUALITY_CHECKM2_THREADS=16`
- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_PROFILE=conda`
- `FETCHM_WEBAPP_QUALITY_ANI_TOOL=skani`

These values keep individual QC jobs much faster while leaving capacity for web
requests and concurrent workers. If several large QC jobs run simultaneously,
lower the two thread variables or temporarily scale job workers down.

The web app records every selected module, threshold, command, and tool-status
snapshot under:

```text
data/jobs/<job_id>/outputs/external_tools/quality_check/
```

This keeps external tools auditable and prevents hidden pipeline behavior from
being mixed into metadata standardization or sequence download logic.
