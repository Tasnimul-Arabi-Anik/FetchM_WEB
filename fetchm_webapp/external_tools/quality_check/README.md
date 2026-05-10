# FetchM Web Quality-Check External Tools

This directory isolates comprehensive sequence quality-check integrations from the
Flask application core.

FetchM Web supports three execution modes:

- `quick`: built-in FASTA statistics plus available metadata thresholds.
- `handoff`: quick QC plus a Nextflow command/manifest for external execution.
- `nextflow`: execute the configured Nextflow QC workflow from the web worker.

`nextflow` execution is disabled by default. Enable it only after the server has
Nextflow, Conda/Mamba, and required databases such as CheckM2/GTDB-Tk installed.

Required environment variables:

- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED=1`
- `FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/path/to/workflow` or `Tasnimul-Arabi-Anik/PanResistome`

The web app records every selected module, threshold, command, and tool-status
snapshot under:

```text
data/jobs/<job_id>/outputs/external_tools/quality_check/
```

This keeps external tools auditable and prevents hidden pipeline behavior from
being mixed into metadata standardization or sequence download logic.
