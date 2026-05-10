# PanResistome QC Workflow Slot

This directory is reserved for a pinned local copy or submodule of the
PanResistome quality-check workflow.

Current default behavior uses a Nextflow handoff command targeting:

```text
Tasnimul-Arabi-Anik/PanResistome
```

For fully local execution, place or mount the reviewed workflow here and set:

```text
FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/app/fetchm_webapp/external_tools/quality_check/panresistome_qc
FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED=1
```

Do not commit downloaded databases, genome FASTA files, or large Nextflow work
directories into this repository.
