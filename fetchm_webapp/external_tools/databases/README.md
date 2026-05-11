# External Databases

FetchM Web stores large external QC databases outside the application code under
the mounted runtime path:

```text
/app/fetchm_webapp/data/external_tools/databases/
```

Recommended layout:

```text
databases/
├── checkm2/
├── gtdbtk/
├── mash/
└── README.md
```

Current policy:

- CheckM2 database: prefetched under `checkm2/CheckM2_database/` and configured
  with `FETCHM_WEBAPP_QUALITY_CHECKM2_DB` pointing directly at the `.dmnd` file.
  This disables repeat auto-downloads and makes every QC job reuse the same local
  database.
- GTDB-Tk database: not downloaded automatically because the reference database
  is large. Install it separately and set `FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH`.
- QUAST, skani/FastANI, and Mash tools are installed in Nextflow-managed Conda
  environments when their modules are first executed.
- Tool Conda environments are cached under
  `/app/fetchm_webapp/data/external_tools/conda/envs`.

Current CheckM2 path in Docker:

```text
/app/fetchm_webapp/data/external_tools/databases/checkm2/CheckM2_database/uniref100.KO.1.dmnd
```

Recommended GTDB-Tk path, when installed:

```text
/app/fetchm_webapp/data/external_tools/databases/gtdbtk/
```

Keep these databases out of Git. Commit only setup code, documentation, and
configuration examples.
