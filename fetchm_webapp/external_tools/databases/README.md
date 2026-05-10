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

- CheckM2 database: configured at `FETCHM_WEBAPP_QUALITY_CHECKM2_DB_DIR`; the
  PanResistome QC workflow can auto-download it on the first CheckM2 run.
- GTDB-Tk database: not downloaded automatically because the reference database
  is large. Install it separately and set `FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH`.
- QUAST, skani/FastANI, and Mash tools are installed in Nextflow-managed Conda
  environments when their modules are first executed.
- Tool Conda environments are cached under
  `/app/fetchm_webapp/data/external_tools/conda/envs`.

Keep these databases out of Git. Commit only setup code, documentation, and
configuration examples.
