# FetchM_WEB Repository Structure

This repository is the source-controlled FetchM web application. Runtime data is
kept outside Git.

## Active Application

- `fetchm_webapp/app.py` - Flask web application, metadata workflows,
  standardization logic, admin dashboards, sequence-download orchestration.
- `fetchm_webapp/templates/` - HTML templates for user, taxon, admin, metadata,
  and sequence-download pages.
- `fetchm_webapp/static/` - web UI styling.
- `fetchm_webapp/lib/fetchm_runtime/` - bundled FetchM runtime modules used by
  the web application.
- `fetchm_webapp/tools/` - audit, backfill, review, and rule-generation scripts.
- `fetchm_webapp/standardization/` - curated standardization rule files.

## Deployment

- `fetchm_webapp/docker-compose.yml` - web, metadata workers, sequence workers,
  standardization workers, and Caddy deployment.
- `fetchm_webapp/Dockerfile` - application image with NCBI Datasets and
  TaxonKit.
- `fetchm_webapp/Caddyfile` - reverse proxy/TLS configuration.
- `fetchm_webapp/.env.example` - documented environment variables.

## Standardization Rules

- `fetchm_webapp/standardization/host_synonyms.csv` - positive host mappings to
  canonical host names and TaxIDs.
- `fetchm_webapp/standardization/host_negative_rules.csv` - missing,
  non-host/source, and not-identifiable host values that should not populate
  `Host_SD`.
- `fetchm_webapp/standardization/controlled_categories.csv` - controlled
  mappings for source, sample type, and environment fields.
- `fetchm_webapp/standardization/geography_reviewed_rules.csv` - reviewed
  geography recovery rules.
- `fetchm_webapp/standardization/collection_date_reviewed_rules.csv` - reviewed
  collection-date recovery rules.

## Publication and Review Notes

- `docs/PUBLICATION_STANDARDIZATION_REPORT.md` - high-level summary of metadata
  standardization improvements and audit results.
- `fetchm_webapp/standardization/review/metadata_standardization_research_log.md`
  - chronological technical log of standardization decisions.
- `fetchm_webapp/standardization/review/fetchm_web_research_paper_notes.md` -
  notes intended for methods/results writing.
- `fetchm_webapp/standardization/review/host_review/README.md` - host-review
  workflow notes.

## Runtime Data

Runtime data is intentionally not committed:

- `fetchm_webapp/data` is a local symlink to
  `/home/ai-pc/Work/dulab206/fetchm_webapp/data`.
- Metadata outputs, jobs, SQLite databases, downloaded sequences, and generated
  review CSVs live there or in ignored review-output folders.
- `.env`, Python caches, generated review tables, and local backup snapshots are
  ignored by Git.

## Legacy CLI Package

The root `fetchm/`, `bin/`, `figures/`, `pyproject.toml`, and `vibrio_v2.tsv`
are retained for compatibility with the original FetchM CLI/package history.
The actively developed web application is under `fetchm_webapp/`.
