# FetchM Web Admin Operations Guide

Version: 2026.05-genus-v1.1

## Purpose

This guide describes how to operate the FetchM Web deployment, monitor jobs and workers, review metadata standardization, and maintain the production-ready genus-level release state.

## Required Environment

Set these values before exposing the service:

- `FETCHM_WEBAPP_SECRET`: required production secret key.
- `FETCHM_WEBAPP_ADMIN_USERS`: comma-separated usernames with admin access.
- `FETCHM_WEBAPP_SECURE_COOKIE=1`: required for HTTPS session cookies.
- `FETCHM_WEBAPP_SESSION_HOURS`: optional session lifetime, default 12 hours.
- `FETCHM_WEBAPP_MAX_UPLOAD_BYTES`: optional upload-size ceiling.
- SMTP variables for password reset and job notifications.
- `NCBI_API_KEY`: recommended for higher NCBI request limits.

If `FETCHM_WEBAPP_ENV=production` or `FLASK_ENV=production`, the app refuses to start with the development default secret.

## Deployment

Build and start the web service:

```bash
docker-compose -f /home/ai-pc/Work/FetchM_WEB/fetchm_webapp/docker-compose.yml build fetchm-web
docker-compose -f /home/ai-pc/Work/FetchM_WEB/fetchm_webapp/docker-compose.yml up -d fetchm-web
```

The image runs as the non-root `fetchm` user. Runtime data is mounted under:

```text
fetchm_webapp/data/
```

If Docker Compose reports the legacy `ContainerConfig` recreate error, remove only the stale renamed web container and rerun `up -d fetchm-web`.

## Admin Navigation

Admin pages are available from `/admin`:

- `Overview`: system health, storage, active workers, stalled jobs, sync state, and security posture.
- `Catalog`: managed taxa and TSV sync controls.
- `Discovery`: discovery scopes and discovery refresh controls.
- `Metadata`: metadata build/refresh state and standardization refresh controls.
- `Standardization`: host, geography, collection-date, and general curation pages.
- `Problems`: user-submitted reports.
- `Jobs`: job analytics, search/filter, cancellation, deletion, and cleanup.
- `Users`: registered users and job counts.
- `Audit Log`: recent sign-in, admin, job, and download events.

## Worker and Queue Monitoring

Use the `Overview` and `Metadata` admin pages to monitor:

- Live worker count.
- Stale worker heartbeats.
- Active jobs.
- Stalled jobs.
- Active metadata claims.
- Queued/running/done/failed standardization tasks.
- Metadata build and refresh queues.
- Disk and database footprint.

Policy controls allow discovery, catalog refresh, and metadata refresh to be paused or resumed. Standardization refresh can be queued from the `Metadata` page or from curation pages after reviewed rule updates.

## Job Operations

The `Jobs` admin page provides:

- Total, active, queued, running, completed, failed, and cancelled job counts.
- Jobs submitted in the last 7 days.
- Finished-job success rate.
- Average finished-job duration.
- Visual status, mode, and top-submitter distribution summaries.
- Search by user, job ID, mode, or input name.
- Cancel queued/running jobs.
- Delete finished jobs.
- Clean old finished jobs by age.

Destructive actions should be used conservatively. Running or queued jobs must be cancelled before deletion.

## External Quality-Check Tools

FetchM Web includes a built-in quick quality check and an isolated external-tool handoff layer for PanResistome-style QC. The code lives under:

```text
fetchm_webapp/external_tools/quality_check/
```

Quick QC does not require extra dependencies. It computes FASTA length, contig count, N50, GC%, ambiguous-N percentage, and applies available CheckM metadata thresholds.

Comprehensive QC modules such as CheckM2, QUAST, ANI/skani, Mash, and GTDB-Tk require the external Nextflow stack. The Docker image includes Java, Nextflow, and Miniforge/Conda; PanResistome Conda environments and databases are cached under `fetchm_webapp/data/external_tools/`.

```text
FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED=1
FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW=/app/fetchm_webapp/data/external_tools/workflows/PanResistome
FETCHM_WEBAPP_QUALITY_NEXTFLOW_PROFILE=conda,lowmem
FETCHM_WEBAPP_QUALITY_CHECKM2_DB_DIR=/app/fetchm_webapp/data/external_tools/databases/checkm2
NXF_SYNTAX_PARSER=v1
```

Clone or update the PanResistome workflow at:

```text
fetchm_webapp/data/external_tools/workflows/PanResistome
```

GTDB-Tk is optional and heavy. Configure `FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH` only after the GTDB reference data is installed separately.

Keep `NXF_SYNTAX_PARSER=v1` while PanResistome still uses legacy Nextflow syntax
under Nextflow 26+.

## Audit Logging

The app records an append-only `audit_log` table for:

- Sign-in success/failure and logout.
- Login/password-reset rate-limit events.
- Password reset requests.
- Admin POST actions.
- Job creation.
- Selected metadata/download events.

Use `/admin/audit-log` to view recent events. Use `Download audit bundle` to export:

- `audit_log_latest_1000.csv`
- `job_analytics.json`
- Latest final audit, quality audit, and source/sample/environment audit files.
- Final dashboard, release note, publication summary, and remaining-batch review tables when present.
- `README.md`

## Metadata Standardization Operations

Production standardization is deterministic and rule-file based. Main rule files:

- `fetchm_webapp/standardization/host_synonyms.csv`
- `fetchm_webapp/standardization/host_negative_rules.csv`
- `fetchm_webapp/standardization/controlled_categories.csv`
- `fetchm_webapp/standardization/geography_reviewed_rules.csv`
- `fetchm_webapp/standardization/collection_date_reviewed_rules.csv`
- `fetchm_webapp/standardization/approved_broad_categories.csv`

After changing rules:

1. Run controlled-category or targeted audit scripts.
2. Queue genus-only standardization refresh.
3. Run final metadata audit.
4. Confirm production gate passes.
5. Commit rule changes and audit results.

Latest production audit artifacts:

- `fetchm_webapp/standardization/review/final_audit/20260505_100958/production_readiness_gate.json`
- `fetchm_webapp/standardization/review/final_audit/20260505_100958/final_metadata_standardization_dashboard.md`
- `fetchm_webapp/standardization/review/metadata_standardization_publication_summary.md`

## Production Gate Expectations

Hard failures should remain at zero:

- File errors.
- Non-country values in `Country`.
- Country-continent or country-subcontinent mismatches.
- Invalid host-like `Sample_Type_SD` rows.
- Noisy/unapproved broad values.
- Body-site leakage.
- Disease/source leakage.
- Raw code/text leakage.
- Controlled-category duplicate/conflict keys.
- Regression test failures.

Soft warnings include unexpectedly low coverage or large review queues.

## Backups

Back up at minimum:

- `fetchm_webapp/data/fetchm_webapp.db`
- `fetchm_webapp/data/jobs/`
- `fetchm_webapp/data/species/`
- `fetchm_webapp/data/metadata/`
- committed standardization rule files

Use SQLite-aware backup or stop writes before copying the database.

## Release and Rollback

Current formal release tag:

```bash
git checkout 2026.05-genus-v1.1
```

To revert a later change:

```bash
git revert <commit-sha>
```

Prefer small commits for UI, security, rule, and audit changes so rollback remains precise.

## Deferred Enhancements

- Multi-factor authentication.
- Role-based admin permissions.
- API token management and quotas.
- Email/in-app notifications beyond current job emails.
- More advanced admin charts and per-taxon completeness rollups.
