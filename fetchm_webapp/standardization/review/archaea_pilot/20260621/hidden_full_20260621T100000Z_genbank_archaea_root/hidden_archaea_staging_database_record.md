# Hidden Archaea Staging Database Record

Snapshot ID: `20260621T100000Z_genbank_archaea_root`

## Local staging database

- Docker container: `fetchm_archaea_hidden_db`
- Docker volume: `fetchm_archaea_hidden_pgdata`
- Docker network: `fetchm_archaea_hidden_net`
- PostgreSQL database: `fetchm_archaea_hidden`
- Visibility: hidden staging only

## Metrics

| Metric | Value |
| --- | ---: |
| Root-unique Archaea assemblies | 41,956 |
| Standardized assemblies | 41,956 |
| Missing standardized assemblies | 0 |
| Rule-reuse risk rows | 0 |
| High-risk rule-reuse rows | 0 |

## Boundaries

- This database is not the public bacterial production database.
- Archaea is not exposed in public search, public Global Insights, downloads, or NAR-facing pages.
- Public bacterial Global Insights were not regenerated.
- Deployment was not run.
