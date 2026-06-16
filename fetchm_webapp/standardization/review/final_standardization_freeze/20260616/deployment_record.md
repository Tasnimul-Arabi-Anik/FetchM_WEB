# Deployment Record: Metadata Standardization Freeze

Deployment timestamp: 2026-06-16T12:31:41Z

## Release

- Deployed commit: `aa84f2cee9ae155ecd61e0bb3efc7f05a07eac76`
- Commit prefix verified in live web container: `aa84f2c`
- Global Insights snapshot: `20260616T114556Z_global_insights`
- Canonical source snapshot: `20260602T140414Z_genbank_bacteria_root`
- Deployment approval: manual approval provided in chat before deployment

## Deployment Action

The active Docker Compose stack was rebuilt from the approved release commit and restarted. The legacy `docker-compose` recreate path raised a `ContainerConfig` error during in-place recreation, so the stack was recovered by removing/recreating containers without deleting named volumes. Data volumes were preserved.

Runtime `.env` commit metadata was updated to the deployed commit and services were restarted from the rebuilt images.

## Health Checks

| Check | Result |
| --- | --- |
| Web health endpoint `/healthz` | pass; returned `status: ok` and deployed commit |
| Web container health | pass |
| Dataset Postgres health | pass; `pg_isready` accepting connections |
| App database connection | pass; `select 1` returned `1` |
| Caddy/public route health | pass |
| Static asset load | pass; `/static/styles.css?v=20260512-saas-theme` returned HTTP 200 |
| Admin route protection | pass; `/admin` redirected to `/login?next=/admin` |
| Global Insights route | pass; `/global-insights` returned HTTP 200 |
| Global Insights summary JSON | pass; snapshot `20260616T114556Z_global_insights` visible |

## Standardization Smoke Tests

| Input | Expected behavior | Result |
| --- | --- | --- |
| `waterlettuce` | `Pistia stratiotes`, TaxID `4477` | pass |
| `water lettuce` | `Pistia stratiotes`, TaxID `4477` | pass |
| `shorebird` | `Charadriiformes`, TaxID `8906` | pass |
| `Cuttloefish` | `Sepiidae`, TaxID `6608` | pass |
| `ground turkey` | food/meat context, not `Country` | pass |
| `benzene-degrading enrichment culture` with `Sample Type=culture` | no duplicate `Isolation_Source_SD=culture`; `Sample_Type_SD=culture` | pass |
| `clinical sample` | clinical/host-associated context; no inferred disease | pass |
| `wastewater surveillance` | environmental material + wastewater; no inferred host disease | pass |
| `canal water` | environment/canal context, not anatomical site | pass |
| `ear canal` | anatomical/site context, not environmental water | pass |

## Global Insights Provenance Smoke Check

The live Global Insights summary JSON reports:

- Source/sample/environment rows audited: `3,131,699`
- Source/sample/environment hard exact leakage rows: `0`
- Source/sample/environment broad vocabulary leakage rows: `0`
- Controlled categories checksum: `1c1b86c7df6c32736f9f3fa02d5a7b871b60e7793e4e9196867ec6c16de0eb2f`
- Host microbial leakage count: `0`
- Geography/date QA timestamp: `2026-06-16T11:41:01.131608+00:00`
- Source/sample/environment QA timestamp: `2026-06-16T11:26:33.856388+00:00`

## Notes

No new curation batches were run during deployment. No canonical metadata refresh or Global Insights regeneration was triggered during deployment; the release used the already frozen snapshot and generated Global Insights artifacts from commit `aa84f2c`.

Metadata standardization is complete for this release. Remaining admin-review values are low-priority/context-dependent and are not deployment blockers.
