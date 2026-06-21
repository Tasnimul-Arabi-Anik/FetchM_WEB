# Hidden Archaea Pilot Snapshot Summary

Date: 2026-06-21
Branch: `archaea-domain-profile-foundation`
Snapshot ID: `20260621T000000Z_genbank_archaea_root`

## Result

The hidden Archaea inventory pilot completed successfully in a temporary isolated Docker Postgres database. It did not touch the production bacterial dataset database, did not expose Archaea in the public UI, did not run canonical metadata refresh, did not regenerate Global Insights, and did not deploy.

## Pilot Metrics

| Metric | Value |
| --- | ---: |
| Domain profile | `archaea` |
| NCBI taxon root | `2157` |
| Pilot page limit | 1 |
| Page size | 50 |
| Raw records fetched | 50 |
| Root-unique assemblies written | 50 |
| Noncanonical records | 0 |
| Duplicate records | 0 |
| NCBI expected total | 41,956 |
| Progress helper expected pages | 42 |
| Pilot status | `pilot_completed` |

## Validation

- Syntax validation passed for domain profiles, dataset store, inventory CLI, and tests.
- Focused domain-profile tests passed: 5 tests.
- Inventory CLI help exposes `--domain` and `--pilot-pages`.
- `git diff --check` passed.
- One-page Archaea pilot wrote only to a temporary isolated database.

## Boundary

This is not a production Archaea database yet. It proves that the shared inventory engine can target the Archaea root safely while preserving bacterial release isolation.

## Recommended Next Step

Keep Archaea hidden and run an audit-only Archaea metadata standardization assessment before adding public UI, Global Insights, or a full canonical Archaea refresh.
