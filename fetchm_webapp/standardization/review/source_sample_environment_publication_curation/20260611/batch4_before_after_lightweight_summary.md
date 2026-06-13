# Batch 4 environmental media lightweight summary

- Baseline commit: `c3e6e86`
- Workflow: lightweight only; no canonical refresh, deployment, or Global Insights regeneration
- Targeted values tested: 25
- Targeted values changed: 10
- Deterministic rules added: 15
- Admin-review values retained unresolved: 10
- Protected Batch 1/2/3 probes passed: 12/12
- Targeted hard leakage after: 0
- Targeted broad-vocabulary leakage after: 0
- Controlled-category duplicate/conflict keys: 0/0
- Regression tests: 72 passed

## Scope

High-confidence environmental phrases were routed to environment medium or context fields. False plant-anatomy and source-derived location/media pseudo-sample precedence was removed. Bare `canal`, `drainage`, `surface`, `influent`, `mud`, and `sand` remain unresolved; other context-sensitive terms are listed in `batch4_admin_review_needed.csv`.

Canonical coverage, review-signal counts, and Global Insights provenance are intentionally deferred until the Batches 4-8 consolidation checkpoint.
