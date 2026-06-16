# Batch 7 Disease and Health-State Curation

Generated: 2026-06-16T06:36:46.552179+00:00

## Scope

Lightweight-only Batch 7 curation for disease, infection, clinical condition, symptom, and health-state values found in source/sample/environment fields. No canonical refresh, Global Insights regeneration, or deployment was run.

## Results

- Values reviewed: 30
- Deterministic controlled-category rows added: 16
- Code/helper updates: 3
- Admin-review values retained unresolved: 11
- Targeted hard leakage after: 0
- Targeted broad leakage after: 0
- Controlled-category duplicate/conflict keys: 0/0
- Regression tests: 75 passed

## Decisions

- Clear disease terms route to `Host_Disease_SD`.
- Clear health-state terms route to `Host_Health_State_SD`.
- Disease + specimen phrases split into disease plus `Sample_Type_SD`.
- Disease + body-site phrases preserve `Isolation_Site_SD` when explicit.
- `clinical sample` and `respiratory sample` remain context-bearing aggregate labels, not disease labels.
- Outbreak, surveillance, contaminated-food, carrier, colonized, and generic clinical terms remain admin-review unless disease evidence is explicit.

## Deferred

Canonical coverage/review-signal counts are deferred to the Batches 4-8 consolidation refresh.
