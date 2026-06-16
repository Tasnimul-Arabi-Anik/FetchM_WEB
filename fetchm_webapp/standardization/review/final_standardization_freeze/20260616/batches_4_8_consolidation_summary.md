# Batches 4-8 Consolidation Summary

Batches 4-8 consolidation refreshed source/sample/environment standardization after reviewed curation updates. Clear disease and health-state terms are routed into dedicated disease/health-state fields, while specimen materials, anatomical sites, environment terms, food terms, and generic clinical context are preserved in their appropriate fields. Ambiguous outbreak, surveillance, contaminated-food, carrier, colonized, and generic clinical terms remain admin-review unless explicit disease evidence is present.

This consolidation does not infer disease from generic clinical context. Disease routing is conservative and evidence-based.

- Status: **pass**
- Rows audited: 3,131,699
- Isolation_Source_SD coverage: 58.88%
- Raw-present standardization: 80.97%
- Hard leakage after: 0
- Broad leakage after: 0
- Controlled-category duplicate/conflict keys: 0/0
- Global Insights snapshot: `20260616T114556Z_global_insights`
- Deployment: manual only; gate intentionally reports safe_to_deploy=false.
