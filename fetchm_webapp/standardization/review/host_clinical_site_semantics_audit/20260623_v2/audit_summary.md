# Host, Clinical, And Site Semantics Audit V2

Snapshot ID: `20260602T140414Z_genbank_bacteria_root`
Generated: 2026-06-23T16:51:53.602181+00:00
Input standardization commit: `28ff0c440ebc0a7351140a69c01427355b7b4fe9`
Audit code commit: `9569d4d80b27aae418b19410247cc8f3415febe3`

## Scope

Phase 1 audit only. No production standardization rules, canonical metadata, Global Insights, or deployment state were changed.

## Dataset

| Metric | Value |
| --- | ---: |
| Canonical rows audited | 3,131,699 |
| Distinct standardized field values classified | 24,333 |
| All candidate assignment occurrences | 7,257,023 |
| Field-value pairs with queue signals | 289 |
| Field-value pairs with any semantic candidate signal | 18,690 |
| Unique assemblies with any semantic candidate signal | 2,023,875 |
| Unique assemblies with confirmed high-confidence signal | 30,740 |
| Unique assemblies with review/uncertain signal | 1,505,911 |

## Action Classes

| Action class | Assignment occurrences |
| --- | ---: |
| `additive_axis_enrichment` | 3,716,073 |
| `classifier_uncertain` | 721,052 |
| `composite_requires_split` | 1,857,357 |
| `confirmed_high_confidence_fix` | 30,853 |
| `legacy_compatibility_label` | 190,284 |
| `manual_review` | 741,404 |

## Queue Summary

| Queue | Values | Assignment count |
| --- | ---: | ---: |
| `non_material_values_in_sample_type` | 76 | 237,584 |
| `processing_values_in_sample_type` | 17 | 118,792 |
| `material_values_in_isolation_source` | 161 | 67,617 |
| `conflicting_anatomical_site_fields` | 10 | 56,594 |
| `environment_values_in_sample_type` | 24 | 39,739 |
| `non_site_values_in_isolation_site` | 8 | 23,368 |
| `non_health_values_in_host_health_state` | 13 | 11,816 |
| `non_disease_values_in_host_disease` | 2 | 5,660 |
| `care_setting_values_in_health_state` | 2 | 1,680 |
| `ambiguous_broad_source_categories` | 7 | 1,215 |
| `study_group_values_in_health_state` | 1 | 1,077 |
| `colonization_values_in_health_state` | 2 | 940 |
| `vital_status_values_in_health_state` | 2 | 329 |
| `disease_stage_values_in_health_state` | 2 | 49 |

## Interpretation

This V2 audit uses compositional classification. Counts are candidate signals and decomposition opportunities, not confirmed erroneous records. `confirmed_high_confidence_fix` is intentionally limited to reviewed strict host-health, host-disease, and isolation-site violations; environment and legacy compatibility candidates remain review or additive queues. Legacy umbrella fields such as `Isolation_Source_SD` and `Sample_Type_SD` are not treated as strict ontologies; strict derived axes are proposed separately.

## Recommended Next Step

Review confirmed high-confidence fixes and composite split candidates. Do not start broad remapping until the field contract is approved.
