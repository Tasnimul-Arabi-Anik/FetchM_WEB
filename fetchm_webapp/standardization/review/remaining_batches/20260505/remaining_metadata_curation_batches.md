# Remaining Metadata Curation Batches

- Generated: 2026-05-05T04:27:26.520130+00:00
- Applied to rule files: yes

## Batch 3: Source-Like Mapped Host Spot-Check

- Reviewed rows: 1,000
- Decisions: keep_host_add_sample_type=930, lab_artifact_not_host=15, remove_host_route_to_source=55
- Host negative rules proposed: 70
- Controlled routing rules proposed: 54

## Batch 4: Source-Like Unmapped Host Routing

- Reviewed rows: 161
- Decisions: retained_manual_review=16, route_non_host_source=145
- Controlled routing rules proposed: 52

## Batch 5: High-Confidence Source/Sample/Environment Suggestions

- Reviewed suggestions: 1,000
- Decisions: apply=833, skip=167
- Controlled rules proposed after safety filters: 103

## Batch 6: Final Low-Frequency Host Pass

- Reviewed host values: 472
- Decisions: add_reviewed_host_synonym=12, leave_review_needed=460
- Host synonym rules proposed: 9

## Applied Counts

- Controlled category rows appended: 209
- Host negative rows appended: 58
- Host synonym rows appended: 9

Generated files:

- `suspicious_source_like_mapped_hosts_reviewed.csv`
- `source_like_unmapped_hosts_routing_reviewed.csv`
- `applied_high_confidence_rules_20260505_review.csv`
- `host_final_review_clustered.csv`
- `controlled_rules_all_proposed.csv`
- `host_negative_rules_all_proposed.csv`
- `host_synonyms_batch6_proposed.csv`