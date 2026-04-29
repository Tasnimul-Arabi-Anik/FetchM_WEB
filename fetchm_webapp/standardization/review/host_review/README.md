# Host Review Workflow

Use this workflow to improve host standardization without forcing uncertain values into `Host_SD`.

## Review Decisions

- `approve_host`: real host organism; add `proposed_host_sd` and `proposed_taxid`.
- `non_host_source`: not a host; do not map to `Host_SD`.
- `missing`: absent/not applicable/unknown.
- `broad_host`: broad host group, for example bird, plant, mammal.
- `needs_manual_review`: unclear or not yet validated.

## Unresolved Host Classes

Do not treat every unresolved host value as the same problem. The audit now writes `unresolved_class` in `host_unmapped_review_queue.csv` and summarizes it in `host_unresolved_classification.csv`.

Current classes:

- `taxonomic_name_needing_taxid_lookup`: likely scientific name; resolve with NCBI Taxonomy before approval.
- `sample_source_misplaced_in_host`: source/sample/environment term found in the host field; route to controlled source/sample/environment mapping.
- `missing_value`: absent, unknown, not applicable, or not provided.
- `broad_host`: broad biological group such as bird, plant, mammal, poultry, or fish.
- `ambiguous_free_text`: unclear text that needs manual review or later embedding/AI-assisted triage.

Latest genus-level unresolved split:

- `35,316` rows: `taxonomic_name_needing_taxid_lookup`.
- `9,344` rows: `ambiguous_free_text`.
- `501` rows: `broad_host`.
- `201` rows: `sample_source_misplaced_in_host`.

## NCBI Taxonomy Resolution

The audit writes unresolved scientific-name candidates to:

- `host_taxonomy_name_candidates.csv`

To resolve locally with TaxonKit:

```bash
tail -n +2 host_taxonomy_name_candidates.csv > host_names.txt
taxonkit name2taxid host_names.txt > host_name_taxid.tsv
python /app/fetchm_webapp/tools/merge_host_taxonomy_resolution.py host_name_taxid.tsv --dry-run
```

If the dry run looks correct, rerun without `--dry-run` to append new high-confidence rules to:

- `fetchm_webapp/standardization/host_synonyms.csv`

After appending rules, rebuild/restart the app and queue a standardization refresh. The refresh task reloads stored metadata rows, applies `ensure_managed_metadata_schema()`, rewrites clean outputs, and marks the task done.

## Non-Host Source Terms

Source/sample/environment values should not be forced into `Host_SD`. They should either remain as non-host source values or be routed through:

- `fetchm_webapp/standardization/controlled_categories.csv`

Examples include stool, meat, food product, environmental sample, hospital environment, soil metagenome, freshwater metagenome, and marine algae.
