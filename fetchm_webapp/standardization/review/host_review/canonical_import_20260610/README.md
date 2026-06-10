# Reviewed Host Canonical Import

- Batch files: 6
- Raw decision rows: 648
- Unique normalized decisions: 635
- Batch conflicts resolved by latest batch: 0
- Existing microbial canonical synonyms flagged: 56
- Applied: yes

## Results

- `context_only`: 3
- `host_synonym_duplicate`: 379
- `host_synonym_updated`: 7
- `microbial_blocked`: 1
- `negative_duplicate`: 150
- `unresolved_to_not_identifiable`: 95

## Outputs

- `import_report.csv`
- `existing_microbial_host_synonyms.csv`

## Canonical File Changes vs. Previous Commit

- `host_synonyms`: 386 added, 0 updated, 61 pre-existing duplicate rows removed
- `host_negative_rules`: 249 added, 0 updated, 1 pre-existing duplicate row removed
- `host_context_rules`: 3 added, 0 updated, 0 removed/deduplicated

- `rule_file_changes.csv` records every changed normalized key.
