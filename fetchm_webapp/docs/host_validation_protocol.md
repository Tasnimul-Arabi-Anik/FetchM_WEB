# Host Metadata Validation Protocol

This protocol supports manuscript-level validation of FetchM host standardization.

## Review File

Use `global_insights/host_validation_review_sample.csv`.

The sample is balanced across four groups:

- `host_primary_high_confidence`: primary Host field mapped by dictionary or cleaned-match rules.
- `host_secondary_evidence_recovery`: Host was recovered from reviewed secondary context.
- `host_unresolved_or_missing`: Host remains missing or unresolved.
- `host_source_like_edge_case`: source/sample descriptors that should usually remain non-host.

## Reviewer Decisions

Fill these columns:

- `reviewer_label`: reviewer's accepted host label, `unresolved`, or `not_host`.
- `reviewer_decision`: use `correct`, `false_positive`, or `unresolved`.
- `error_type`: optional; suggested values are `wrong_host`, `overcalled_source_as_host`, `missed_host`, `ambiguous_submitter_metadata`, or `taxonomy_resolution`.
- `notes`: optional reviewer explanation.

## Rules

A host assignment is `correct` when the standardized host is supported by the primary Host field or by explicit secondary evidence.

A host assignment is `false_positive` when FetchM maps a source, sample material, food product, environmental descriptor, disease term, or laboratory descriptor as a biological host.

An unresolved host is `correct` if the available metadata do not support a biological host assignment. It is `false_positive` only if the reviewer identifies clear host evidence that FetchM missed.

## Publishing Reviewed Results

After review, import the completed file with the guarded importer:

```bash
python tools/import_validation_records.py global_insights/host_validation_review_sample.csv
```

The importer refuses blank or partially reviewed rows and writes `global_insights/validation_records.csv` only after every row has `reviewer_label` and a valid `reviewer_decision`.

Regenerate Global Insights after import. The Validation Accuracy panel will summarize host precision, false-positive rate, unresolved rate, and common error types.
