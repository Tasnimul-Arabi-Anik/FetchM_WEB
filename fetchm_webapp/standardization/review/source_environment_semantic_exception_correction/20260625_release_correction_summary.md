# Source/Environment Semantic Exception Correction

- Snapshot: `20260602T140414Z_genbank_bacteria_root`
- Backup table: `assembly_standardization_backup_semantic_exception_20260624201244`
- Rows scanned: 3,131,699
- Rows changed: 30,738
- Phase 2A provenance methods migrated: 76,046
- Patient clinical-subject clears: 733
- Patient human-context clears: 386
- Colonization-status clears: 9
- Vital-status clears: 13
- Bare-fluid clears: 0
- Post-apply changed rows projected: 0

## QA

- Host QA: `pass`
- Geography/date QA: `pass`
- Source/sample/environment QA: `pass`
- Semantic-completion hard failures: 0
- Strict forbidden site values remaining: 0
- Patient environment-only additions: 0
- Legacy/raw/protected changes: 0/0/0
- Controlled duplicate/conflict keys: 0/0

## Tests

- Docker `python -m unittest discover -s tests -p test_*.py`: 117 tests passed.
- Exact discovered test names are recorded in `20260625_tests/discovered_unittest_names.txt`.
- Test-count note: Current corrective branch discovery count. Earlier 116 and 111 counts came from different branch states before this correction added focused semantic tests and before stale semantic assertions were updated.

## Release Status

- Global Insights regenerated as `20260624T213614Z_global_insights` with 3,131,699 rows.
- Deployment remains pending/manual.
