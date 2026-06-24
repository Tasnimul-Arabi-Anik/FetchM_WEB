# Phase 2A Context Exception Correction

- Snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows scanned: 3,131,699
- Rows changed: 30,738
- Provenance method entries migrated: 76,046
- Patient clinical-subject clears: 733
- Patient human-context clears: 386
- Colonization clears: 9
- Vital-status clears: 13
- Bare-fluid clears: 0
- Canonical write run: `true`
- Backup table: `assembly_standardization_backup_semantic_exception_20260624201244`

Normal metadata refresh now invokes the shared semantic completion transformer after host/source/sample/environment standardization and before persistence.
