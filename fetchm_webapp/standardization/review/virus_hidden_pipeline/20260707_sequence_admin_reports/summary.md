# Hidden Virus Sequence Admin Reports

Status: hidden admin sequence-model reporting scaffold. Public Virus release remains disabled.

## Implemented

- Admin Virus taxon search now reads `domain_virus_sequence_record` for sequence-only Virus imports.
- Admin Virus taxon reports now summarize sequence accessions, molecule type, segment, completeness, BioSample, and payload coverage without forcing Virus records into assembly inventory.
- Hidden Virus metadata CSV export includes sequence-record columns.
- The admin template uses record-oriented labels for Virus reports.

## Boundary

This does not run a live Virus build, does not expose Virus publicly, and does not change bacterial or archaeal report paths.

## Validation

- `python -m py_compile fetchm_webapp/dataset_production_store.py fetchm_webapp/tests/test_domain_profiles.py`: passed.
- `python -m unittest tests.test_domain_profiles`: 19 passed.
- `python -m unittest discover -s tests -p 'test_*.py'`: 181 passed.
