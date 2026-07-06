# Hidden Virus Persistence Scaffold

Status: hidden persistence scaffold. Public Virus release remains disabled.

## Implemented

- `seed_virus_canonical_entities_batch()` persists hidden Virus sequence records.
- `seed_virus_canonical_entities_batch()` persists hidden Virus genome groups.
- `seed_virus_canonical_entities_batch()` persists hidden taxon relationships for host/lab-host/predicted-host/source-host evidence.
- `fetch_domain_missing_metadata --domain virus` now writes Virus entities after standardized payload seeding.
- `tools/import_hidden_virus_sequences.py` can import reviewed JSON/JSONL NCBI Virus-style reports into the hidden Virus model.

## Boundary

This batch does not run a live Virus build, does not publish Virus data, and does not regenerate Global Insights. It creates the persistence path needed before a hidden Virus snapshot can be built and validated.

## Validation

- `python -m unittest tests.test_domain_profiles`: 12 passed.
