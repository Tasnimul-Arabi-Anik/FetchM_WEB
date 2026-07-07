# Hidden Virus QA Gate Scaffold

Status: hidden QA gate scaffold. Public Virus release remains disabled.

## Implemented

- `tools/qa_hidden_virus_pipeline.py` validates hidden Virus sequence records.
- Checks genome group IDs and segment-count consistency.
- Checks taxon relationships point to existing Virus subjects.
- Checks controlled relationship types, target domains, and confidence labels.
- Reports relationship-type and target-domain distributions.
- Verifies any matching hidden inventory snapshot remains `admin_hidden` and release locked.

## Boundary

This QA gate validates the hidden Virus model tables. It does not perform a live Virus build, does not publish Virus data, and does not regenerate Global Insights.

## Validation

- `python -m unittest tests.test_domain_profiles`: 15 passed.
