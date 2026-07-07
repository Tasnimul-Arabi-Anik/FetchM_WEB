# Hidden Virus Pipeline Scaffold

Status: admin-only scaffold. Public Virus release remains locked.

## Implemented

- Virus domain option in the admin pipeline selector.
- `/admin/virus` search, report, CSV export, inventory queue, metadata fetch queue, and schedule controls.
- Hidden Virus profile tagging with `virus_hidden_v1`.
- Hidden-domain QA profile support for Virus.
- Scheduler due-check support for non-bacterial hidden domains.

## Explicitly Not Complete

- No viral sequence or segment canonical model has been built.
- No virus-specific host relationship model has been built.
- No virus-specific readiness, completeness, or validation thresholds have been approved.
- No real Virus canonical snapshot was generated in this scaffold batch.
- Public Virus search and release are disabled.

## Validation

- Targeted hidden-domain tests: 10 passed.
- Full `python -m unittest discover -s tests -p 'test_*.py'`: 162 passed.
- `python -m py_compile` on modified Python files: passed.
- `git diff --check`: passed.

## Safety

This scaffold does not change bacterial public release behavior, does not alter the hidden Archaea pipeline, does not run Virus standardization, and does not regenerate Global Insights.
