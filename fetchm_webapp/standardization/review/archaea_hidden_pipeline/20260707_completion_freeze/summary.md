# Hidden Archaea Completion Freeze

- Status: `hidden_archaea_admin_database_complete_for_review`
- Branch: `hidden-domain-admin-pipelines-v1`
- Branch commit: `5931a4ad42e0a09e583172dd590531a0322b550d`
- Main base commit: `24121f70a57d2baa9b058d1c850fe15e52051623`
- Snapshot: `20260706T163621Z_genbank_archaea_root`
- Root assemblies: 44,183
- Standardized assemblies: 44,183
- Missing standardized assemblies: 0
- Hidden-domain QA: pass; hard failures 0
- Public enabled: false
- Release locked: true

## Admin Presentation Validation

- Search query: `Methano`
- Top result: `Methanobrevibacter` genus, 968 genomes
- Taxon report rows: 968
- Metadata CSV rows: 968
- Report release lock: true

## Validation

- `tests.test_domain_profiles`: 22 passed
- Full unittest discovery: 195 passed
- `py_compile`: passed
- `git diff --check`: passed

## Safety

- Bacterial canonical metadata changed: false
- Global Insights regenerated: false
- Public Archaea routes enabled: false
- Bucket B/environment semantic rules applied: false
- Virus database build run: false

Archaea is complete as a hidden admin database for review. Public release remains blocked pending a separate explicit approval.
