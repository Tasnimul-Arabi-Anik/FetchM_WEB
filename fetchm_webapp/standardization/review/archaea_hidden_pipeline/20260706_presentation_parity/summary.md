# Hidden Archaea Presentation Parity Increment

Status: `hidden_archaea_presentation_parity_increment_complete`

## Scope

- Expanded admin-only Archaea taxon reports with bacterial-style metadata coverage, completeness, provenance, and distribution summaries.
- Added admin-only standardized metadata CSV export for hidden Archaea genus/species reports.
- Kept public Archaea release locked and public bacterial routes unchanged.

## Live Data Validation

- Snapshot: `20260706T163621Z_genbank_archaea_root`
- Taxon: `Methanobrevibacter` genus
- Rows: 968
- Species-level labels: 50
- Countries: 37
- Collection span: 1983-2024
- Complete genomes: 16
- Profile: `archaea_hidden_v1`
- Release status: `locked_admin_hidden`

## Validation

- Focused hidden Archaea report/export tests: OK
- Full unittest discovery: 156 tests OK
- `py_compile`: passed
- `git diff --check`: passed

## Safety

- Public release remains disabled.
- Hidden Archaea remains admin-only.
- Bacterial canonical tables and public routes were not changed.
- No large generated row-level artifacts were committed.

## Remaining Before Public Release

- Separate manual public-release approval remains required.
- Archaea-specific readiness thresholds and public Global Insights are not enabled.
- Admin-only sequence/QC workflow parity can be added later if needed.
