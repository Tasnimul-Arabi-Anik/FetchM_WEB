# Hidden Archaea Operational Build Summary

Status: `admin_hidden_operational_build_complete`

- Branch: `hidden-archaea-pipeline-backend`
- Source commit: `a89fd6c9a321102b1588b29835f194a4e7c8c248`
- Snapshot: `20260706T163621Z_genbank_archaea_root`
- Domain: `archaea` / TaxID `2157`
- Visibility: `admin_hidden`
- Public release: locked (`public_enabled=false`)

## Inventory

- Status: completed
- Root unique assemblies: 44,183
- Raw records: 44,183
- Noncanonical records: 0
- Duplicate records: 0
- Completed pages: 45

## Metadata

- Status: completed
- Standardized assemblies: 44,183 / 44,183
- Missing standardized assemblies: 0
- Profile: `archaea_hidden_v1`
- Release status: `locked_admin_hidden`
- Batches complete: 2,210

## Schedule

- Enabled: true
- Interval: 60 days
- UTC hour: 18
- Currently due: false

## Admin Validation

- `Methano` top result: `Methanobrevibacter`
- Search count/report count: 968 / 968
- Unauthenticated `/admin/archaea`: 302 to `/login?next=/admin/archaea`

## Validation

- Full unittest discovery: 154 tests OK
- `py_compile`: passed
- `git diff --check`: passed

## Safety Notes

- Archaea data are stored in separate `domain_*` PostgreSQL tables.
- Bacterial canonical metadata, public routes, and Global Insights were not converted to Archaea.
- Public Archaea release remains locked until a future manual approval and QA release task.
