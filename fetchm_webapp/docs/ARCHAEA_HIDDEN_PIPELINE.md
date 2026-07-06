# Hidden Archaea Pipeline

This pipeline prepares Archaea in the background without changing the public bacterial release. It mirrors the bacterial canonical workflow only where the infrastructure is safely reusable. Biological rules, QA gates, readiness thresholds, and release approval remain domain-specific.

## Current Scope

Implemented in this milestone:

- Admin selector can choose `Archaea` as a hidden preparation lane.
- PostgreSQL storage uses separate `domain_*` tables and rejects `bacteria` on the hidden-domain path.
- Archaea root inventory can be built from NCBI Datasets REST API using TaxID `2157`.
- Hidden inventory status is visible in Admin only.
- Public search, sequence/QC entrypoints, Global Insights, and release activation remain locked.

Not implemented yet:

- Archaeal metadata fetch and standardization.
- Archaeal rule pack and QA gate.
- Admin-only archaeal taxon search/report pages.
- Scheduled hidden Archaea runs.
- Manual public promotion.

## Inventory Command

Validation run:

```bash
python tools/build_domain_root_inventory.py --domain archaea --max-pages 1
```

Full hidden inventory run:

```bash
python tools/build_domain_root_inventory.py --domain archaea
```

The default snapshot ID is formatted as:

```text
YYYYMMDDTHHMMSSZ_genbank_archaea_root
```

## Release Lock

Every hidden-domain snapshot is created with:

```text
visibility = admin_hidden
release_locked = true
```

Do not expose Archaea publicly until a separate reviewed release task explicitly unlocks it after metadata coverage, QA, and manuscript-readiness checks pass.

## Safety Rules

- Do not run bacterial canonical inventory tools with TaxID `2157`.
- Do not write Archaea rows into `bacterial_inventory_snapshot`, `bacterial_inventory_membership`, or `assembly_standardization`.
- Do not reuse bacterial biological thresholds as archaeal acceptance criteria without review.
- Preserve bacterial public behavior while Archaea remains hidden.
