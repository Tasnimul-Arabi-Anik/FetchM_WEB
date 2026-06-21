# Hidden Archaea Readiness Gate

Snapshot ID: `20260621T100000Z_genbank_archaea_root`
Generated: 2026-06-21T15:35:35.597436+00:00

## Decision

Archaea is technically ready as a hidden staging database, but it is not public-release ready.

## Hidden Staging Checks

| Check | Result |
| --- | ---: |
| Required files present | `true` |
| Root-unique assemblies | 41,956 |
| Standardized assemblies | 41,956 |
| Missing standardized assemblies | 0 |
| High-risk rule-reuse rows | 0 |
| Hidden database ready | `true` |
| Hidden standardization audit pass | `true` |
| Hidden visibility pass | `true` |
| Hidden staging ready | `true` |

## Public Release Gate

Public release ready: `false`

Archaea remains hidden. Public UI exposure, public Global Insights regeneration, and deployment remain blocked until the following are complete:

- Archaea-specific metadata curation batches are not complete.
- Manual validation of Archaea standardized metadata is not complete.
- Public Archaea Global Insights have not been generated or reviewed.
- Public UI scope exposure has not been approved.

## Recommended Next Step

Run Archaea-specific hidden curation and manual validation before any public UI or Global Insights exposure.
