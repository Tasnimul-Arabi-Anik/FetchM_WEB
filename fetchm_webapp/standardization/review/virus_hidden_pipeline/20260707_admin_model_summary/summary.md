# Hidden Virus Admin Model Summary

Status: hidden admin presentation scaffold. Public Virus release remains disabled.

## Implemented

- `hidden_virus_model_summary()` aggregates persisted Virus sequence records, genome groups, host/taxon relationships, molecule types, and examples.
- `/admin/virus` displays hidden Virus model metrics separately from assembly-style inventory metadata.
- `/admin/virus/<rank>/<name>` displays model metrics filtered by organism query.
- The admin template labels host/taxon relationships as relationship data, not as bacterial/archaeal record domains.

## Boundary

This does not run a live Virus build and does not expose Virus publicly. It makes persisted hidden Virus model data reviewable by admins once imported or built.

## Validation

- Focused hidden Virus/admin tests: 18 passed.
