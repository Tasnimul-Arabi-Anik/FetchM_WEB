# Hidden Virus Model Contract

Status: hidden scaffold. Public Virus release remains disabled.

## Canonical Entities

- `common_record`
- `virus_sequence`
- `virus_genome_group`
- `biosample`
- `taxon_relationship`
- `standardized_metadata_payload`

## Tables Reserved for Hidden Virus Data

- `domain_virus_sequence_record`
- `domain_virus_genome_group`
- `domain_taxon_relationship`

## Semantics

Virus records remain viral records even when the target host is bacterial, archaeal, or eukaryotic. Host evidence is represented as taxon relationships (`natural_host`, `propagated_in`, `predicted_to_infect`, `isolated_from_host`) instead of changing the record domain. Segmented viruses are represented by one or more `virus_sequence` records grouped by `virus_genome_group`.

## Current Boundary

This batch defines the hidden model, source-adapter fields, schema placeholders, and tests. It does not build a real Virus snapshot and does not expose Virus publicly.

## Validation

- `python -m unittest tests.test_domain_profiles`: 8 passed.
