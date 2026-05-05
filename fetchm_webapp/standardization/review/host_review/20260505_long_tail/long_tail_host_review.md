# Long-Tail Host Review

Generated: 2026-05-05

This pass reviewed the remaining low-frequency `top_host_review_needed.csv`
values after the production-ready metadata audit.

## Policy

- Add exact species-level rules only for unambiguous common names resolved by
  TaxonKit or already established taxonomy.
- Use broad host ranks for ambiguous common names such as bird/fish/plant group
  labels.
- Do not guess abbreviated or mixed scientific names.
- Keep unresolved/mixed scientific names in `review_needed`.
- Block geography, sample-device, and food-processing source descriptors from
  `Host_SD`.

## Applied Rules

- Host synonym rules added: 38
- Host negative rules added: 5

Detailed rules are in `long_tail_host_rules.csv`.

## Intentionally Left For Review

Examples left untouched include:

- abbreviated scientific names such as `D. marginatus`, `B. minor`, and
  `N. sp. 246`
- mixed taxa such as `Crasosstrea spp., Farfantepenaeus spp.`
- TaxonKit-unresolved scientific names such as `Perognathus penicillatus`,
  `Carpobrotus rossii`, `Oryctes gigas`, `Trididemnum orbiculatum`, and
  `Discodermia calyx`
- ambiguous free-text values where host/source/sample policy is not certain

These are long-tail curation items, not pipeline failures.
