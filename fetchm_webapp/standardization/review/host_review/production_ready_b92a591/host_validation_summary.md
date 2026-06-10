# Host Validation Summary

Production checkpoint: `b92a591`

Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`

| Metric | Value |
| --- | ---: |
| Canonical assemblies standardized | 3,131,699 |
| `Host_SD` non-empty | 1,706,778 |
| `Host_Context_SD` non-empty | 2,606 |
| Unresolved host rows | 596 |
| Non-allowlisted microbial leakage | 0 |
| Allowlisted microbial host rows | 0 |
| Validation sample size | 600 |

The validation sample contains 150 records from each of four groups: primary
high-confidence host matches, secondary-evidence recoveries, unresolved or
missing hosts, and source-like edge cases.

No completed manual labels were available for this checkpoint, so no numerical
false-positive rate is asserted. The review form captures likely false-positive
categories including wrong taxon, over-specific taxon, source/material leakage,
context-only value promoted to `Host_SD`, microbial leakage, and unsupported
secondary-evidence recovery.

The production QA gate passed with zero microbial leakage, zero context-only
promotion failures, zero reviewed exact-rule precedence failures, zero
eukaryotic-algae demotions, and zero resolved superkingdom lineage conflicts.
