# Batch 3 Anatomical And Body-Site Curation

Batch 3 refreshed and audited all 3,131,699 canonical bacterial metadata rows using implementation commits `59cdd43` and `fa9531f`.

## QA Result

- QA status: pass
- Regression tests: 71 passed
- Hard exact-source leakage: 0 before and after
- Broad vocabulary leakage: 0 before and after
- Rule duplicate/conflict keys: 0/0
- File errors: 0

## Body-Site Review

The focused body-site queue fell from seven values affecting 1,065 rows to two values affecting eight rows. Bare `canal` accounted for 1,052 pre-cleanup rows and is no longer forced into an anatomical or environmental destination.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Isolation_Site_SD rows | 404,239 (12.91%) | 399,121 (12.74%) | -5,118 |
| Site-routed source rows | 3,996 | 3,952 | -44 |
| Sample_Type_SD rows | 1,406,917 (44.93%) | 1,380,569 (44.08%) | -26,348 |
| Review-signal rows | 147,632 | 150,659 | +3,027 |
| Review-signal unique values | 1,615 | 1,609 | -6 |
| Environment-routed rows | 422,172 | 422,911 | +739 |
| Food-context rows | 164,796 | 168,924 | +4,128 |

The coverage decreases are intentional semantic corrections, not missing-data regressions:

- 20,952 body-site-only `wound` values no longer populate Sample_Type_SD.
- `wound swab` gained 2,317 correctly split sample rows.
- 5,692 false animal/commodity anatomical sites were removed across organ, gastrointestinal, and skin categories.
- 1,052 bare `canal` exact-source assignments were suppressed.
- Wound-site coverage increased by 586 rows.

## Ambiguity Decisions

Explicit anatomical phrases such as `ear canal`, `birth canal`, `anal canal`, `root canal`, and biliary canal route to Isolation_Site_SD. Explicit environmental phrases such as `canal water`, `canal sediment`, and `irrigation canal` retain environment routing. Context-free `canal`, `drainage`, and `surface` remain unresolved/manual review.

The unresolved raw queue contains 971 canal rows, 967 drainage rows, and 249 surface rows. They are intentionally not guessed.

## Context Protection

Food and animal commodity phrases such as `chicken breast meat`, `pork liver`, and `beef heart` retain food context rather than becoming clinical sites. `fish gut` and `oyster tissue` remain host/food context rather than human anatomy. Clinical and respiratory aggregate context remains preserved.

The overall review-signal row increase is caused by preserved food/host context and remains vocabulary triage, not hard leakage.
