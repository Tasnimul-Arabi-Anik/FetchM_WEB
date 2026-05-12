# FetchM Web UI Design System

FetchM Web uses a custom scientific SaaS theme implemented in `static/styles.css`.
The theme is intentionally applied to the existing Flask/Jinja templates to avoid
the risk of a frontend stack rewrite.

## Visual Direction

The interface should feel like a research control room: calm, technical, and
trustworthy. The palette combines deep navy, teal, seafoam, warm paper, and a
small amber accent. Dense data views should remain readable before becoming
decorative.

## Core Tokens

- `--bg`, `--bg-deep`: page atmosphere and dark scientific gradients.
- `--paper`, `--paper-strong`, `--paper-glass`: cards, panels, and translucent surfaces.
- `--ink`, `--ink-soft`, `--muted`: primary, secondary, and subdued text.
- `--accent`, `--accent-strong`, `--accent-bright`, `--accent-soft`: primary action system.
- `--secondary`, `--secondary-soft`, `--gold`: supporting action and highlight colors.
- `--radius-sm` through `--radius-xl`: rounded form language for controls and panels.
- `--shadow`, `--shadow-soft`, `--shadow-focus`: elevation and accessibility focus rings.

## Implementation Rules

- Prefer extending tokens rather than adding one-off hex colors.
- Keep pages within the existing Jinja/CSS system unless there is a separate
  migration plan for a frontend framework.
- Use `button`, `.button-like`, `.panel`, `.metric-card`, `.metadata-tabbar`,
  and `.fetchm-action-card` as shared primitives.
- Do not widen dashboards in ways that require horizontal page scrolling.
- For data-heavy pages, prioritize compact filters, wrapped chips, sticky
  navigation, and clear overflow inside tables only.

## QA Expectations

- Login, register, homepage, metadata analysis, sequence download, quality check,
  and admin pages should remain keyboard navigable.
- Buttons and links must show a visible focus ring.
- The deployed app should be checked at desktop and mobile widths before pushing
  major UI changes.
- Browser smoke tests live under `tools/ui_smoke_test.py`.
