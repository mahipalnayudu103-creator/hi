# UI Collapsible Options Design

## Goal

Keep the Renko playback screen clean by hiding advanced settings until the user opens them. The first view should show only the important workflow:

1. Choose CSV file.
2. Choose date range.
3. Build charts.
4. Play, pause, reset, and inspect charts.

Extra options should be grouped under collapsible headings, like an outline. The user can open a section when they need it and close it again when done.

## Main Layout

The left sidebar should feel like a compact control panel, not a long settings page.

Recommended order:

1. Data Source
2. Date Range
3. Build Renko
4. Playback
5. Advanced Options
6. Cache / Previous Builds
7. System Monitor

Each group should use a heading row with:

- A clear title.
- A small chevron icon.
- A short status value when useful.
- Click/tap support on the whole heading row.

Example:

```text
> Advanced Options
```

When opened:

```text
v Advanced Options
  Build Mode
  Processing Engine
  Chunk Size
  Price Source
  Anchor Mode
```

## Default Open / Closed State

Open by default:

- Data Source
- Date Range
- Build Renko
- Playback

Closed by default:

- Advanced Options
- Cache / Previous Builds
- System Monitor
- Debug / Console

If a section has an error or warning, it should open automatically and highlight the problem.

## Section Behavior

When closed:

- Hide all internal controls.
- Keep the heading visible.
- Show a short summary if helpful.

Examples:

```text
Advanced Options    CPU, Full build, Bid
Cache               Previous build available
System              CPU 18%, RAM 42%
```

When opened:

- Show controls immediately below the heading.
- Do not move the user to another page.
- Do not use a modal for normal settings.
- Preserve the open/closed state while the user works.

## Cache Reuse UI

The cache section should stay hidden unless there is useful information.

Show it when:

- Exact previous build is found.
- Larger previous build can satisfy smaller selected range.
- Similar previous builds exist for the same CSV.

Suggested closed heading:

```text
Cache / Previous Builds    Reuse available
```

Suggested opened content:

```text
Previous Build Found
File: EURUSD_Ticks.csv
Built Range: 2026-06-01 to 2026-06-10
Selected Slice: 2026-06-01 to 2026-06-05
Pips: 1p, 2p, 3p, 4p
Action: Use cached slice
```

The app should make the best cache choice automatically, but still show what it reused so the user trusts it.

## Visual Style

- Use compact rows.
- Use thin borders between groups.
- Avoid large cards inside the sidebar.
- Use small icons only where they help scanning.
- Keep button labels short.
- Use consistent spacing so opening a section does not feel messy.

Suggested heading row style:

- Height: 36-42px.
- Font size: 13-14px.
- Font weight: 600.
- Chevron on the right or left.
- Hover background slightly brighter.
- Open section body with 8-12px vertical spacing.

## Accessibility

Each collapsible heading should be a real button.

Required behavior:

- `aria-expanded="true"` when open.
- `aria-expanded="false"` when closed.
- `aria-controls` pointing to the section body.
- Keyboard support with Enter and Space.

## Implementation Notes

Use one reusable pattern for all sidebar groups:

```html
<section class="sidebar-section">
  <button class="section-toggle" aria-expanded="false" aria-controls="advancedOptionsPanel">
    <span>Advanced Options</span>
    <span class="section-summary">CPU, Full build</span>
    <span class="chevron">›</span>
  </button>
  <div id="advancedOptionsPanel" class="section-body hidden">
    ...
  </div>
</section>
```

Avoid duplicating toggle code for every section. Use one small JavaScript helper that finds all `.section-toggle` buttons and opens/closes the matching body.

