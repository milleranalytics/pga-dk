# Handoff: PGA Slate Terminal

## Overview

A local-only, single-user web front end that replaces a Streamlit app for weekly PGA
DraftKings research. It shows a model-ranked field for the current tournament, a deep
player card that contextualizes any player against the rest of the field, and a lineup
builder with a salary-cap optimizer. It reads from two sources the owner already
maintains: a weekly CSV of model predictions exported from a Jupyter notebook, and a
~20 MB SQLite database of historical golf stats.

The owner's weekly workflow today: run the notebook (updates DB, retrains model, scores
the new field, exports CSV) → open the app → research players → build 1–N lineups →
export to DraftKings. Today steps 3–5 happen in Streamlit plus a separate Excel
optimizer. The goal is to collapse all of it into this one app.

## About the Design Files

The files in this bundle are **design references created in HTML** — working prototypes
demonstrating intended look and behavior, not production code to copy directly.

They are authored in a component format specific to the design tool they were made in
(a template plus a logic class, with inline styles). **Do not try to run or extend that
format in the target project.** Recreate the design in whatever environment the project
uses or should use.

If no codebase exists yet — the likely case here — pick the framework. For a single-user
local tool, the recommendation is **Vite + React + TypeScript** built to static files,
with **TanStack Table** for the grid (the field is only ~150 rows, so virtualization is
optional) and **sql.js** for database access. Avoid a heavyweight component library; the
design is dense and custom, and a Material/Chakra-style kit will fight it.

The **logic in these prototypes is real and worth porting directly**, in particular the
optimizer (an exact dynamic program, described in full below) and the field-percentile
flag engine. Those are documented as algorithms, not just visuals.

### Two layouts are included

- **`PGA Slate Terminal.dc.html` — layout A, the primary design.** Build this one.
- **`PGA Slate Board.dc.html` — layout B, an alternative direction** the owner reviewed
  and rejected as the primary, while adopting its lineup-rail treatment into A. It is
  included because its **compare board** — pin up to four players, every metric becomes
  a row across columns with the best value highlighted — is a feature the owner liked
  and may want later. Treat it as a documented future enhancement, not a build target.
  Its color scheme and typography are **not** the direction; A's are.

## Fidelity

**High-fidelity.** Colors, typography, spacing, and interaction behavior are final and
should be matched closely. Exact values are in Design Tokens below. Two caveats:

- The prototype's **round-level history, course history, and SG-by-phase splits are
  synthesized stand-in data**. The visual treatment of those sections is final; the data
  behind them must be replaced with real queries. See Data Layer.
- The **Prediction Tracker** and **Results Browser** tabs are stubs. They render a
  placeholder showing the SQL they need. Their intent is described below but they were
  never built out; implement them fresh following the established visual system.

---

## Data Layer

This is the part that most needs a decision made correctly, so it comes first.

### Sources

1. **Weekly predictions CSV** — exported by the notebook, ~150 rows, one per player in
   this week's field. Columns (verbatim, this is the real header):

   ```
   PLAYER, SALARY, P_TOP20, SCORE, MODEL_SCORE, ODDS_SHARE, LEVERAGE,
   VEGAS_ODDS, SG_FORM, PCT_FORM_SHRUNK, SG_CH_SHRUNK, CUT_PERCENTAGE,
   FEDEX_CUP_POINTS, OWGR_RANK
   ```

   Sample row: `Cameron Young,10500,0.544,1.0,0.7706,0.0586,0.0,11.0,1.22,0.303,1.11,100.0,3086.0,19.0`

   `P_TOP20` is a probability in 0–1 (display as a percentage to one decimal).
   `SALARY` is a DraftKings salary in whole dollars, always a multiple of 100.
   `LEVERAGE` is a signed number, model view minus Vegas view.
   `VEGAS_ODDS` is the numerator of fractional outright odds (11 → display `11/1`).
   `SG_CH_SHRUNK` is shrunk strokes-gained course history; exactly 0.0 means no history.
   `CUT_PERCENTAGE` is 0–100.
   Tournament name, date, and course are **not** in the CSV — they must be supplied.

2. **SQLite database** — ~20 MB, the owner's full historical stats DB. Contains at
   minimum a results table (per-event finishes and scores) and a predictions-tracking
   table (past model predictions, retained on a 2-year rolling window). Exact schema was
   not available at design time; **read the notebook's DB-writing cells to recover it**
   before implementing.

### Architecture: sql.js, no Python backend

The owner explicitly wants the Results Browser to explore the **full** database, not a
pre-exported slice. That rules out publishing JSON extracts. It does **not** require a
backend.

Load the whole `.db` file into **sql.js** (SQLite compiled to WebAssembly) and query it
in the browser. At 20 MB this is comfortable — the file loads into memory in well under
a second and queries return in single-digit milliseconds. The WASM binary is ~1.5 MB and
caches after first load.

**Constraint that drives the deployment shape:** a page opened via `file://` cannot
`fetch()` a local `.db` or `.csv` (browser security). Three ways out, in order of
preference:

1. **The notebook serves the app.** A final notebook cell starts `http.server` on a
   fixed port in a background thread and opens the browser at that URL. The owner runs
   the notebook end-to-end and the finished app opens by itself with this week's data
   already in it. This is the recommended target: it satisfies the owner's stated goal
   ("the notebook publishes everything this site needs to run") with no extra process to
   remember.
2. A standalone `run.sh` / `.bat` that starts a static server and opens the browser.
3. Drag-and-drop the `.db` and `.csv` onto the page. Works with zero setup but has to be
   redone every session. Worth building as a **fallback path** regardless, so the app is
   never dead if the server isn't running.

### What the notebook should publish

Into a single folder alongside the built static site:

- `data/slate.csv` — the weekly export, unchanged.
- `data/meta.json` — `{"tournament": "...", "date": "YYYY-MM-DD", "course": "..."}`.
  This is the missing context the CSV lacks. Cheap for the notebook to write since it
  already knows all three.
- `data/golf.db` — a copy (or hardlink) of the SQLite DB.

Idempotent: rerunning the notebook overwrites the folder, reloading the page picks
everything up.

### Important constraint on available stats

**The owner's stats are full-season aggregates, not per-event.** The only per-event
quantity in the DB is the **score**, from which strokes-gained per event can be derived.

Consequences for the player card:

- **SG by phase (OTT / APP / ARG / PUTT)** is a **season-level** stat. Render it as
  "SG by phase — <season>" and source it from the season stats table. Do not attempt a
  per-event phase breakdown; the data does not exist. (The owner's current Streamlit app
  labels it the same way, so this matches expectations.)
- **The SG-per-round scatter** should plot **per-event SG derived from score**, not per
  round. Retitle it accordingly ("SG per event"). The rolling-average line stays.
- **Recent results** (date, tournament, finish, score → SG) is fully real.
- **Course history** aggregates (events, avg finish, best, cut %) are fully real —
  compute by grouping the results table on course.

---

## Screens / Views

The app is **one screen** for the primary workflow. This was a deliberate response to the
owner's complaint about tab-switching in Streamlit: the field grid, the player card, and
the lineup builder are all visible simultaneously, and clicking a row updates the card in
place rather than navigating.

Root layout: `100vh`, `display:flex; flex-direction:column; overflow:hidden`.
Order: top bar (fixed height) → workspace (`flex:1`).

The workspace is a **three-column flex row** with `overflow-x:auto`:

| column | sizing |
|---|---|
| field grid | `flex:1 0 620px; min-width:620px` |
| player card | `flex:none; width:448px` |
| lineup rail | `flex:none; width:278px` |

Intrinsic minimum is 1346px. **This matters:** the row must scroll rather than let the
grid absorb the shortfall. An earlier version let the grid be the only shrinkable child
and it collapsed to 188px at a narrow viewport, hiding ten of twelve columns. At a normal
desktop width (~1400px and up) all three columns sit side by side with no scrolling.

### 1. Top bar

Height ~62px. `background:#0e1116`, `border-bottom:1px solid #232830`.
Single flex row, three children:

- **Brand block** — `flex:none; min-width:250px; padding:9px 18px; white-space:nowrap`.
  Two lines:
  - Line 1 is a flex row, `gap:10px`: `PGA SLATE TERMINAL` in IBM Plex Mono, 10px,
    weight 600, `letter-spacing:0.18em`, `#57d98a` — followed by the **A/B layout
    toggle**, two 22×16px squares (`radius:2px`, Mono 9.5px weight 600). Active is
    `background:#57d98a; color:#0b0d10`; inactive is `border:1px solid #2e343d;
    color:#8b929c` and links to the other layout. This only exists because two layouts
    ship in the prototype; **drop it in production** unless the compare board gets built.
  - Line 2, baseline-aligned, `gap:10px`: tournament name (IBM Plex Sans, 17px, 600,
    `letter-spacing:-0.01em`, `#e8eaed`), course (Mono 11px `#8b929c`), date
    (Mono 11px `#5f666f`).
- **Tab group** — `flex:1 1 auto; min-width:0; overflow-x:auto; padding:0 8px;
  border-left:1px solid #232830`. Tabs are `flex:none; padding:7px 13px;
  border-radius:4px; font-size:12px; weight 500; white-space:nowrap`.
  Active: `background:#1b2430; color:#e8eaed`. Inactive: transparent, `#8b929c`.
  Labels: `This Week`, `Prediction Tracker`, `Results Browser`.
- **Filter input** — `flex:none; padding:0 14px; border-left:1px solid #232830`.
  Input is `width:170px; background:#161a20; border:1px solid #2e343d; radius:4px;
  padding:7px 10px; font-size:12px; color:#e8eaed; outline:none`,
  placeholder `Filter player…`. Filters the grid on substring, case-insensitive.

**Layout warning:** this bar went through several failed iterations. The children's
intrinsic widths exceed narrow viewports, and whichever child is `flex:none` pushes the
others off-screen. The working arrangement is exactly the above: brand and filter are
`flex:none`, only the tab group flexes and scrolls. **Never add a fourth `flex:none`
child** — the A/B toggle was tried there and starved the tab group until two of three
tabs were unreachable, which is why it lives inside the brand block. An earlier version
also had a second status row (COURSE / FIELD / CAP / ROSTER / SAVED); it was removed as
low-value vertical cost and the course moved up next to the tournament name.

### 2. Field grid (left column)

Container: `flex:1 0 620px; min-width:620px; border-right:1px solid #232830;
overflow:auto`. Both the header row and the row list are `min-width:906px` so they scroll
together horizontally as one unit. The header is `position:sticky; top:0; z-index:2`.

Grid template, used identically by header and rows:

```
84px minmax(150px,1fr) 80px 112px 62px 54px 56px 60px 60px 54px 52px 62px
```

**Header row** — height 30px, `background:#12151a`,
`border-bottom:1px solid #2e343d`, IBM Plex Mono 10px, `letter-spacing:0.09em`,
`#8b929c`. Every column except the action column is click-to-sort; the active column
appends ` ▼` or ` ▲` in `#57d98a`. Labels in order:
`＋ L X` · `PLAYER` · `SALARY` · `P(TOP-20)` · `VAL` · `LEV` · `ODDS` ·
`SG:F` · `SG:C` · `CUT` · `OWGR` · `EXP`.
All numeric headers are right-aligned with `padding-right:10px`.

There is deliberately **no rank column** — the owner removed it to reclaim horizontal
space, on the grounds that P(top-20) is the ranking. Model rank still appears in the
player card header and in the lineup slots.

**Data rows** — height 34px, `border-bottom:1px solid #171b21`, IBM Plex Mono 12px,
`cursor:pointer`. Cell contents left to right:

- **Actions** — flex row, `gap:3px`, `padding-left:8px`. Three 20×20px buttons,
  `border:1px solid #2e343d; border-radius:3px`, centered glyph.
  - `＋` (12px) — add/remove from current lineup. Active: `background:#57d98a;
    color:#0b0d10`. Inactive: transparent, `#6f7681`.
  - `L` (10px, weight 600) — lock. Active green, same treatment.
  - `X` (10px, weight 600) — exclude. Active: `background:#e0655c; color:#0b0d10`.
  All three `stopPropagation` so they don't also select the row.
- **Player** — IBM Plex **Sans** 13px weight 500, `padding-left:10px`, ellipsis on
  overflow. `#e8eaed` normally, `#6f7681` when excluded.
- **Salary** — `$10,500` format, right-aligned, `#c8ccd2`.
- **P(top-20)** — right-aligned value to one decimal (`54.4`), weight 500,
  colored by `p20Color()`: green at ≥80th field percentile, then the `tier()`
  ramp. **The progress bar was removed (Aug 2026)** and the column narrowed
  112px → 62px. Scaled to the field's best, the bar filled most of the column for
  most of the field, making a green wall that was the loudest thing on the screen
  and said only what the digits 6px away already said. Horizontal space is the
  scarce resource here.
- **VAL** — `P_TOP20 × 100 / (SALARY / 1000)`, two decimals. This is a **derived
  column that did not exist in the Streamlit app**, added because the owner said
  salary-vs-probability is the axis they actually optimize on. Weight 500.
  `#57d98a` ≥85th pct, `#e0655c` ≤15th, else `#c8ccd2`.
- **LEV** — signed, one decimal. `#6aa9f0` if ≥+2, `#e6b053` if ≤−2, else `#7d848d`.
- **ODDS** — integer, `#8b929c`.
- **SG:F** (`SG_FORM`) — signed, two decimals. `#9fd8b4` positive, `#d09a95` negative.
- **SG:C** (`SG_CH_SHRUNK`) — same, but exactly 0 renders in `#4c525a` to visually
  mark "no course history" rather than "neutral course history". This distinction
  matters to the owner.
- **CUT** — integer percent, `#8b929c`.
- **OWGR** — integer, `#5f666f`.
- **EXP** — exposure across saved lineups, integer percent, or `—` when nothing is
  saved. `#e6b053` at ≥60%, `#c8ccd2` above zero, `#3d444f` at zero.

**Row states** (revised Aug 2026). Background carries the **committed** state,
the inset 2px left edge carries the **transient** one, so the two compose instead
of overwriting each other:

| | background | edge |
|---|---|---|
| in current lineup | `lineup-bg` | `blue` |
| excluded | `exclude-bg` | — |
| focused (the card you are reading) | `select-bg` if not in lineup | `focus-edge` |

A row that is both in the lineup and focused is therefore blue with a light edge —
which is what it is. Under the old rule focus *replaced* lineup shading, so the
player you were reading about dropped out of the lineup group while you read
about him. Background precedence: lineup → excluded → focused.

### 3. Player card (center column)

`width:448px; flex:none; background:#0b0d10; overflow-y:auto`. When no player is
selected, a centered `#5f666f` 12.5px message: "Select a player in the grid to load
their detail card."

**Revised Aug 2026 — cards, not rules.** Section (a) is **pinned**
(`position:sticky; top:0; z-index:2`), full-bleed on `panel` with
`padding:14px 16px` and a `line` bottom border: who this is and the three actions
you take on him stay reachable at any scroll depth. Sections (b)–(h) sit in a
`padding:10px; display:flex; flex-direction:column; gap:10px` stack, each its own card —
`background:panel; border:1px solid line; border-radius:6px; padding:12px 14px 14px`.
The 10px of app background between cards is the divider; the old hairline rules at
14px spacing let eight stacked sections read as one continuous list. Inner bar tracks
stay on `surface`, which is why the column background dropped to `bg` — cards need to
be lighter than what is behind them and darker than what is inside them.

Every section heading is IBM Plex Mono 10px, weight 600, `letter-spacing:0.14em`,
`#8b929c`, uppercase. (The Prediction Tracker still uses the original flush,
hairline-separated variant — `<Section>` without the `card` prop.)

**a. Header + stat cards.** Player name (Sans 19px, 600, `-0.01em`) on the left; on the
right, Mono 11px `#5f666f` reading `RANK 1 / 149`.
Below, a four-card row: `display:grid; grid-template-columns:repeat(4,1fr); gap:1px;
background:#232830; border:1px solid #232830; border-radius:5px; overflow:hidden` —
the 1px gap over a `#232830` background is what draws the dividers. Each card is
`background:#12151a; padding:8px 10px`, with a Mono 9px `letter-spacing:0.1em` `#5f666f`
label and a Mono 19px weight-600 value. Cards:

Each tile takes the **same color its column takes in the grid, from the same
helper** — clicking a row must never change what a color means:

| label | value | color |
|---|---|---|
| `P(TOP-20)` | `54.4%` | `p20Color()` — as the grid column |
| `SALARY` | `$10,500` | flat `text` |
| `VAL /$1K` | `5.18` | `valColor()` — as the grid column |
| `VEGAS ODDS` | `11/1` | `tier()` — as the grid column |

Odds got promoted to a stat card at the owner's request — they consider it a
front-of-mind number. It replaced Leverage, which still appears in the grid's LEV column
and in the percentile table. Odds is in blue specifically so it reads as a market signal
distinct from the green model numbers.

*Do not put two values in one card.* An earlier version combined leverage and odds as
`+0.0 / 11`; at 19px mono that string needs ~78px in a 69px content box and wrapped to
two lines, inflating all four cards.

Then a three-button row, `gap:6px`, each `flex:1; padding:8px; radius:4px;
border:1px solid #2e343d; font-size:12px; weight 500; text-align:center`:
`Add to lineup` / `In lineup` (toggles, green when active), `Lock`, `Exclude`.

**b. FLAGS.** Right-aligned sub-label: `vs <N> field players`.
A vertical list, `gap:7px`. Each row: a 7px circle (`margin-top:5px`, `flex:none`) then
12.5px `#d5d9df` text at `line-height:1.35`.
This is the feature the owner singled out as most valuable — see Flag Engine below.

**c. FORM PROFILE.** `grid-template-columns:repeat(5,1fr); gap:8px`. Each cell has a
Mono 9px `#5f666f` label and a Mono 16px weight-600 value, `white-space:nowrap`.
Cells: `SG FORM`, `RNDS 12M`, `CUTS /20`, `STREAK`, `TOP-20 /20`.

**d. SG BY PHASE — SEASON.** Four rows, `gap:6px`, each
`grid-template-columns:86px 1fr 96px`. Left is a Mono 11px `#8b929c` `nowrap` label:
`Driving`, `Approach`, `Around green`, `Putting` — the label column is 86px specifically
so "Around green" stays on one line and all four rows are evenly spaced. Center is a
16px-tall `background:#12151a; radius:3px` track with a 1px `#3d444f` zero line at
`left:38%`; the bar is absolutely positioned (`top:3px; bottom:3px; radius:2px`), growing
right from the zero line for positive values and left for negative, clamped at ±1.4
strokes. Right is Mono 11px: signed value in the bar color, then the field rank in
`#5f666f`. Positive `#57d98a`, negative `#e0655c`.

**e. SG PER ROUND — 24 MO** (retitle to "SG PER EVENT" per the data constraint above).
Right sub-label `rolling form ——`. An inline SVG, `viewBox="0 0 416 152"`,
`width:100%; height:auto`. Plot area x from 36 to 410, y from 16 to 118, with y=67 as
the zero line. Scale clamps at ±5.6 strokes over ±51px.
- Vertical axis line at x=36, `#232830`.
- Dashed zero line, `#3d444f`, `stroke-dasharray:3 3`.
- Y labels at x=30, `text-anchor:end`, Mono 9px `#5f666f`: `+5`, `0`, `−5`.
- Data points: `r=2.1` circles, `fill:#5b7fb8`, `opacity:0.75`.
- Rolling mean: a `polyline`, `fill:none; stroke:#e0806c; stroke-width:1.8;
  stroke-linejoin:round`. Window is the trailing 12 observations.
- Five x-axis ticks at y=134, `text-anchor:middle`, Mono 9px `#5f666f`,
  formatted `Aug '25`.

**f. PERCENTILE vs FIELD.** Rows of `grid-template-columns:96px 58px 1fr 44px`, height
20px, `gap:8px`. Mono 11px `#8b929c` label, Mono 12px `#e8eaed` right-aligned value, a
5px percentile bar (`background:#12151a`, fill width = percentile), and the field rank
as `#12` in Mono 10px `#5f666f`. Bar color `#57d98a` ≥80th, `#7d848d` ≥45th,
`#4c525a` below. Metrics: `P(top-20)`, `Value/$1k`, `SG form`, `SG course`, `Cut %`,
`Leverage`, `Salary`.

**g. COURSE HISTORY.** Sub-label `◆ = this week`.
Header and rows share `grid-template-columns:1fr 32px 44px 40px 44px; gap:0 6px`.
Header is Mono 9px `letter-spacing:0.06em` `#5f666f`: `COURSE`, `EV`, `AVG`, `BEST`,
`CUT%`. Rows are height 23px, `border-top:1px solid #171b21`, Mono 11.5px.
This week's course is sorted to the top, prefixed `◆ `, `background:#1b2430`,
name in `#57d98a`. Show 7 rows.

**h. RECENT RESULTS.** `max-height:238px; overflow-y:auto`. Rows are
`grid-template-columns:74px 1fr 46px 50px`, height 24px,
`border-top:1px solid #171b21`. Date in Mono 11.5px `#5f666f` (`Jul 19 '26`),
tournament in **Sans** 12px `#c8ccd2` with ellipsis, event SG signed one decimal
(`#9fd8b4` / `#d09a95`), finish right-aligned weight 600 — `T26` format, bare `1` for a
win, `CUT` in `#6f7681`. Finish color: `#57d98a` top 10, `#c8ccd2` top 20,
`#8b929c` beyond.

### 4. Lineup rail (right column)

`flex:none; width:278px; display:flex; flex-direction:column; min-height:0;
overflow-y:auto; border-left:1px solid #232830; background:#0e1116`.

This started as a full-width dock across the bottom and was moved to a right rail at the
owner's request — they found the vertical treatment clearer, particularly for saved
lineups as full cards rather than horizontal chips.

**The rail must scroll as a whole column** (`overflow-y:auto` above) **and the saved
list needs a floor.** Its five fixed-height children sum to ~470px, which at a short
viewport left the flexible saved-lineups container 10px to render several hundred px of
content. The saved list is therefore `flex:1 0 auto; min-height:126px`.

Children top to bottom:

1. **Header** — `padding:12px 14px 9px`, baseline-spaced: `LINEUP` (Mono 10px, 600,
   `letter-spacing:0.14em`, `#8b929c`) and the roster spec `6 × $50,000`
   (Mono 10px `#5f666f`).
2. **Slots** — `padding:0 10px`, one row per roster spot. Each is
   `display:flex; align-items:center; gap:9px; height:34px; padding:0 9px;
   margin-bottom:2px; border-radius:3px; border-left:2px solid <accent>`.
   Filled: `background:#12151a`, accent `#57d98a` when locked else `#2e343d`;
   name (Sans 12px, 500, ellipsis), salary (Mono 11px `#8b929c`), and a 34px
   right-aligned P(top-20) (Mono 11px `#57d98a`).
   Empty: `background:#101317`, accent `#1a1e24`, reads `Empty` / `—` in `#3d444f`.
   Clicking a filled slot removes that player.
3. **Totals** — `margin:10px 10px 0; padding:10px 12px; border-radius:4px;
   background:#12151a; display:grid; grid-template-columns:1fr 1fr; gap:5px 10px`,
   all Mono. Four labels (9px, `letter-spacing:0.1em`, `#5f666f`) over four values
   (15px, weight 600): `SALARY` / `REMAINING`, then `Σ P(TOP-20)` / `AVG LEFT`.
   Remaining turns `#e0655c` when negative and `#57d98a` when the roster is full and
   legal. `AVG LEFT` is remaining salary divided by empty slots, floored to the nearest
   100 — the number the owner uses to judge whether a build is still viable.
4. **Buttons** — `padding:10px; flex-direction:column; gap:5px`. `Optimize` is a
   full-width primary: `background:#57d98a; color:#0b0d10; padding:9px; radius:4px;
   font-size:12px; weight 600`. Below it a row of three secondaries
   (`border:1px solid #2e343d; color:#c8ccd2; font-size:11.5px; padding:7px`):
   `Gen <N>`, `Save`, and a 30px `✕` (clear).
5. **SAVED header** — `padding:7px 14px; border-top:1px solid #232830`, baseline-spaced:
   `SAVED 5` (Mono 10px, 600, `0.14em`, `#8b929c`) and, when empty, the hint
   `none yet — Optimize, then Save` in `#3d444f`.
6. **Saved lineup cards** — `flex:1 0 auto; min-height:126px; padding:0 10px 10px`.
   Each card is `padding:8px 10px; margin-top:6px; border-radius:4px;
   border:1px solid #2e343d; background:#12151a`. Top line (Mono 11px, baseline-spaced):
   `L1` in `#8b929c` on the left; on the right a `gap:9px` group of total salary
   (`#c8ccd2`), total P(top-20) (`#57d98a`), and a `✕` delete (`#5f666f`,
   `stopPropagation`). Below, the full player names joined by ` · ` in 11px `#6f7681`
   at `line-height:1.45`, wrapping freely. The card matching the current build gets
   `border:#57d98a; background:#13201a`. Clicking a card loads it into the slots.

### 5. Prediction Tracker (not built)

Purpose: judge model calibration over a **2-year rolling window**, from the existing
predictions-tracking table in the DB. The prototype stubs this with the shape of the
query it needs:

```sql
SELECT event_date, tournament, player, p_top20_pred,
       finish_pos, (finish_pos <= 20) AS hit
FROM predictions
JOIN results USING (event_id, player_id)
ORDER BY event_date DESC;
```

Intended content: a calibration curve (predicted probability decile vs realized top-20
rate, with the 45° reference line), an overall Brier score, and a hit-rate-by-decile
table. The owner reads numbers faster than charts, so lead with the table and treat the
curve as support. Reuse the grid's visual language rather than inventing a new one.

### 6. Results Browser (not built)

Purpose: **free exploration of the full database**, not a fixed report. The owner
explicitly wants to query everything, which is why the app loads the whole DB rather
than a pre-exported slice.

Build it as the same grid engine pointed at arbitrary result sets: a query area (either
a raw SQL box or faceted filters on player / tournament / course / date range), then the
standard sortable dense table below. Page results above a few thousand rows.

---

## Interactions & Behavior

- **Row click** → selects that player, loads the card. No navigation, no scroll jump.
- **`＋` / Add to lineup** → toggles membership in the current build. Adding is a no-op
  when the roster is already full; there is no error state, the button simply doesn't
  act.
- **`L` / Lock** → forces the player into every optimizer solve. Independent of the
  current build.
- **`X` / Exclude** → removes the player from every solve. A player can be both in the
  current build and excluded; the build wins for the current build, the exclusion applies
  to future solves.
- **Column header click** → sorts; clicking the active column reverses direction.
  Initial direction is ascending for `PLAYER` and `OWGR_RANK`, descending for everything
  else. Default sort is `P_TOP20` descending.
- **Filter input** → case-insensitive substring match on player name, applied before sort.
- **Optimize** → fills the current build to a full roster, keeping locked players and
  anything already manually picked, maximizing the objective under the salary cap.
- **Gen N** → appends N distinct optimal lineups to the saved set (see below). Exposed
  as a native `title` tooltip on the button and on the SAVED label, spelling out every
  rule, because the constraint set is not guessable from a three-word button.
- **Save** → saves the current build if it is a full roster and not already saved.
- **Card click** → loads that lineup. **Card ✕** → deletes it.
- **Persistence** → locks, excludes, the current build, and all saved lineups are written
  to `localStorage` on every mutation, under a key derived from tournament + date + field
  size. A new week's CSV produces a new key, so last week's lineups disappear
  automatically. The owner asked for exactly this behavior.

No animations or transitions anywhere. This is a data tool; motion would be noise.

---

## The Optimizer

Runs entirely client-side. **No solver library, no backend, no MILP dependency.**

DraftKings golf is a pure 0/1 knapsack with a cardinality constraint: choose exactly
6 players, total salary ≤ $50,000, maximize the summed objective. Salaries are always
multiples of 100, so the salary axis discretizes to 500 buckets. An exact dynamic program
over (slots × salary buckets) is ~500k operations — under a millisecond. It returns the
**provably optimal** lineup, not a greedy approximation.

```
buckets B = floor(remainingCap / 100)
dp[slots][bucket] = best objective, initialized -1, dp[0][0] = 0
choice[slots][bucket] = index of the player chosen at that state

for each player p in pool:                  # outer loop = 0/1, no reuse
  for s = remainingSlots down to 1:
    for b = B down to weight(p):            # descending = each player used once
      prev = dp[s-1][b - weight(p)]
      if prev >= 0 and prev + value(p) > dp[s][b]:
        dp[s][b] = prev + value(p)
        choice[s][b] = p

answer = argmax over b of dp[remainingSlots][b]
reconstruct by walking choice[] backwards
```

- `weight(p) = SALARY / 100`
- `value(p) = P_TOP20` (or `MODEL_SCORE` when the objective is switched)
- **pool** excludes: manually excluded players, locked players, and players already in
  the build — those are pre-committed, their salary is subtracted from the cap and their
  slots from the count before the DP runs.

**Gen N** wraps the DP to produce N *distinct* lineups:

1. Compute a usage count per player across all currently saved lineups.
2. Compute a hard ceiling: `max(1, floor(maxExposure% × (savedCount + N)))`.
3. Before each solve, ban every player whose usage has reached the ceiling — **except
   locked players**, which are exempt by definition.
4. Apply a small objective penalty of `0.006 × usage` plus seeded jitter, so successive
   solves diverge instead of returning near-identical lineups.
5. Reject any lineup whose player set already exists and re-solve. Guard at `8 × N`
   attempts to avoid a loop when constraints are over-tight.
6. Append results to the saved set; never replace it.

The hard ceiling matters — an earlier version used only the soft penalty, which does not
actually bound exposure and quietly lets one player appear in every lineup.

---

## State Management

All client-side; nothing is server-owned.

**Loaded once:** `players[]` (CSV rows, enriched), `meta` (tournament / date / course),
`pct` and `rnk` lookup maps (see Flag Engine), plus the sql.js database handle.

**Session state:**
- `tab` — `slate` | `tracker` | `results`
- `query` — filter string
- `sortKey`, `sortDir`
- `selected` — player id shown in the card

**Persisted to localStorage** (key = `pgaslate:v1:<tournament>:<date>:<fieldSize>`):
- `locks` — id → true
- `excludes` — id → true
- `picks` — ordered ids in the current build
- `saved` — array of `{ id, ids[] }`

**Derived every render:** exposure counts, filtered+sorted rows, lineup totals, the
selected player's flags / percentiles / history. All cheap at this scale; memoize the
per-player history if profiling says to, nothing else.

**Enrichment at load:** compute `VAL = P_TOP20 × 100 / (SALARY / 1000)` and assign each
player a stable model `rank` from descending `P_TOP20` before anything else. Then, for
each of `P_TOP20`, `VAL`, `SG_FORM`, `SG_CH_SHRUNK`, `CUT_PERCENTAGE`, `LEVERAGE`,
`SALARY` and the four SG phases, build `rnk[metric][playerId]` (1-based, best first) and
`pct[metric][playerId]` (0–1, 1 = best) by sorting the field once per metric. This is
what makes the flags and percentile bars instant.

---

## Flag Engine

The owner called this the single most valuable feature — it is an automatic scan for
where a player is an **outlier against the current field**, so two players can be
compared without reading every number. Port the thresholds as-is; they are tuned.

Three severities by dot color: `#57d98a` positive, `#e6b053` caution,
`#e0655c` negative, `#6aa9f0` informational.

| Condition | Severity | Message |
|---|---|---|
| `P_TOP20` pct ≥ 0.92 | green | `Model love: 54.4% top-20 — rank 1 of 149` |
| `VAL` pct ≥ 0.88 | green | `Value play: 5.18 P20%/$1k — rank 12 of 149` |
| `VAL` pct ≤ 0.12 | red | `Poor value: 1.92 P20%/$1k — rank 141 of 149` |
| `SG_FORM` pct ≥ 0.85 | green | `Hot form: SG +1.22/rd — top 6% of field` |
| `SG_FORM` pct ≤ 0.15 | red | `Cold form: SG −0.41/rd — rank 132 of 149` |
| `CUT_PERCENTAGE` ≥ 95 | green | `Steady: 100% cuts made` |
| `CUT_PERCENTAGE` ≤ 62 | amber | `Volatile: 58% cuts made` |
| `SG_CH_SHRUNK` ≥ 0.8 | green | `Course fit +1.11 at Detroit Golf Club` |
| `SG_CH_SHRUNK` ≤ −0.5 | red | `Course history −0.78 — poor fit` |
| `SG_CH_SHRUNK` == 0 | amber | `No course history at Detroit Golf Club` |
| phase pct ≥ 0.90 | green | `Strong driving — top 6% (+0.55, rank 12)` |
| phase pct ≤ 0.10 | red | `Weak putting — bottom 8% (−0.42, rank 137)` |
| `LEVERAGE` ≥ 3 | blue | `Leverage +3.5 — model ahead of Vegas` |
| `LEVERAGE` ≤ −3 | amber | `Leverage −3.5 — Vegas ahead of model` |
| P20 rank − salary rank ≤ −18 | green | `Underpriced: P20 rank 8 vs salary rank 31` |
| P20 rank − salary rank ≥ 18 | red | `Overpriced: P20 rank 44 vs salary rank 12` |
| `OWGR_RANK` ≥ 150 | amber | `Longshot: OWGR 178` |
| none of the above | grey | `No outliers vs field on any tracked metric.` |

Phase flags run over all four of OTT / APP / ARG / PUTT. Every message embeds the actual
value **and** the field rank — the owner wants to see the number, not just the verdict.

**Consistency rule:** any two numbers describing the same quantity must be computed from
the same source. An early build showed `Steady: 100% cuts made` (from the real CSV
column) directly above `CUTS /20 95%` (from an independent synthetic draw), which reads
as the app contradicting itself. When both a season aggregate and a last-20 figure are
shown, derive the last-20 from the aggregate or label the window unmistakably.

---

## Design Tokens

> **Palette revised Aug 2026** (branch `dashboard-restyle`). `src/tokens.ts` is the
> authority; the hexes below are that file's current values, with the original
> handoff values in the last column. The rest of this document still quotes the
> original hexes inline — read those as *token references*, not as literal colors.
>
> **Four hues, one job each. A change that needs a fifth is the wrong change.**
>
> 1. **Green = good, red = bad.** Exactly one of each, at one weight, in every
>    context — grid cell, card number, bar, chart mark, dot. There is no "soft"
>    variant; two greens made the eye ask whether the difference meant something.
>    Only values with a **direction** earn a hue: `+0.42` SG is good, `11/1` odds
>    and `0.83` volatility are merely large.
> 2. **Blue = the lineup you are building**, and the controls that act on it —
>    in-lineup row shading, `＋`, the rail's active saved card, `Optimize`, form
>    controls. Blue never means good and never encodes a value. `L` stays green
>    and `X` stays red, because those two *are* verdicts on a player.
> 3. **Amber = caution about the app's own state** — over-exposure, degraded
>    sync, truncated results. Never a data value; a bad number is red.
> 4. **No hue = tier by lightness**: `text → text2 → muted → dim → dimmer`
>    (`tier()` in tokens.ts), for anything with an ordering but no verdict —
>    odds, OWGR, cut rate. Salary is the exception that proves it: it is
>    deliberately flat, because there is no good end of the salary range.
> 5. **Focus is grey.** The row you are viewing gets a neutral wash plus a light
>    edge, never a hue — it changes on every click and must not compete with the
>    committed state (lineup) underneath it.
>
> Every value-to-color decision lives in `tokens.ts` as a helper (`tier`,
> `dirColor`, `p20Color`, `valColor`) rather than inline at the call site, so a
> metric's grid column and its card tile cannot drift apart.

**Colors**

| Token | Hex | Use | Was |
|---|---|---|---|
| bg | `#0b0d10` | app background, scrollbar track, player-card column | |
| panel | `#0e1116` | top bar, cards, lineup rail | |
| surface | `#12151a` | grid header, stat cards, bar tracks, slots |
| surface-alt | `#161a20` | input fields |
| slot-empty | `#101317` | empty lineup slots |
| track | `#1e232a` | progress bar tracks |
| line-soft | `#171b21` | row dividers |
| line | `#232830` | section and panel borders |
| line-strong | `#2e343d` | button borders, header underline, scrollbar thumb |
| axis | `#3d444f` | chart zero lines, empty-state text |
| text | `#e8eaed` | primary |
| text-2 | `#c8ccd2` | numeric secondary |
| muted | `#8b929c` | labels |
| dim | `#5f666f` | tertiary labels, ranks, neutral (directionless) bars | |
| dimmer | `#4c525a` | footnotes, null-ish values | |
| green | `#6fae8a` | **good** — the only green | `#57d98a` |
| red | `#c9736b` | **bad** — the only red | `#e0655c` |
| amber | `#cfa059` | caution about app state | `#e6b053` |
| blue | `#6f9fd8` | lineup membership, primary action, active control | `#6aa9f0` |
| select-bg | `#1a1e25` | focused row, active tab, pinned course row | `#1b2430` |
| focus-edge | `#c8ccd2` | 2px inset edge on the focused row | *new* |
| lineup-bg | `#16202e` | in-lineup row, active saved card | `#13201a` |
| exclude-bg | `#181113` | excluded row | `#1a1113` |
| scatter | `#8b929c` | chart data points (= muted) | `#5b7fb8` |
| trend | `#c8ccd2` | chart rolling-average line (= text-2) | `#e0806c` |

`green-soft` (`#9fd8b4`) and `red-soft` (`#d09a95`) **were removed** — dense-row
numerics use the one green and the one red. Chart marks carry no hue: the rounds
scatter is the dim ramp and the rolling-form line is brighter only because it is
the summary you are meant to read, not a better kind of value.

**Typography** — two families, both Google Fonts.
`IBM Plex Sans` (400/500/600/700) for names, headings, and prose.
`IBM Plex Mono` (400/500/600) for **every number** and every uppercase micro-label.
That split is the core of the look: tabular numerals align down dense columns, and mono
labels read as instrumentation rather than web UI. Do not substitute Inter or Roboto.

Scale in use: 9, 10, 11, 11.5, 12, 12.5, 13, 15, 16, 17, 19 px.
Micro-labels carry `letter-spacing` from `0.06em` to `0.18em`; large headings use
`-0.01em`.

**Spacing** — 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 20 px.
Section padding is `14px 16px`. Grid gutters are `padding-right:10px`.

**Radii** — 2px (bars), 3px (small buttons, tracks, lineup slots), 4px (buttons, inputs,
cards), 5px (stat card group), 50% (flag dots).

**Elevation** — none. No box-shadows except the 2px inset row-state edges. Depth comes
from surface value steps, not shadow.

**Fixed dimensions** — grid column floor 620px with 906px of content; player card 448px;
lineup rail 278px; saved-list floor 126px; row height 34px; grid header 30px; lineup slot
height 34px.

**Custom scrollbars** — 10px, `#0b0d10` track, `#2e343d` thumb with a 5px radius and a
2px track-colored border, `#3d444f` on hover.

---

## Adding New Screens Later

The owner will want more screens than exist here (the two stub tabs at minimum, probably
more after that). To keep them consistent without redesigning each one, **build a
primitives layer first and compose every new screen from it.** Concretely:

- `tokens.ts` — the color table above as named constants, plus the type scale. Nothing
  should hardcode a hex after this file exists.
- `<DataGrid>` — the field grid generalized: column defs (key, label, width, align,
  formatter, color function), click-to-sort with the ` ▼`/` ▲` affordance, sticky header,
  34px rows, `min-width` + shared horizontal scroll. **The Results Browser is this
  component with different column defs**, not a new table.
- `<Section>` — the `padding:14px 16px` + `border-bottom:1px solid #232830` block with
  the Mono 10px `0.14em` uppercase heading and optional right-aligned sub-label.
- `<StatCard>` / `<StatCardRow>` — the 1px-gap grid that draws its own dividers.
- `<PercentileBar>` — value + bar + rank, with the three-stop color logic.
- `<Flag>` — dot + text, severity-colored.
- `<Rail>` — the scrolling right column shell.

Rules any new screen must follow, in priority order:

1. **Every number is IBM Plex Mono.** Every label is uppercase Mono at 9–10px with
   letter-spacing. Only names, headings, and sentences are Sans.
2. **No new accent colors.** Green = good/model, red = bad, amber = caution, blue =
   market/selection. If a new screen seems to need a fifth color, it probably needs a
   different encoding instead.
3. **Context, not bare values.** Wherever a number appears, show its rank or percentile
   against the relevant population. That principle is what makes the player card work.
4. **Tables before charts.** The owner reads numbers faster. Charts are support.
5. **No shadows, no motion, no rounded-card-with-left-accent-stripe patterns** beyond the
   2px state edges already in use.
6. **Density target:** ~20 rows visible without scrolling; 34px row height; 12px numerics.

## Assets

None. No images, no icon font, no SVG illustrations. The only glyphs are Unicode
characters set in the body font: `＋` (fullwidth plus, U+FF0B), `✕` (U+2715),
`◆` (U+25C6), `▼` `▲` (U+25BC / U+25B2), `·` (U+00B7), `—` (em dash), `−` (U+2212,
minus sign — used in the chart's axis label, distinct from a hyphen).

Fonts load from Google Fonts. For a genuinely offline local tool, self-host the two
IBM Plex families instead.

## Files

- `PGA Slate Terminal.dc.html` — **layout A, the design to build.** Contains the full
  template and the logic class (optimizer, flag engine, percentile computation, history
  synthesis, chart geometry). Read the logic class for exact algorithm details; read the
  template for exact markup and styling.
- `PGA Slate Board.dc.html` — layout B, the rejected alternative. Reference only, for its
  compare-board concept. Its palette and fonts are not the target.
- `slate-data.js` — the real weekly export as `window.SLATE_ROWS`, 149 players from the
  Rocket Classic, plus `window.SLATE_META`. Use it as fixture data while building.

## Suggested Build Order

1. Static shell + design tokens + the two fonts + the primitives listed above.
2. CSV ingest and the enrichment/percentile pass. Ship the grid with real data.
3. Player card sections a–d and f (all computable from the CSV alone).
4. Optimizer and lineup rail, with localStorage persistence. At this point the tool is
   already more capable than the Streamlit app plus Excel.
5. sql.js integration and the notebook's publish-and-serve cell.
6. Player card sections e, g, h against real queries — retitling the scatter to per-event.
7. Prediction Tracker.
8. Results Browser.
