# Handoff: PGA Slate Terminal

> **Written as a handoff, kept as the spec.** It is maintained: every section describes
> what the app does now, and changes since the original build are marked in place with
> their reasoning. Two things to read in past tense — the Streamlit app and the Excel
> optimizer it talks about replacing are both gone (August 2026; `app.py` lives on in git
> history), and the notebook's `slate.js` export has replaced reading the CSV directly.
> Where the source comments say a threshold was "ported from Streamlit", that is
> provenance for the number, not a file you can still open.

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
overflow:auto`. Both the header row and the row list are `min-width:832px` so they scroll
together horizontally as one unit. The header is `position:sticky; top:0; z-index:2`.

Grid template, used identically by header and rows:

```
84px minmax(150px,1fr) 80px 112px 62px 54px 56px 60px 60px 54px 52px 62px
```

**Header row** — height 30px, `background:#12151a`,
`border-bottom:1px solid #2e343d`, IBM Plex Mono 10px, `letter-spacing:0.09em`,
`#8b929c`. Every column except the action column is click-to-sort; the active column
appends ` ▼` or ` ▲` in `#57d98a`. Labels in order:
*(blank)* · `PLAYER` · `SALARY` · `P(TOP-20)` · `VAL` · `LEV` · `ODDS` ·
`SG:F` · `SG:C` · `CUT9M` · `OWGR` · `EXP`.
All numeric headers are right-aligned with `padding-right:10px`.

**The action column's `L X` header was removed (Aug 2026).** It repeated, in the same
two glyphs and the same order, the two buttons sitting directly beneath it in every row —
a heading that can only restate its own column's contents is a heading worth deleting.

**The freed header cell now holds `CLR`**, which drops every lock and every exclusion at
once — the only control on the grid that acts on all rows rather than one, which is what a
column header is for. It is **invisible until there is something to clear**: a permanent
button that is a no-op on most page loads is a control you read and dismiss every time you
look at the grid, and appearing only once a constraint exists makes its presence the
status report as well as the action. It **arms on first press** (`CLR` → amber `CLR 5?`,
disarming after 3s), the same shape the rail's saved-lineup `CLEAR ALL` uses, because it is
more destructive than its name suggests — locks hold lineup slots now, so a hand-built
roster is entirely locks and one press would empty it. The `title` states the consequence
in full, including how many players will leave the lineup.

There is deliberately **no rank column** — the owner removed it to reclaim horizontal
space, on the grounds that P(top-20) is the ranking. Model rank still appears in the
player card header and in the lineup slots.

**Data rows** — height 34px, `border-bottom:1px solid #171b21`, IBM Plex Mono 12px,
`cursor:pointer`. Cell contents left to right:

- **Actions** — flex row, `gap:3px`, `padding-left:8px`. Two 20×20px buttons,
  `border:1px solid #2e343d; border-radius:3px`, centered glyph. **No tooltips** — the
  owner declined them; `L` and `X` are self-evident once known, and a hover card on every
  row of a dense grid is noise. The rail's buttons keep theirs, where the rules being
  described are not guessable.
  - `L` (10px, weight 600) — lock. Active: `background:#57d98a; color:#0b0d10`.
    Inactive: transparent, `#6f7681`.
  - `X` (10px, weight 600) — exclude. Active: `background:#e0655c; color:#0b0d10`.
  Both `stopPropagation` so they don't also select the row.

  **The `＋` (add to lineup) button was removed (Aug 2026)** and the column narrowed
  84px → 60px. Optimize now rebuilds around locks rather than filling in around the
  current build, so "picked but not locked" is no longer a state any solve preserves —
  `＋` and `L` had become two ways to say the same thing, and `L` is the one that
  survives a re-optimize.

  **`L` then absorbed `＋`'s job outright (Aug 2026), by re-solving.** Pressing it sets the
  constraint *and* rebuilds the lineup around it in one step, so the grid is once again
  where a lineup gets hand-built — but without `＋`'s defect, since a locked pick is one
  every later solve keeps. See *Interactions* for the model and for what happens when the
  constraints cannot be satisfied.
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

**That row was replaced by read-only state chips (Aug 2026).** `Lock` and `Exclude` were
the grid's `L` and `X` a second time over, and `Add to lineup` was worse than redundant:
on an empty roster it filled a slot the next Optimize threw away, and on a full one it
did nothing. Every action now lives in exactly one place — the row — and the card is the
read-out. In its place, up to three outlined chips beside `RANK`: `IN LINEUP` (blue),
`LOCKED` (green), `EXCLUDED` (red). Outlined, not filled, because a chip you cannot press
must not look like the solid buttons on the rail.

**b. FLAGS.** Right-aligned sub-label: `vs <N> field players`.
A vertical list, `gap:7px`. Each row: a 7px circle (`margin-top:5px`, `flex:none`) then
12.5px `#d5d9df` text at `line-height:1.35`.
This is the feature the owner singled out as most valuable — see Flag Engine below.

**A `?` reference panel was added to the sub-label (Aug 2026)**, because this is the one
panel on the card that reaches a *verdict* instead of reporting a number, and a verdict
you cannot audit is one you either trust blindly or ignore. A 14px outlined `?` beside the
sub-label opens a 384px panel (`background:surface`, `border:1px lineStrong`, `radius:6px`,
`max-height:58vh`, `overflow-y:auto`, `z-index:10`, anchored `top:100%; right:0`) listing
every rule grouped by category, each with its severity dot and its trigger. Its header row
is `position:sticky` so the title and the × stay reachable at the bottom of a long scroll.

**Click to open; it does not close on mouse-out.** It was built as a hover flyout first,
and that was wrong for the one thing it is actually for: the content is longer than the
panel, so it has to be *scrolled*, and a panel that vanishes when the pointer leaves it is
a panel you cannot comfortably reach the scrollbar of. Reference material you read is not a
tooltip you glance at. Dismissed by the ×, by the `?` again, or by Escape — and
deliberately **not** by clicking elsewhere: there is nothing player-specific in it, so
leaving it up while you click through players in the grid is a legitimate way to use it,
and an outside-click handler would take it away mid-comparison every time.

**Its content is generated from `FLAG_GUIDE` in `src/flags.ts`, whose every number is
interpolated from the `THRESHOLDS` object the engine itself reads.** That is the only
thing that makes the panel worth having — a hand-written copy of the rules is correct on
the day it is written and quietly wrong from the first retune onward, and these thresholds
have already been retuned three times. Add or retune a rule in `THRESHOLDS`, and add or
amend its entry in `FLAG_GUIDE` directly beneath it.

**c. FORM PROFILE.** `grid-template-columns:repeat(5,1fr); gap:8px`. Each cell has a
Mono 9px `#5f666f` label and a Mono 16px weight-600 value, `white-space:nowrap`.
Cells: `SG FORM`, `RNDS 12M`, `CUTS /20`, `STREAK`, `TOP-20 /20`.

**`RNDS 12M` was removed and the grid is now `repeat(4,1fr)` (Aug 2026).** It was the only
one of the five that was neither a result nor a rate — it reported how much golf got
played, which the SG-per-round scatter immediately below shows as its own point count, and
which the flag engine already raises as *Thin sample* on the ~8% of the field where it
changes how the rest should be read. The value is still exported and still drives that
flag. Nothing was promoted into the free space: every candidate (momentum, volatility)
already appears in *Percentile vs field* carrying a field rank this row has no column for,
and a duplicate is a worse tenant than an empty seat.

**d. STROKES GAINED — SEASON.** *(Originally "SG BY PHASE"; renamed Aug 2026 when the
tee-to-green total joined it.)* Five rows, `gap:6px`, each
`grid-template-columns:86px 1fr 96px`. Left is a Mono 11px `#8b929c` `nowrap` label:
`Tee to green`, `Driving`, `Approach`, `Around green`, `Putting` — the label column is
86px specifically so "Around green" stays on one line and all rows are evenly spaced.
Center is a
16px-tall `background:#12151a; radius:3px` track with a 1px `#3d444f` zero line at
`left:38%`; the bar is absolutely positioned (`top:3px; bottom:3px; radius:2px`), growing
right from the zero line for positive values and left for negative, clamped at ±1.4
strokes. Right is Mono 11px: signed value in the bar color, then the field rank in
`#5f666f`. Positive `#57d98a`, negative `#e0655c`.

**The clamp is gone; bars scale to the field's own extremes** (`field.phaseScale`, computed
once over every player and all five rows). No space is wasted on a range nobody occupies,
and the scale holds still while you toggle between players. The section sub-label states
it: `scale −1.7 … +1.7 (field)`.

**On the tee-to-green row (Aug 2026):** T2G is `ott + app + arg` — everything but putting,
and the single most-checked read on a golfer, so it leads the section rather than living
only inside the flag engine. It is in the shared scale, not on one of its own: all five
are strokes gained per round, so one scale is the honest choice — a +0.5 driving bar and a
+0.5 T2G bar are the same length because they are the same number of strokes. It costs
nothing in practice, since the symmetric extreme is set by a *phase* either way (this
field: app −1.67 vs ttg −1.56), so the phase bars do not shrink. Because it is a **sum of
three rows printed beneath it**, it is marked as a subtotal — by a 1px `lineSoft` rule
under the row, and by nothing else. An unmarked sum sitting in a list of its own parts
invites reading five independent measurements and adding them up.

The label is the same `muted` as every other row. It was a step brighter (`text2`)
alongside the rule, and that was the same message stated twice — which read as T2G being a
more *important* metric rather than a different *kind* of one. One signal, one meaning.

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

**Five metrics, not seven** — `P(top-20)` is what the card is sorted by and already sits
in the header, and `Value`, `Leverage` and `Salary` are inputs the optimizer acts on
directly, so ranking them by eye adds nothing you would act on. What is left is what is
genuinely about the *golfer* rather than about the slate, plus two measures nothing else
here states: `SG form`, `SG course`, `Cut % 9mo`, `Momentum 90d`, `Volatility`.

**Bars take `rankColor()` — the same ramp as P(top-20) and VAL (Aug 2026).** The old
three-stop ramp had a green end and no red one, so the worst SG form in the field drew a
stub of grey in the identical shade as the merely-below-average, and the panel could only
ever report who was *good*. Now: green ≥90th, `text` ≥80th, `text2` ≥45th, `muted` ≥15th,
**red below**. A nearly-empty bar already says "bottom of the field"; the hue is what says
whether that is bad. `Volatility` stays `neutral` (flat grey) — it has an ordering but no
verdict, since a wide spread is what a GPP wants and a cash lineup does not. An unmeasured
metric dims the row and prints `n/a` rather than drawing an empty bar, which would read
as "measured, and worst".

**g. COURSE HISTORY.** Sub-label `◆ = this week`.
Header and rows share `grid-template-columns:1fr 32px 44px 40px 44px; gap:0 6px`.
Header is Mono 9px `letter-spacing:0.06em` `#5f666f`: `COURSE`, `EV`, `AVG`, `BEST`,
`CUT%`. Rows are height 23px, `border-top:1px solid #171b21`, Mono 11.5px.
This week's course is sorted to the top, prefixed `◆ `, `background:#1b2430`,
name in `#57d98a`. Show 7 rows.

**Rebuilt as AT THIS COURSE** — scoped to this week's venue only, not a scrolling list of
every course the player has seen. This card is a this-week view: depth on the venue
actually being played beats breadth across venues that are not, and the other courses live
in the Course tab and the Results Browser. It is a 4-up tile row (`EVENTS`, `AVG FIN`,
`BEST`, `CUTS`) over one row per visit (date, tournament, SG, finish). `AVG FIN` counts a
missed cut as 90 — the DB's fill value — which with few events drags the average hard, and
is exactly why every visit is listed underneath rather than hidden behind the aggregate.

**Tile naming and units (Aug 2026):** the tile is `CUTS` reading `67%`, not `CUT%` reading
`67`. The unit belongs in the *value*, matching `CUTS /20` and `TOP-20 /20` in Form
profile — a bare `67` under a `CUT%` header sitting one card away from a `80%` under a
`CUTS /20` header makes the reader check twice whether the two are the same kind of
number. They are; only the window differs. Tiers run bright → dim as the result worsens
(`BEST` green ≤10, `text` ≤20, else `muted`; `CUTS` green ≥80, red <50, else `text`).

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
   full-width primary: `background:#5b8ff9; color:#0b0d10; padding:9px; radius:4px;
   font-size:12px; weight 600` — blue, not green, because Optimize builds the LINEUP and
   blue is the lineup (rule 2 below). Below it a row of three secondaries
   (`border:1px solid #2e343d; color:#c8ccd2; font-size:11.5px; padding:7px`):
   `Gen <N>`, `Save`, and a 30px `✕` (clear). Under the row, and only when the last
   `Gen` press came up short, one amber Mono 9.5px line naming the constraint that ran
   out — same rule as the sync badge: silent when there is nothing to act on.
5. **SAVED header** — `padding:7px 14px; border-top:1px solid #232830`, baseline-spaced:
   `SAVED 5` (Mono 10px, 600, `0.14em`, `#8b929c`) on the left. On the right, the hint
   `none yet — Optimize, then Save` in `#3d444f` when the set is empty, otherwise a
   **`CLEAR ALL`** text button (Mono 10px, 600, `#5f666f`) that **arms on the first click**
   — becoming `DELETE 5?` in amber — and deletes on the second, disarming itself after 3s.
   Labelled rather than a bare `✕` because the label says what it does; the accident it
   would otherwise invite is prevented by the two-step instead of by being small. No modal
   and no `window.confirm` — the armed state lives in the button.
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

> **THE MODEL (Aug 2026).** `locks` and `excludes` are the CONSTRAINTS; `picks` is the
> SOLVER'S OUTPUT. **Every `L` and `X` press re-solves the lineup immediately**, so what is
> on the rail is always the best roster satisfying what you have asked for. Locking is
> therefore *"put this man in my lineup and rebuild around him"* in one press — which is
> what pressing `L` and then Optimize always meant, done in one step instead of two.
>
> This replaced a model where `L` appended the player to `picks` itself. That could not
> answer the full-roster case: appending a seventh player yields a roster DraftKings does
> not accept and that the rail (which draws exactly `roster` slots) cannot display, so the
> press did nothing visible and you had to hit Optimize to see it land. The solver answers
> the same question correctly, in about a millisecond, so there is no reason to make the
> user ask for it.

- **Row click** → selects that player, loads the card. No navigation, no scroll jump.
- **`L` / Lock** → forces the player into every solve, **and re-solves on the spot**. On a
  full roster the optimizer decides who makes way, under the cap — which is the whole
  point, since that is a decision it can make optimally and the app cannot make honestly.
  Pressing `L` again releases the constraint and re-solves without it, so the player stays
  only if he earns the slot on merit. Setting a lock clears any exclusion on the player.
- **`X` / Exclude** → removes the player from every solve, **and re-solves on the spot**,
  so a man in your lineup is replaced by the next best the moment you exclude him. Setting
  an exclusion clears any lock. *(The old "a player can be both in the current build and
  excluded" carve-out is gone: there is no longer a gap between setting a constraint and
  the build obeying it.)*
- **When the constraints cannot be satisfied** — seven locks for six slots, locks that
  exceed the cap — the edit is still applied, the build is left untouched, and the rail
  names the constraint that ran out. Keeping the edit matters: locking a seventh player is
  *how you find out* you have seven locks, and silently refusing the press would leave
  nothing on screen to undo.
- **Clicking a filled lineup slot** → removes that player and drops any lock on him, and
  is the one edit that **deliberately does not re-solve**. It is the only way to say "show
  me this lineup one player short" and have it stick; routing it through the solver would
  hand the slot straight back to the same player whenever he was still the best available,
  which is a remove button that visibly does nothing. The lock has to go with him, or the
  constraints would claim a player the build does not contain.
- **`CLR`** (action-column header, hidden when there is nothing to clear) → drops every
  lock and every exclusion. It does **not** touch the lineup and does **not** re-solve: the
  roster on the rail is still valid and cap-legal, it is simply unconstrained now. Arms on
  the first press — not because it is destructive, but because there is no undo for a set
  of locks you spent minutes choosing.
- **Column header click** → sorts; clicking the active column reverses direction.
  Initial direction is ascending for `PLAYER` and `OWGR_RANK`, descending for everything
  else. Default sort is `P_TOP20` descending.
- **Filter input** → case-insensitive substring match on player name, applied before sort.
- **Optimize** → **rebuilds the lineup from scratch**, keeping only locked players and
  clearing every other slot, maximizing the objective under the salary cap. It used to
  fill in around the current build, which made it a no-op on a full roster: re-optimizing
  meant pressing ✕ first, every single time. Now pressing it twice is meaningful and
  Clear is only needed to actually empty the rail.
- **Gen N** → appends N distinct optimal lineups to the saved set (see below), and leaves
  the current build alone. Every rule is spelled out in a native `title` tooltip on the
  button and on the SAVED label, because the constraint set is not guessable from a
  three-word button. When it adds fewer than N, an amber line under the buttons names the
  constraint that ran out — the shortfall is never silent.
- **Save** → saves the current build if it is a full roster and not already saved.
- **Lineup slot click** → removes that player from the build, **and clears his lock** if
  he had one. Keeping the lock would mean the next Optimize silently puts him back with
  nothing on screen saying why; a removal has to actually take.
- **Card click** → loads that lineup. **Card ✕** → deletes it. **CLEAR ALL** → deletes the
  whole saved set, on the second click.
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
dp[i][s][b] = best objective using the first i players, exactly s slots, exactly b
              buckets of salary;  -inf everywhere, dp[0][0][0] = 0

for i = 1..n:                               # item dimension stays EXPLICIT
  for s = 0..remainingSlots:
    for b = 0..B:
      dp[i][s][b] = dp[i-1][s][b]                            # skip player i
      if s > 0 and b >= weight(i):                           # take player i
        dp[i][s][b] = max(dp[i][s][b], dp[i-1][s-1][b-weight(i)] + value(i))

answer = argmax over b of dp[n][remainingSlots][b]
reconstruct: for i = n..1, player i was taken iff dp[i][s][b] != dp[i-1][s][b]
```

The item dimension is not an optimization mistake to be collapsed away. The design
prototype collapsed it and reconstructed from a single `choice[s][b]` = "last player to
improve this state", which rosters the **same player twice** in 34% of solves
(`test/dp-check.mjs`) — visible in the prototype's own screenshots. Keeping the layers
makes "was player i taken here?" answerable directly, and no player can be picked twice
because the walk-back visits each layer once. ~4 MB and ~500k operations at 149 players.

- `weight(p) = SALARY / 100`
- `value(p) = P_TOP20` (or `MODEL_SCORE` when the objective is switched)
- **The cap is a ceiling, not a target.** The answer is `argmax` over *every* bucket at or
  under the cap, scanning upward and keeping a bucket only on a strict improvement — so of
  two lineups with the same objective the cheaper one is returned, and leftover salary is
  left over. Real slates still come back at exactly $50,000, and that is the field being
  priced sensibly rather than the solver padding the bill: on this week's slate the best
  lineup at $49,900 scores 2.347 against 2.358 at $50,000, and value rises at every
  $1,000 step of cap. Give the same solver an objective uncorrelated with price
  (`value = 1/salary`) and it spends $36,000 of the $50,000. Asserted against brute force
  in `test/optimizer-check.ts` on random pools, where value and salary are unrelated —
  the only kind of instance where the property can actually fail visibly.
- **pool** excludes: manually excluded players, locked players, and players already in
  the build — those are pre-committed, their salary is subtracted from the cap and their
  slots from the count before the DP runs.

**Gen N** wraps the DP to produce N *distinct* lineups. Its invariant, stated as a
property: **it returns exactly N lineups whenever N lineups exist** that are distinct
from each other and from the saved set and satisfy locks, exclusions and the exposure
ceiling — and when it returns fewer, it says which constraint ran out. Tested in
`test/generate-check.ts`, including against exhaustive enumeration on small pools.

1. Compute a usage count per player across all currently saved lineups.
2. Compute a hard ceiling: `max(1, floor(maxExposure% × (savedCount + N)))`.
3. For each lineup: ban every player whose usage has reached the ceiling — **except
   locked players**, which are exempt by definition (capping a player you asked for in
   every lineup is a contradiction, and the explicit instruction wins).
4. Solve for the best lineup that is not already in the saved set, by **Lawler's
   partitioning**: solve the constrained problem; if the answer is one already held,
   split "everything except that answer" into one subproblem per chosen player —
   *child j = exclude the j-th, force the first j-1* — and expand the best-valued open
   subproblem next. Those children are disjoint and jointly exhaustive, so this walks
   lineups in descending order without repeats, and an empty queue is **proof** that no
   distinct lineup exists rather than a guess.
5. A soft `0.006 × usage` penalty biases the objective toward spreading the field instead
   of stacking one core until it hits the ceiling. It is a nudge only — distinctness is
   guaranteed by step 4 — so it is safe to tune.
6. Append results to the saved set; never replace it, and never touch the current build.
7. On a shortfall, report `stop`: `infeasible` (no roster fits the cap), `exposure`
   (the rest would breach the ceiling), `exhausted` (no different roster exists), or
   `capped` (hit the 400-solve safety valve). The rail renders it in amber.

**Two failure modes this replaced, both worth remembering:**

- The soft penalty *alone* does not bound exposure — an early prototype quietly let one
  player appear in every lineup. Hence the hard ceiling in step 3.
- The penalty plus seeded jitter does not produce distinctness either. The shipped
  version re-solved a jittered objective up to `8 × N` times and dropped duplicate
  results — but a duplicate changed no state (usage counted only *accepted* lineups, and
  the jitter was an order of magnitude smaller than the gap between the best and
  second-best lineup), so every remaining attempt re-derived the same optimum and threw
  it away. **Gen 5 on an empty saved list returned one lineup**, and pressing it again
  returned none. Hence step 4: "the best lineup that is not one of these" is a question
  the search answers, not one a perturbed objective is hoped to stumble onto.

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
where a player is an **outlier**, either against the current field or against an absolute
standard, so two players can be compared without reading every number.

**`src/flags.ts` is the source of truth, and the table the original handoff carried is
gone from this document on purpose.** Every threshold lives in one exported `THRESHOLDS`
object with no magic numbers buried in the rules, each carrying its provenance (`[S]`
ported from the Streamlit app's `player_flags()`, `[D]` from the design handoff) and, where
it has been retuned, the census that forced the retune. A second copy of the numbers here
would be correct on the day it was written and quietly wrong from the next retune onward —
which has already happened three times. Read the constants; they are commented.

Severity by dot color, from `SEVERITY_COLOR`: `green` good, `red` bad, `amber` caution,
`dim` none. **`info` no longer renders blue** — blue means lineup membership, and a dot
that color would claim the player is in your build. Red sorts first, then green, then
cautions: the most actionable warning must not be buried under three positives.

The rules, by group — *driving these is the `FLAG_GUIDE` flyout described in card section
(b), which interpolates its numbers from the same `THRESHOLDS` object:*

| Group | Fires on |
|---|---|
| **Form** | SG form in the field's top/bottom 15% (relative), and above `+1.00` / at or below `−0.50` per round (absolute). Both directions can co-fire — one asks "hot for *this* field", the other "hot, full stop". |
| **Cuts & streaks** | 2+ straight missed cuts, 6+ straight made; 9-month cut rate ≥95% or ≤40%. |
| **Ceiling** | Last-20 top-20 rate ≥35% or ≤5%, and only with ≥8 starts. This is the model's actual target outcome, so it is the closest thing to a direct historical read on the number being predicted. |
| **Sample size** | Under 20 rounds in 12 months. A caveat on every other form number for the player, not a verdict on him. |
| **This week's course** | **At most one fires.** Strokes (`SG:C` field-percentile among *measured* players) and record (≥3 events, cut rate, best finish) are weighed together and reported in one sentence naming both. Plus the three no-verdict states: no starts, pre-window starts only, thin history. |
| **Season SG** | Any of the four phases in the field's top/bottom 10%, and SG T2G likewise. T2G can co-fire with a phase — a player can be elite T2G on approach alone. |
| *(none)* | `No outliers vs field on any tracked metric.` — a finding in itself. |

**Price flags are deliberately absent.** The handoff's `VAL`, `LEVERAGE` and
P20-vs-salary-rank rules are all gone: two duplicated grid columns outright, and all three
describe price efficiency, which the optimizer acts on *directly*. An overpriced player is
simply never selected and an underpriced one is targeted without being announced, so
flagging it spent attention on a decision already being made.

Every message embeds the actual value **and**, where meaningful, the field rank — the
owner wants to see the number, not just the verdict. Counts are spelled out rather than
abbreviated: a course record reads `(3 events)`, never `(3 ev)`, which beside a column of
probabilities and strokes-gained figures reads as *expected value*.

`npm run census` prints how often each rule fires across the loaded field. Run it after
touching any threshold — the retunes above all came from it, and it is how you catch a
rule that has quietly started describing the median player instead of a tail.

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
>    in-lineup row shading, the rail's active saved card, `Optimize`, form
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
> `dirColor`, `rankColor`) rather than inline at the call site, so a metric's grid
> column and its card tile cannot drift apart.
>
> **One ramp for verdict metrics.** `rankColor()` serves both P(top-20) and VAL:
> green ≥90th, `text` ≥80th, `text2` ≥45th, `muted` ≥15th, red below, `dimmer`
> when unmeasured. That is `tier()` with exactly two edits — the top band split at
> 0.90 for green, the bottom band recolored red — so every other breakpoint is
> shared and a verdict column and a directionless column change shade at the same
> percentiles. At ≥80th, every column in the grid is the brightest white unless it
> has earned green.
>
> The fifth band is load-bearing. VAL is roughly P(top-20) per dollar, so the two
> correlate hard; with a four-band ramp they collapse into identical blocks under
> the P(top-20) sort and the VAL column stops adding anything. The extra edge
> gives them more places to genuinely disagree — a player whose VAL band beats his
> P(top-20) band is one the salary is underrating.
>
> `rankColor` replaced `p20Color`/`valColor`, whose green thresholds differed by
> 0.05 (0.80 vs 0.85). That gap leaked `tier()`'s brightest white into VAL's
> `[0.80, 0.85)` sliver — a shade P(top-20)'s green covered and could never show.
> It did make the columns disagree more often, but on an artifact of two
> mismatched constants rather than on anything about the players.

**Colors**

| Token | Hex | Use | Was |
|---|---|---|---|
| bg | `#0b0d10` | app background, scrollbar track, player-card column | |
| panel | `#0e1116` | top bar, cards, lineup rail | |
| surface | `#12151a` | grid header, stat cards, bar tracks, slots |
| surface-alt | `#161a20` | input fields |
| slot-empty | `#101317` | empty lineup slots |
| track | `#232931` | progress bar tracks | `#1e232a` |
| line-soft | `#191e25` | row dividers | `#171b21` |
| line | `#282e37` | section and panel borders | `#232830` |
| line-strong | `#39404a` | button borders, header underline, scrollbar thumb | `#2e343d` |
| axis | `#49515c` | chart zero lines, empty-state text | `#3d444f` |
| text | `#e8eaed` | primary — names (Sans 600), headings | |
| text-2 | `#d4d8de` | numeric secondary — salary, odds, CUT9M, OWGR | `#c8ccd2` |
| muted | `#a3aab4` | labels | `#8b929c` |
| dim | `#767e88` | tertiary labels, ranks, neutral (directionless) bars | `#5f666f` |
| dimmer | `#5d646d` | footnotes, null-ish values | `#4c525a` |
| green | `#6ccb5f` | **good** — the only green | `#8ccfa4`, `#6fae8a` |
| red | `#ff7b72` | **bad** — the only red | `#ff99a4`, `#e29088` |
| amber | `#eab453` | caution about app state | `#e0b46e`, `#cfa059` |
| blue | `#4cc2ff` | lineup membership, primary action, active control | `#57bdf0`, `#6f9fd8` |
| select-bg | `#1b2028` | focused row, active tab, pinned course row | `#1a1e25`, `#1b2430` |
| focus-edge | `#d4d8de` | 2px inset edge on the focused row (= text-2) | `#c8ccd2` |
| lineup-bg | `#122733` | in-lineup row, active saved card | `#14262f`, `#16202e` |
| exclude-bg | `#181113` | excluded row | `#1a1113` |
| scatter | `#a3aab4` | chart data points (= muted) | `#5b7fb8` |
| trend | `#d4d8de` | chart rolling-average line (= text-2) | `#e0806c` |

`green-soft` (`#9fd8b4`) and `red-soft` (`#d09a95`) **were removed** — dense-row
numerics use the one green and the one red. Chart marks carry no hue: the rounds
scatter is the dim ramp and the rolling-form line is brighter only because it is
the summary you are meant to read, not a better kind of value.

**Contrast pass (Aug 2026).** The scheme above kept its structure — four hues, one
grey ramp — but every foreground was raised one step, because the muted pass
overshot into "hard to read". Three rules drove it:

1. **Names vs. numbers is the Windows 11 Settings pairing.** A white bold title
   with a description under it that is *darker, but not by much*. `text` (bold,
   Sans 600) is the name; `text-2` is every directionless number. The old gap
   between `#e8eaed` and `#c8ccd2` was wider than that, so the numbers read as
   secondary information rather than as the data.
2. **A hue must not cost legibility.** `green` is WinUI's dark-theme
   `SystemFillColorSuccess` — the "Connected" dot in Windows Settings — taken
   as-is; the hand-mixed mint before it was desaturated to the point of reading
   washed out beside the brighter greys.

   `red` is deliberately *not* the matching `SystemFillColorCritical` (`#ff99a4`),
   which sits at hue 353 and reads pink at grid density. Understand the contrast
   budget before changing it: matching green's 8.7:1 with a red **requires** a
   pale one, because red's luminance rides a channel worth 0.21 of the total
   against green's 0.72 — an equal-contrast red is a pink red, and no deep red
   reaches 8.7:1. So red is pinned at hue ~4 and 7.5:1 (`#ff7b72`): close enough
   that neither column out-weighs the other in a dense grid, deep enough to still
   read as red. Both are far above the 5.5:1 the first muted pass left red at.
   Hue says *which direction*, never *how much*.

   `amber` is matched to the pair by hand at ~9.3:1 rather than taken from WinUI,
   whose `SystemFillColorCaution` (`#fce100`) would out-shout both for a rarer
   message.
3. **Blue is the Windows 11 dark accent itself** — `SystemAccentColorLight2`,
   the toggle and primary-button azure — not the old 213° slate. It separates
   cleanly from the greens and reads as an interface colour rather than a data
   colour, which is what rule 2 says it is. `lineup-bg` was retinted to match,
   and a filled lineup slot's left edge is now blue instead of grey (LOCK still
   overrides with green).

   **Primary buttons are outlined, not filled** (`Optimize`, DB Query's `Run`).
   At this brightness a filled accent slab is a permanent maximum sitting beside
   a grid whose entire job is letting one number stand out — so a primary takes
   the same blue-edge/dark-interior treatment as an in-lineup slot, and outranks
   the grey secondaries by weight and label colour instead. **Filled blue is
   reserved for state that is currently on** — `CardBtn`'s active "In lineup" —
   where the fill *is* the message. An action is not a state.

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

**Fixed dimensions** — grid column floor 620px with 832px of content; player card 448px;
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
characters set in the body font: `✕` (U+2715),
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
