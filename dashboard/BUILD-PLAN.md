# Build Plan — PGA Slate Terminal

## Moving lineups between computers

`localStorage` is per browser, so lineups do not travel on their own. The repo
is the channel.

`data/lineups/current.json` holds locks, excludes, the current picks and the
saved lineups, and is **written automatically** — there is no save button for
it. The rail shows a small status line ("saved to repo 14:32") so it is clear
the work is on disk.

```
work:  build lineups  →  git add data/lineups/current.json && git commit && push
home:  git pull       →  refresh the dashboard  →  lineups are there
```

Design notes, all deliberate:

- **One file, overwritten in place.** Not one per week — a growing archive of
  past lineups is clutter nobody reads.
- **It names its week.** A file from a different tournament or date is ignored
  entirely, so last week's is invisible rather than deleted, and is overwritten
  the moment this week's lineups start being built.
- **Newest wins.** Both the browser copy and the file carry `saved_at`. The file
  is adopted only when it is strictly newer, or when there is nothing local to
  lose — pulling an older file never clobbers newer local work.
- **The server does the writing.** The endpoint is `POST /api/lineups` on the
  notebook's own local server. Browser-side alternatives are worse: the File
  System Access API is Chromium-only and re-prompts every session, and a GitHub
  token embedded in the page would be readable by anyone once hosted.

Opened from `file://` or a plain static server, autosave is off and the rail
says so; localStorage still holds everything.

## Setup on another computer

**To use the dashboard — no Node needed.** `dist/index.html` is committed, so it
is already built.

```bash
git pull
python -m utils.dashboard      # rebuild dashboard/**/data/slate.js
```

`slate.js` is generated weekly and therefore gitignored, so a fresh clone opens
empty until that command runs. It rebuilds from files that ARE tracked —
`data/golf.db`, `data/current_week_export.csv`, `data/current_week.json` — in a
couple of seconds, with no odds scrape, no DraftKings file and no model
training. Then open the dashboard the normal way, via the notebook's last cell
(`serve_dashboard()`), or by double-clicking `dashboard/dist/index.html` for
everything except the two database tabs.

**To change the UI — Node required.**

```bash
winget install OpenJS.NodeJS.LTS     # once
cd dashboard
npm install                          # from the committed package-lock.json
npm run dev                          # hot reload on localhost:5173
npm run build                        # writes dist/, which IS committed
npm test                             # optimizer + column-filter checks
npm run census                       # flag firing rates on the current slate
```

Commit `dist/index.html` along with the source after a UI change — that file is
what makes the no-Node path work on the other machine.

`npm run dev` is fully functional including the two database tabs: a dev-only
plugin in `vite.config.ts` serves the repo's `data/` directory at `/data`,
since Vite's dev root is `dashboard/` and `golf.db` would otherwise be out of
reach.


Companion to `README.md` (the Claude Design handoff). The handoff is the **design
authority** — colors, layout, thresholds, optimizer algorithm are all correct there and
should be followed as written. This document covers what the handoff could not know: the
real database schema, the real notebook, and the data contract between them.

Where the two disagree, **this document wins on data, the handoff wins on design.**

---

## Corrections to the handoff

The handoff was written without access to `data/golf.db` or the notebook. Four of its
assumptions are wrong.

### 1. Per-round SG exists — do not retitle the scatter

The handoff says the only per-event quantity is score, and instructs retitling section (e)
from "SG per round" to "SG per event."

**Ignore that.** `utils/features.py:build_rounds()` already produces a per-round SG table
from `tournaments."ROUNDS:1"`…`"ROUNDS:4"`:

> SG = that event-round's field mean score − the player's score

Scores are stored as raw strokes in some eras and par-relative in others, but never mixed
within a single event-round, so the within-round difference is valid either way (this is
documented in the function's docstring and was verified).

Section (e) stays **"SG PER ROUND — 24 MO"** exactly as designed, and the scatter has ~4×
more points than the handoff assumed.

### 2. The `predictions` table schema is different

The handoff's stub SQL for the Prediction Tracker joins on `event_id` / `player_id` and
reads a `p_top20_pred` column. None of those exist. Real schema:

```
predictions(SEASON, TOURNAMENT, ENDING_DATE, PLAYER, SALARY, P_TOP20, SCORE,
            MODEL_SCORE, ODDS_SHARE, LEVERAGE, VEGAS_ODDS, SG_FORM, PREDICTED_AT)
```

Graded by joining `tournaments` on `(TOURNAMENT, ENDING_DATE, PLAYER)` and reading
`FINAL_POS` / `POS`. `utils/model.py:grade_predictions()` already implements this join.

### 3. There are no IDs — every join is on player-name strings

No `player_id`, no `event_id`, anywhere in the schema. DK salary names, PGA Tour feed
names, and odds-site names are reconciled through `data/name_mappings.json` and
`PLAYER_NAME_MAP` in `utils/db_utils.py`.

**This is the main reason the notebook precomputes rather than letting the browser join.**
JS-side joins would silently drop any player whose name differs across sources, and the
failure is invisible — a player just quietly has no course history.

### 4. `golf.db` is committed to git

Do **not** copy the 20 MB DB into `dashboard/`. That adds a fresh 20 MB blob to git history
every week. The server is rooted at the repo root and serves the existing `data/golf.db`
in place.

### 5. The prototype optimizer emits duplicate players — do NOT port it as-is

The handoff instructs porting the optimizer directly ("the logic in these prototypes is
real and worth porting directly", "returns the **provably optimal** lineup"). The DP
*values* are provably optimal. **The reconstruction is broken**, and it is visible in the
owner's own screenshot of the prototype: the lineup rail rosters Rickie Fowler twice, and
saved lineup L2 contains Keegan Bradley twice.

Measured against brute force over 3000 random 6-of-N instances
(`node dashboard/test/dp-check.mjs`):

```
reconstructions containing a DUPLICATE player: 1019 / 3000   (34%)
dp objective != brute-force optimum:              0 / 3000
```

So the objective is right and achievable; the recovered roster is not a valid witness.

**Cause.** `PGA Slate Terminal.dc.html:496-528` keeps one `ch[s][b]` = "last player index
to improve this state". The forward pass is correct 0/1 (descending `s` means `dp[s-1]`
is untouched by the current player when `dp[s]` reads it). But a player updates *every*
slot level in its own iteration, so the same `pi` can end up owning both `ch[s][b]` and
`ch[s-1][b-w]` — and the walk-back emits it at both steps. More generally `ch[s-1][b-w]`
may have been overwritten by a later player after `dp[s][b]` was computed from an earlier
snapshot of that cell.

**Fix in Phase 1:** make the item dimension explicit rather than reconstructing from a
collapsed table. Either layer the DP as `dp[pi][s][b]` (150 x 7 x 501 ~ 526k states —
trivial at this scale, and reconstruction is then "did the value change at layer pi")
or keep a `take[pi][s][b]` bit array. **Whichever is chosen, keep the brute-force
differential test** — the invariant is "no player appears more than once AND the objective
equals the exhaustive optimum", and it must be asserted, not eyeballed. A wrong lineup
here is not a cosmetic bug: it silently costs a roster spot.

### Also confirmed available (the handoff was unsure)

- **Season SG phases** — `stats.SGOTT / SGAPR / SGATG / SGP` plus matching `_RANK`
  columns, keyed `(SEASON, PLAYER)`. Section (d) and the four phase flags are fully real.
  Note the handoff's label mapping: Driving → `SGOTT`, Approach → `SGAPR`,
  Around green → `SGATG`, Putting → `SGP`.
- **Course history** — `tournaments` carries `COURSE`, `POS`, `FINAL_POS`. Section (g) is
  a real group-by. (Course names were unified in commit `116838b`; trust them.)

---

## Architecture

Two data paths, deliberately split by whether consistency-with-the-model matters.

| | Source | Delivery | Needs server? |
|---|---|---|---|
| Field, player card, flags, optimizer, tracker | precomputed in Python | `data/slate.js` (`<script>` tag) | no |
| Results Browser, ad-hoc queries | `data/golf.db` via sql.js | `fetch()` | yes |

**Why the split.** Anything whose number also appears in a model feature or a flag message
is computed by the *same Python that trains the model* — so `Steady: 100% cuts made` and
`CUTS /20` can never disagree, and SG form on the card is definitionally the SG form the
model was fed. Free-form exploration has no such constraint, so it goes straight to SQL.

**Why `.js` and not `.json`/`.csv`.** A page opened by double-click (`file://`) cannot
`fetch()` a local file, but `<script src>` is permitted. Wrapping the payload as
`window.SLATE = {…}` means the whole primary workflow survives with no server running.
sql.js needs `fetch()`, so the Results Browser is the only thing that goes dark on
`file://` — the app detects this and hides that tab rather than erroring.

### Stack

Vite + React + TypeScript, built to `dashboard/dist/`.

**The `file://` story needs `vite-plugin-singlefile`, and this is not optional.** Vite's
default build emits `<script type="module" crossorigin src="./assets/index-*.js">`, and
browsers block *external* module scripts over `file://` — an opaque origin fails the CORS
check that module loading requires. So a stock Vite build only runs over HTTP.

`viteSingleFile({ inlinePattern: ["*.js", "*.css"] })` inlines the bundle into
`dist/index.html`. An **inline** module script has nothing to fetch, so it executes fine
from disk. The pattern is root-level only (a single `*` does not cross `/`), which is what
keeps `data/slate.js` external — it must stay a separate file because the notebook
rewrites it weekly, and it is a classic non-module script, which `file://` permits.

Note the ordering this depends on: module scripts are deferred until after parsing, and
`slate.js` is a parser-blocking classic script in `<body>`, so `window.SLATE` is always
set before React mounts.

This is also the answer to "why a bundler instead of plain multi-file vanilla" — unbundled
ES modules have the same `file://` problem and cannot be fixed the same way.

`npm run build` is dev-time only. It is never part of the weekly routine.

---

## Data contract: `data/slate.js`

Written by the notebook next to `golf.db`. One global, one file, ~400–800 KB.

```js
window.SLATE = {
  meta: {
    tournament, course, ending_date, season,   // from tournament_config
    field_size, generated_at,
    cap: 50000, roster: 6
  },

  // The existing export, unchanged — same 14 columns as current_week_export.csv
  players: [
    { PLAYER, SALARY, P_TOP20, SCORE, MODEL_SCORE, ODDS_SHARE, LEVERAGE,
      VEGAS_ODDS, SG_FORM, PCT_FORM_SHRUNK, SG_CH_SHRUNK,
      CUT_PERCENTAGE, FEDEX_CUP_POINTS, OWGR_RANK }
  ],

  // Player card sections c, d, e, g, h — keyed by PLAYER (post name-mapping)
  form: {
    "<player>": {
      rounds_12m,                       // SG_ROUNDS_12M
      cuts_20, top20_20,                // over last 20 starts
      streak: [n, "made" | "missed"],
      phases: { ott, app, arg, putt,
                ott_rank, app_rank, arg_rank, putt_rank },   // season, from stats
      rounds:  [["YYYY-MM-DD", sg], …],                      // 24 mo, section (e)
      courses: [{ course, ev, avg, best, cut_pct }, …],      // section (g)
      results: [{ date, tournament, finish, sg }, …]         // section (h)
    }
  },

  // Prediction Tracker — 602 rows today, trivially embedded
  tracker: [
    { date, tournament, player, p_top20, finish, hit }
  ]
};
```

**Contract rules**

- `players[]` is emitted from the same `export_df` the CSV is written from — one source,
  no drift.
- `form` keys use the post-mapping canonical player name, matching `players[].PLAYER`
  exactly. A player with no history gets an entry with empty arrays, never a missing key.
- `rounds` is `[date, sg]` pairs rather than objects — it is the bulk of the payload
  (~150 players × ~80 rounds) and object keys would roughly triple it.
- The TS side declares this shape in `src/types.ts`. A column rename in the notebook then
  becomes a compile error instead of `undefined` surfacing inside a flag message.

### File placement

Vite serves `public/` in dev and `dist/` in a build, so there is no single location both
modes read. `export_dashboard()` writes `dashboard/public/data/slate.js` and mirrors it
into `dashboard/dist/data/slate.js` when a build exists. **Exactly two copies, and no
third "canonical" one at the repo root** — a generated file with three homes is a
split-brain waiting to happen.

Both are gitignored. `dashboard/dist/index.html` is *not* ignored: it changes only when
the UI changes, not weekly, so committing it lets the second computer pull and use the
dashboard without installing Node at all.

The app loads `./data/slate.js` relatively, which resolves correctly in dev, in the built
`dist/`, and from disk.

---

## Notebook integration

The weekly routine gains **one line in an existing cell and one new final cell.** Nothing
about the existing flow changes, and `current_week_export.csv` keeps being written so the
Excel sheet and Streamlit app stay usable through the whole transition.

**Export cell (currently cell 31)** — after `save_predictions(...)` / `save_current_week_meta(...)`:

```python
from utils.dashboard import export_dashboard
export_dashboard("data/golf.db", export_df, tournament_config)
```

**New final cell** — serve and open:

```python
from utils.dashboard import serve_dashboard
serve_dashboard()   # static server at repo root, background thread, opens the browser
```

Result: run the notebook top to bottom, and the finished app opens by itself with this
week's field already in it, the DB reachable for the Results Browser, and last week's
lineups already cleared (localStorage is keyed on tournament + date + field size).

New module `utils/dashboard.py`, built entirely from functions that already exist —
`build_rounds`, `sg_at_course_for_event`, `sg_features_for_event`, `load_tables`,
`grade_predictions`, and `current_streak` (currently in `app.py`; move it to `utils/`).

---

## Phases

| # | Deliverable | Unlocks |
|---|---|---|
| **0** | Node install · Vite/React/TS scaffold · `utils/dashboard.py` writing `players` + `meta` | `slate.js` in the weekly routine |
| **1** | Tokens + primitives · field grid · card sections a–d, f · optimizer · lineup rail · localStorage | **Excel sheet is replaceable** |
| **2** | Extend exporter with `form` + `tracker` · card sections e, g, h · Prediction Tracker tab | Streamlit's player research is replaceable |
| **3** | `serve_dashboard()` · sql.js against `golf.db` · Results Browser | Free-form DB exploration |
| **4** | Retire `app.py` | One tool instead of three |

Phase 1 is the value inflection — at that point the app already does more than the
Streamlit app plus the Excel optimizer, because the optimizer is exact rather than
whatever Excel's solver was doing.

Build the primitives layer (`tokens.ts`, `DataGrid`, `Section`, `StatCard`,
`PercentileBar`, `Flag`, `Rail`) in Phase 1 as the handoff instructs, before there is
pressure to ship a screen. The Results Browser in Phase 3 is `DataGrid` with different
column defs, not a new table.

## Deferred

- **Compare board** (pin 4 players, metrics as rows, best value highlighted) — from the
  rejected layout B. Wanted, but not on this path.
- **Self-hosted fonts.** The handoff loads IBM Plex from Google Fonts, which breaks the
  offline/`file://` story. Vendor both families into the build in Phase 1.
- The A/B layout toggle in the top bar — prototype-only, drop it.
