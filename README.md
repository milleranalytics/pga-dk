# PGA DraftKings model + Slate Terminal

Weekly workflow for PGA Tour DraftKings lineups: a Jupyter notebook that maintains the
database, trains the model and scores this week's field, and a browser dashboard — the
**PGA Slate Terminal** — for research and lineup building.

## The weekly routine

Run `pga-dk.ipynb` top to bottom. It imports last week's results, refreshes season stats,
scrapes this week's odds, builds features, trains, scores the field, and publishes
everything the dashboard needs. Then run the last cell (`serve_dashboard`) and build
lineups in the browser.

## Layout

| | |
|---|---|
| `pga-dk.ipynb` | the weekly workflow, in order, with the reasoning in markdown between cells |
| `utils/db_utils.py` | database maintenance: results import, season stats, odds, name mapping |
| `utils/features.py` | point-in-time feature construction, shared by training and scoring |
| `utils/model.py` | pooled training, scoring, prediction logging and grading |
| `utils/dashboard.py` | publishes `slate.js`, and serves the dashboard from the repo root |
| `dashboard/` | the PGA Slate Terminal (Vite + React + TypeScript). See `dashboard/README.md` |
| `experiments/` | forward-chained evaluation of pipeline changes |
| `data/golf.db` | the historical database — results, season stats, odds, logged predictions |

## What the notebook publishes

- `data/current_week_export.csv` — the scored field. The tracked source the dashboard is
  rebuilt from on the other computer (`rebuild_from_disk`), and the in-notebook check on
  what the dashboard should be showing.
- `dashboard/public/data/slate.js` + `dashboard/dist/data/slate.js` — what the dashboard
  actually reads. Generated weekly and gitignored; rebuilt automatically when stale.
- `data/current_week.json` — the current-week marker (tournament, course, ending date).
- New rows in `golf.db`: this week's odds, and this week's predictions for later grading.

## Setup

```
pip install -r requirements.txt
```

Node is needed only to *develop* the dashboard UI (`cd dashboard && npm install`), not to
use it — `dashboard/dist/index.html` is committed, so a pull is enough on a second
machine.

**Retired:** a read-only Streamlit sidecar (`app.py`) and an Excel lineup optimizer. The
dashboard replaced both — `app.py` was removed in August 2026 and lives on in git history.
