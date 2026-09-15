# PGA DraftKings model + Slate Terminal

Weekly workflow for PGA Tour DraftKings lineups: a Jupyter notebook that maintains the
database, trains the model and scores this week's field, and a browser dashboard — the
**PGA Slate Terminal** — for research and lineup building.

## The weekly routine

Run `pga-dk.ipynb` top to bottom. It imports last week's results, refreshes season stats,
downloads this week's DraftKings prices, scrapes this week's odds, builds features, trains,
scores the field, and publishes everything the dashboard needs. Then run the last cell
(`serve_dashboard`) and build lineups in the browser.

**One cell is home-only.** *Download DraftKings salaries* is the only thing here that
talks to DraftKings, and the work network blocks `draftkings.com` outright. Run it at
home, commit `data/salaries/`, and `git pull` at work — the cell that builds the field
reads the saved file and makes no request under any circumstance. See
[`data/salaries/README.md`](data/salaries/README.md).

## Layout

| | |
|---|---|
| `pga-dk.ipynb` | the weekly workflow, in order, with the reasoning in markdown between cells |
| `utils/db_utils.py` | database maintenance: results import, season stats, odds, name mapping |
| `utils/dk_api.py` | this week's DraftKings field and prices: the fetch, and the archive it is read back from |
| `utils/features.py` | point-in-time feature construction, shared by training and scoring |
| `utils/model.py` | pooled training, scoring, prediction logging and grading |
| `utils/dashboard.py` | publishes `slate.js`, and serves the dashboard from the repo root |
| `dashboard/` | the PGA Slate Terminal (Vite + React + TypeScript). See `dashboard/README.md` |
| `experiments/` | forward-chained evaluation of pipeline changes |
| `data/golf.db` | the historical database — results, season stats, odds, logged predictions |
| `data/salaries/` | one DraftKings export per tournament, committed. The system of record for prices — DK does not serve them again |

## What the notebook publishes

- `data/current_week_export.csv` — the scored field. The tracked source the dashboard is
  rebuilt from on the other computer (`rebuild_from_disk`), and the in-notebook check on
  what the dashboard should be showing.
- `dashboard/public/data/slate.js` + `dashboard/dist/data/slate.js` — what the dashboard
  actually reads. Generated weekly and gitignored; rebuilt automatically when stale.
- `data/current_week.json` — the current-week marker (tournament, course, ending date).
- New rows in `golf.db`: this week's odds, and this week's predictions for later grading.

## Setup

Create a project-local Python 3.14 environment so this project's packages do not
modify or depend on packages installed globally:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m ipykernel install --prefix .venv --name pga-dk --display-name "Python 3.14 (pga-dk .venv)"
```

In the notebook editor, select `.venv\Scripts\python.exe` as the kernel. Confirm the
selection in a cell with `import sys; print(sys.executable)`; it should print a path
inside this repository's `.venv` directory.

Node is needed only to *develop* the dashboard UI (`cd dashboard && npm install`), not to
use it — `dashboard/dist/index.html` is committed, so a pull is enough on a second
machine.

**Retired:** a read-only Streamlit sidecar (`app.py`) and an Excel lineup optimizer. The
dashboard replaced both — `app.py` was removed in August 2026 and lives on in git history.
