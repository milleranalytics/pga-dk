# PGA DraftKings model + Slate Terminal

Weekly workflow for PGA Tour DraftKings lineups: a Jupyter notebook that rebuilds the
database from the PGA Tour's own API, trains the model and scores this week's field, and
a browser dashboard — the **PGA Slate Terminal** — for research and lineup building.

## The weekly routine

Run `pga-weekly.ipynb` top to bottom. Nothing to type: this week's event comes from the
Tour's schedule, and every golfer (DraftKings, odds, results) is matched by the Tour's
player id. It rebuilds `data/pga.db`, reads this week's DraftKings prices, takes this
week's odds, trains, scores the field, logs the forecast, and publishes the dashboard.
The last cell opens the dashboard; build lineups in the browser.

**One cell is home-only.** *4a. Download* is the only thing that talks to DraftKings, and
the work network blocks `draftkings.com` outright. Run it at home, commit `data/salaries/`,
and `git pull` at work — every other cell reads the saved file. See
[`data/salaries/README.md`](data/salaries/README.md).

**Commit each week:** `data/salaries/`, `data/odds/`, `data/predictions/`,
`data/api_week_export.csv`, `data/api_week.json` and any new `data/api_cache/` files. A
price, a board or a forecast cannot be fetched again once the week has passed.

## Layout

| | |
|---|---|
| `pga-weekly.ipynb` | the weekly workflow, in order, with the reasoning in markdown between cells |
| `pga_api/` | the PGA Tour API client, the `pga.db` build, the strokes-gained ratings, odds, the weekly steps, the model's rows, logging and checks |
| `utils/features.py` | point-in-time feature construction, shared by training and scoring |
| `utils/model.py` | the model: percentile forest blended with the market |
| `utils/dk_api.py` | the DraftKings download and the archive of saved prices |
| `utils/dashboard.py` | publishes `slate.js`, and serves the dashboard from the repo root |
| `dashboard/` | the PGA Slate Terminal (Vite + React + TypeScript). See `dashboard/README.md` |
| `experiments/` | forward tests of model changes (`sg_form_eval.py`) and their shared metrics |
| `old_workflow/` | the retired `pga-dk.ipynb` / `golf.db` workflow. Nothing reads it; see its README |

## The data

`data/pga.db` is **derived and gitignored**: every run rebuilds it from committed files, so
each computer makes its own and nothing is synced as a database.

| committed | what it is |
|---|---|
| `data/api_cache/` | the API's replies (results, rounds, stats, strokes gained, fields). A rebuild needs no network for anything already finished |
| `data/salaries/` | one DraftKings file per tournament. DraftKings does not serve them again |
| `data/odds/` | each week's odds board (FanDuel, or golfodds.com). The latest saved board for an event is the one `pga.db` keeps |
| `data/predictions/` | each week's logged forecast, for the report card |
| `data/history/` | golf.db's odds (2015–2026) and forecast log, frozen when it was retired |
| `data/player_aliases.csv`, `data/name_mappings.json` | names no rule matches to a Tour player id |

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

**Retired:** the `pga-dk.ipynb` / `golf.db` workflow (September 2026, in `old_workflow/`),
a read-only Streamlit sidecar (`app.py`, August 2026) and an Excel lineup optimizer. All
live on in git history.
