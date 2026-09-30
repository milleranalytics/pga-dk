# old_workflow/ — the retired golf.db workflow

`pga-dk.ipynb` and `golf.db`, the hand-maintained workflow that `pga-weekly.ipynb`
replaced in September 2026 (Bank of Utah Championship week, the last week both ran).
**Nothing outside this folder reads anything in it**, so the whole folder can be deleted.

It is a record, not a working copy: the notebook will not run from here. Its imports
expect this folder's files back in their old places, and it used functions that have since
been removed from the shared modules (`utils/model.py`'s golf.db training, logging and
grading; `utils/dashboard.py`'s `rebuild_from_disk`; `utils/dk_api.py`'s `load_field`). To
run it, check out the last commit before the move.

| here | was | what it was |
|---|---|---|
| `pga-dk.ipynb` | repo root | the weekly notebook: typed-in tournament config, golf.db updates |
| `data/golf.db` | `data/` | the hand-maintained database: results, season stats, odds, predictions |
| `data/current_week.json`, `data/current_week_export.csv` | `data/` | its week marker and scored field |
| `data/name_mappings.json` | `data/` | its hand-made name maps (DraftKings, odds, tournaments) |
| `utils/db_utils.py`, `utils/schema.py` | `utils/` | golf.db maintenance: results and stats import, odds backfill, renames |
| `pga_api/compare.py`, `pga_api/golf_db_repair.py` | `pga_api/` | the golf.db ↔ pga.db parity check, and the repairs it drove |
| `experiments/forward_eval.py` (+ results CSVs) | `experiments/` | the July 2026 forward test on golf.db |

## What the new workflow kept from it

- **golf.db's odds and forecast log**, frozen as `data/history/golfdb_odds.csv` and
  `data/history/golfdb_predictions.csv` — the only data in golf.db the API cannot give
  back. `pga_api/archives.py` reads them on every rebuild.
- **The golfodds.com scraper** (`db_utils.get_current_week_odds`), now
  `pga_api/golfodds.py`: `weekly.odds(week, source="golfodds")`.
- **`normalize_name`**, now `pga_api/names.py`. The name maps it came with were not
  needed: the resolver matches 27 of their 30 names by itself (nicknames, accents,
  hyphens), and the other three are now aliases in `data/player_aliases.csv`.
- **The forward test's metric** (`score_event`), now `experiments/metrics.py`.
