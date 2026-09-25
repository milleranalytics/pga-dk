"""data/dashboard.db: pga.db written in golf.db's layout, for the dashboard.

The dashboard (utils/dashboard.py's slate export, and the browser's sql.js
panels) reads golf.db's four tables by name and joins them on the golfer's
name. This writes those tables from pga.db, with one spelling per golfer (the
Tour's, as export() shows it) and one name per course, so every join the
dashboard makes lines up with this week's slate. Derived and gitignored:
rebuilt by each run, in seconds.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from pga_api import build, legacy
from pga_api.model import display_names, logged_predictions

PATH = Path(__file__).resolve().parent.parent / "data" / "dashboard.db"


def write(path: Path = PATH) -> Path:
    names = display_names()
    with sqlite3.connect(build.DB_PATH) as con:
        ev = pd.read_sql("SELECT * FROM events", con)
        odds = pd.read_sql("SELECT * FROM odds", con)
    t, s, _ = legacy.tables()
    shown_course = legacy.course_names()
    ev_name = ev.set_index("tournament_id")["name"].str.replace(r" \(20\d\d\)$", "", regex=True)

    def fmt_round(v):
        """golf.db's text: strokes as '68', score to par as '-2' / 'E' / '+3'."""
        if pd.isna(v):
            return None
        v = int(v)
        return str(v) if v >= 50 else ("E" if v == 0 else f"{v:+d}")

    tournaments = pd.DataFrame({
        "SEASON": t["SEASON"], "ENDING_DATE": t["ENDING_DATE"].dt.strftime("%Y-%m-%d"),
        "TOURN_ID": t["TOURN_ID"], "TOURNAMENT": t["TOURNAMENT"].map(ev_name),
        "COURSE": t["COURSE"].map(shown_course), "PLAYER": t["PLAYER"].map(names),
        "POS": t["POS"], "FINAL_POS": t["FINAL_POS"].astype(int),
        **{f"ROUNDS:{i}": t[f"ROUNDS:{i}"].map(fmt_round) for i in (1, 2, 3, 4)},
        "OFFICIAL_MONEY": t["OFFICIAL_MONEY"], "FEDEX_CUP_POINTS": t["FEDEX_CUP_POINTS"],
    })

    stats = s.copy()
    stats["PLAYER"] = stats["PLAYER"].map(names)

    o = odds.merge(ev[["tournament_id", "season", "end_date"]], on="tournament_id")
    odds_t = pd.DataFrame({"SEASON": o["season"], "TOURNAMENT": o["tournament_id"].map(ev_name),
                           "ENDING_DATE": o["end_date"], "PLAYER": o["player_id"].map(names),
                           "ODDS": o["odds_text"], "VEGAS_ODDS": o["decimal_minus_one"]})

    # One forecast per event: this notebook's where it logged one, else the old one's.
    p = logged_predictions()
    if len(p):
        p = p.sort_values("logged_by").drop_duplicates(["tournament_id", "player_id"], keep="first")
        p = p.merge(ev[["tournament_id", "season", "end_date"]], on="tournament_id")
        preds = pd.DataFrame({"SEASON": p["season"], "TOURNAMENT": p["tournament_id"].map(ev_name),
                              "ENDING_DATE": p["end_date"], "PLAYER": p["player_id"].map(names),
                              **{c.upper(): p[c] for c in ("salary", "p_top20", "score", "model_score",
                                                           "odds_share", "leverage", "vegas_odds",
                                                           "sg_form", "predicted_at") if c in p}})
    else:
        preds = pd.DataFrame(columns=["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER"])

    tmp = path.with_suffix(".building")
    tmp.unlink(missing_ok=True)
    with sqlite3.connect(tmp) as con:
        tournaments.to_sql("tournaments", con, index=False)
        stats.to_sql("stats", con, index=False)
        odds_t.to_sql("odds", con, index=False)
        preds.to_sql("predictions", con, index=False)
    con.close()
    tmp.replace(path)
    return path
