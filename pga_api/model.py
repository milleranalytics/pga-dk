"""Train, score, log and grade, on pga.db.

The feature functions and the model are utils/features.py and utils/model.py,
unchanged: pga_api.legacy hands them pga.db in golf.db's shape, keyed by the
Tour's ids. pga_api.validate holds the checks that this matches the golf.db
pipeline.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from pga_api import build, legacy
from utils.db_utils import normalize_name
from utils.features import (add_market_share, build_event_rows, build_rounds, list_events,
                            rolling_features_for_event, sg_at_course_for_event,
                            sg_features_for_event)
from utils.model import train_and_score  # noqa: F401  (re-exported for the notebook)

DATA = Path(__file__).resolve().parent.parent / "data"
PRED_DIR = DATA / "predictions"
EXPORT_CSV = DATA / "api_week_export.csv"
FIRST_SEASON = 2016

EXPORT_COLS = ["PLAYER", "SALARY", "P_TOP20", "SCORE", "MODEL_SCORE", "ODDS_SHARE", "LEVERAGE",
               "VEGAS_ODDS", "SG_FORM", "PCT_FORM_SHRUNK", "SG_CH_SHRUNK",
               "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "OWGR_RANK"]
ROUNDING = {"P_TOP20": 3, "MODEL_SCORE": 4, "ODDS_SHARE": 4, "LEVERAGE": 1, "SG_FORM": 2,
            "PCT_FORM_SHRUNK": 3, "SG_CH_SHRUNK": 2, "CUT_PERCENTAGE": 1,
            "FEDEX_CUP_POINTS": 0, "OWGR_RANK": 0}


def display_names() -> pd.Series:
    """player_id -> the name shown everywhere: the Tour's, accents dropped as golf.db did.

    The dashboard joins on names, so two golfers may not share one: the one with
    fewer starts is shown with his id, 'Zach Johnson (29747)'."""
    with sqlite3.connect(build.DB_PATH) as con:
        pl = pd.read_sql("SELECT player_id, name FROM players", con)
        starts = pd.read_sql("SELECT player_id, COUNT(*) n FROM results GROUP BY player_id", con)
    pl["shown"] = pl["name"].map(normalize_name)
    pl = pl.merge(starts, on="player_id", how="left").fillna({"n": 0})
    pl = pl.sort_values("n", ascending=False)
    dup = pl.duplicated("shown", keep="first")
    pl.loc[dup, "shown"] = pl.loc[dup, "shown"] + " (" + pl.loc[dup, "player_id"] + ")"
    return pl.set_index("player_id")["shown"]


# ---------------------------------------------------------------- train

def training(as_of, first_season: int = FIRST_SEASON, verbose: bool = True):
    """Every stroke-play event from first_season up to (not including) `as_of`,
    each with its features as they stood before it. -> (rows, context)"""
    as_of = pd.Timestamp(as_of)
    t, s, o = legacy.tables()
    rounds = build_rounds(t)
    events = list_events(t, list(range(first_season, as_of.year + 1)))
    events = events[events["ENDING_DATE"] < as_of]
    rows = pd.concat([build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
                      for _, ev in events.iterrows()], ignore_index=True)
    if verbose:
        print(f"training: {len(rows):,} golfer-events from {len(events)} events "
              f"({events['SEASON'].min()}-{events['SEASON'].max()}), "
              f"odds for {rows['VEGAS_ODDS'].notna().mean():.0%}")
    return rows, {"t": t, "s": s, "o": o, "rounds": rounds}


# ---------------------------------------------------------------- this week

def week_rows(ctx: dict, week, dk: pd.DataFrame, odds: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Features for this week's priced field, exactly as training builds them.

    `dk` is weekly.prices() (player_id, salary); `odds` has player_id and
    VEGAS_ODDS (fractional odds as a number: 12/1 -> 12.0). A priced golfer the
    Tour cannot identify keeps a row, with fills where his history would be."""
    t, s, rounds = ctx["t"], ctx["s"], ctx["rounds"]
    end = pd.Timestamp(week.end_date)
    course = legacy.week_course_key(week.tournament_id)

    df = pd.DataFrame({"PLAYER": dk["player_id"].fillna("dk:" + dk["dk_name"]).values,
                       "SALARY": dk["salary"].values})
    stats = s[s["SEASON"] == week.season - 1].drop_duplicates("PLAYER").drop(columns="SEASON")
    df = df.merge(stats, on="PLAYER", how="left")
    o = odds.dropna(subset=["player_id"]).drop_duplicates("player_id")
    df = df.merge(o.rename(columns={"player_id": "PLAYER"})[["PLAYER", "VEGAS_ODDS"]],
                  on="PLAYER", how="left")
    roll = rolling_features_for_event(t, end, course, exclude_wd=True)
    df = df.merge(roll["window"][["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density",
                                  "CONSECUTIVE_CUTS", "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK"]],
                  on="PLAYER", how="left")
    df = df.merge(roll["course"], on="PLAYER", how="left")
    df = df.merge(sg_features_for_event(rounds, end), on="PLAYER", how="left")
    df = df.merge(sg_at_course_for_event(rounds, end, course), on="PLAYER", how="left")
    df = add_market_share(df)
    df["FIELD_SIZE"] = len(df)
    if verbose:
        for label, col in (("odds", "VEGAS_ODDS"), ("last season's stats", "SGTTG"),
                           ("strokes-gained form", "SG_FORM")):
            print(f"  {label}: {df[col].notna().mean():.0%} of the priced field")
        print(f"  course history at this course: {df['SG_CH_SHRUNK'].notna().sum()} golfers")
    return df


def export(scored: pd.DataFrame, week) -> pd.DataFrame:
    """The dashboard's 14 columns, named for display, plus player_id; saved to
    data/api_week_export.csv."""
    names = display_names()
    out = scored.copy()
    out["player_id"] = out["PLAYER"]
    out["PLAYER"] = [names.get(p, p[3:] if str(p).startswith("dk:") else p) for p in out["PLAYER"]]
    out = out[[c for c in EXPORT_COLS if c in out.columns] + ["player_id"]]
    for c, nd in ROUNDING.items():
        if c in out:
            out[c] = out[c].round(nd)
    out = out.sort_values("P_TOP20", ascending=False).reset_index(drop=True)
    out.to_csv(EXPORT_CSV, index=False)
    week_marker(week)
    print(f"exported {len(out)} players to data/{EXPORT_CSV.name}")
    return out


def week_marker(week) -> None:
    import json
    names = legacy.course_names()
    meta = {"tournament_id": week.tournament_id, "name": week.name,
            "course": names.get(legacy.week_course_key(week.tournament_id), week.course),
            "season": week.season, "ending_date": str(week.end_date)}
    (DATA / "api_week.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- the log

def _slug(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def log_predictions(export_df: pd.DataFrame, week) -> Path | None:
    """Save this week's forecast to data/predictions/ (committed: it can only be
    made before the event). Logged once per event; a re-run changes nothing."""
    PRED_DIR.mkdir(exist_ok=True)
    path = PRED_DIR / f"{week.season}-{week.end_date}-{_slug(week.name)}.csv"
    if path.exists():
        print(f"predictions for {week.name} already logged ({path.name}); nothing changed.")
        return None
    cols = ["player_id", "PLAYER", "SALARY", "P_TOP20", "SCORE", "MODEL_SCORE", "ODDS_SHARE",
            "LEVERAGE", "VEGAS_ODDS", "SG_FORM"]
    df = export_df[[c for c in cols if c in export_df]].copy()
    df.insert(0, "tournament_id", week.tournament_id)
    df["PREDICTED_AT"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    df.to_csv(path, index=False)
    print(f"logged {len(df)} predictions to data/predictions/{path.name}  (commit it)")
    return path


def logged_predictions() -> pd.DataFrame:
    """Every logged forecast: the golf.db log (the old notebook) and this one's."""
    with sqlite3.connect(build.DB_PATH) as con:
        old = pd.read_sql("SELECT * FROM predictions", con).assign(logged_by="golf.db notebook")
    files = sorted(PRED_DIR.glob("*.csv"))
    new = (pd.concat([pd.read_csv(f, dtype={"player_id": str}) for f in files], ignore_index=True)
           .rename(columns=str.lower).assign(logged_by="API notebook") if files else pd.DataFrame())
    return pd.concat([old, new], ignore_index=True)


def report_card(last_n: int = 10) -> pd.DataFrame:
    """Each logged week against its result: how many of the top 15 by P_TOP20
    finished top 20 (the forward test's yardstick), what P_TOP20 expected, and
    the top 15's cut rate. One row per notebook, so the two can be compared."""
    preds = logged_predictions()
    if preds.empty:
        print("No predictions logged yet.")
        return pd.DataFrame()
    with sqlite3.connect(build.DB_PATH) as con:
        res = pd.read_sql("SELECT tournament_id, player_id, position, finish_rank FROM results", con)
        ev = pd.read_sql("SELECT tournament_id, name, end_date FROM events", con)
    j = preds.merge(res, on=["tournament_id", "player_id"], how="left")
    rows = []
    for (tid, who), g in j.groupby(["tournament_id", "logged_by"]):
        top = g.nlargest(15, "p_top20")
        graded = g["position"].notna().any()
        rows.append({"tournament_id": tid, "logged_by": who,
                     "top15_in_top20": int((top["finish_rank"] <= 20).sum()) if graded else None,
                     "expected_top20": round(float(top["p_top20"].sum()), 1),
                     "top15_cut_rate": round(float((~top["position"].isin(["CUT", "W/D", "DQ"])).mean()), 2)
                     if graded else None,
                     "status": "graded" if graded else "awaiting results"})
    wk = pd.DataFrame(rows).merge(ev, on="tournament_id").sort_values("end_date", ascending=False)
    wk = wk[["end_date", "name", "logged_by", "top15_in_top20", "expected_top20",
             "top15_cut_rate", "status"]]
    recent = wk["end_date"].drop_duplicates().head(last_n)
    wk = wk[wk["end_date"].isin(recent)].reset_index(drop=True)
    for who, g in wk[wk["status"] == "graded"].groupby("logged_by"):
        print(f"{who}: {len(g)} graded week(s), top 15 -> top 20 hits {g['top15_in_top20'].mean():.2f} "
              f"(forward-test baseline about 6.5)")
    return wk
