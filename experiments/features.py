# experiments/features.py
# Vectorized, point-in-time feature builder.
# Replicates the semantics of utils/db_utils.py rolling-feature functions
# (get_cut_and_fedex_history, get_recent_avg_finish, get_course_history,
# build_training_rows) but computes all events in one pass so the
# forward-chaining evaluation is tractable.

import sqlite3
import numpy as np
import pandas as pd

PERCENT_STATS = ["SCRAMBLING", "DRIVING_ACCURACY", "BIRDIES", "GIR"]

META_COLS = [
    "PLAYER", "SEASON", "TOURNAMENT", "ENDING_DATE", "COURSE",
    "POS", "FINAL_POS", "TOP_20",
]


def load_tables(db_path: str):
    con = sqlite3.connect(db_path)
    t = pd.read_sql("SELECT * FROM tournaments", con)
    s = pd.read_sql("SELECT * FROM stats", con)
    o = pd.read_sql("SELECT SEASON, TOURNAMENT, ENDING_DATE, PLAYER, VEGAS_ODDS FROM odds", con)
    con.close()

    t["ENDING_DATE"] = pd.to_datetime(t["ENDING_DATE"])
    t["FINAL_POS"] = pd.to_numeric(t["FINAL_POS"], errors="coerce")
    t["PLAYER"] = t["PLAYER"].astype(str).str.strip()

    s["PLAYER"] = s["PLAYER"].astype(str).str.strip()
    # Clean percentage stats stored as strings like '62.5%'; coerce the rest numeric
    for col in s.columns:
        if col in ("PLAYER",):
            continue
        s[col] = (
            s[col].astype(str).str.replace("%", "", regex=False)
            .replace(["None", "nan", "NaN", "--", "DNP", ""], np.nan)
        )
        s[col] = pd.to_numeric(s[col], errors="coerce")

    o["ENDING_DATE"] = pd.to_datetime(o["ENDING_DATE"])
    o["PLAYER"] = o["PLAYER"].astype(str).str.strip()
    o = o.drop_duplicates(subset=["TOURNAMENT", "ENDING_DATE", "PLAYER"], keep="first")
    return t, s, o


def list_events(t: pd.DataFrame, seasons) -> pd.DataFrame:
    ev = (
        t[t["SEASON"].isin(seasons)][["SEASON", "TOURNAMENT", "ENDING_DATE", "COURSE"]]
        .drop_duplicates()
        .sort_values("ENDING_DATE")
        .reset_index(drop=True)
    )
    return ev


def _round_half_away(x, decimals):
    # SQLite/db_utils ROUND() rounds .5 away from zero; numpy rounds half-to-even.
    m = 10 ** decimals
    return np.sign(x) * np.floor(np.abs(x) * m + 0.5) / m


def _trailing_streak(made_cut_by_date: np.ndarray) -> int:
    # Count of consecutive True values at the END of the (date-sorted) array
    return int(made_cut_by_date[::-1].cumprod().sum())


def rolling_features_for_event(t, end_date, course, window_months=9, ch_years=7,
                               exclude_wd=False):
    """Cuts/FedEx/form/course-history for one event, as of the day before it starts.

    Mirrors db_utils: window is [end - N months, end - 1 day], MADE_CUT = POS not in
    (CUT, W/D), RECENT_FORM = avg FINAL_POS (cuts included at their filled value),
    COURSE_HISTORY = avg FINAL_POS at same course over ch_years.
    exclude_wd=True drops W/D rows from the windows entirely (Stage 1 change).
    """
    start = end_date - pd.DateOffset(months=window_months)
    win = t[(t["ENDING_DATE"] >= start) & (t["ENDING_DATE"] <= end_date - pd.Timedelta(days=1))]
    if exclude_wd:
        win = win[win["POS"] != "W/D"]

    out = {}
    if not win.empty:
        win = win.sort_values(["PLAYER", "ENDING_DATE"])
        made = ~win["POS"].isin(["CUT", "W/D"])
        g = win.assign(MADE_CUT=made).groupby("PLAYER")
        agg = g.agg(
            TOTAL_EVENTS_PLAYED=("POS", "count"),
            CUTS_MADE=("MADE_CUT", "sum"),
            FEDEX_CUP_POINTS=("FEDEX_CUP_POINTS", lambda x: pd.to_numeric(x, errors="coerce").sum()),
            RECENT_FORM=("FINAL_POS", "mean"),
        )
        agg["CUT_PERCENTAGE"] = (agg["CUTS_MADE"] / agg["TOTAL_EVENTS_PLAYED"] * 100).round(1)
        agg["form_density"] = (agg["FEDEX_CUP_POINTS"] / agg["TOTAL_EVENTS_PLAYED"]).round(2)
        agg["RECENT_FORM"] = _round_half_away(agg["RECENT_FORM"], 1)
        agg["adj_form"] = (agg["RECENT_FORM"] / np.log1p(agg["TOTAL_EVENTS_PLAYED"])).round(2)
        agg["CONSECUTIVE_CUTS"] = g["MADE_CUT"].apply(lambda s: _trailing_streak(s.to_numpy()))
        out["window"] = agg.reset_index()
    else:
        out["window"] = pd.DataFrame(columns=[
            "PLAYER", "TOTAL_EVENTS_PLAYED", "CUTS_MADE", "FEDEX_CUP_POINTS",
            "RECENT_FORM", "CUT_PERCENTAGE", "form_density", "adj_form", "CONSECUTIVE_CUTS"])

    ch_start = end_date - pd.DateOffset(years=ch_years)
    ch = t[(t["COURSE"] == course) &
           (t["ENDING_DATE"] >= ch_start) & (t["ENDING_DATE"] <= end_date - pd.Timedelta(days=1))]
    if exclude_wd:
        ch = ch[ch["POS"] != "W/D"]
    if not ch.empty:
        chg = ch.groupby("PLAYER").agg(
            CH_EVENTS=("FINAL_POS", "count"),
            COURSE_HISTORY=("FINAL_POS", "mean"),
        )
        chg["COURSE_HISTORY"] = _round_half_away(chg["COURSE_HISTORY"], 1)
        chg["adj_ch"] = (chg["COURSE_HISTORY"] / np.log1p(chg["CH_EVENTS"])).round(2)
        out["course"] = chg.drop(columns=["CH_EVENTS"]).reset_index()
    else:
        out["course"] = pd.DataFrame(columns=["PLAYER", "COURSE_HISTORY", "adj_ch"])
    return out


def build_event_rows(t, s, o, event, stats_season_offset=1, exclude_wd=False,
                     window_months=9):
    """One event's (player, features, label) rows, all point-in-time."""
    end_date = event["ENDING_DATE"]
    season = int(event["SEASON"])
    tournament = event["TOURNAMENT"]
    course = event["COURSE"]

    base = t[(t["ENDING_DATE"] == end_date) & (t["TOURNAMENT"] == tournament)].copy()
    if base.empty:
        return pd.DataFrame()
    base = base.drop(columns=[c for c in ["ROUNDS:1", "ROUNDS:2", "ROUNDS:3", "ROUNDS:4",
                                          "OFFICIAL_MONEY", "FEDEX_CUP_POINTS", "TOURN_ID"]
                              if c in base.columns])

    # Prior-season stats (no same-season fallback: strict anti-leakage)
    stats_sub = s[s["SEASON"] == season - stats_season_offset].copy()
    stats_sub = stats_sub.drop_duplicates(subset=["PLAYER"], keep="first")
    stats_sub = stats_sub.drop(columns=["SEASON"], errors="ignore")
    df = base.merge(stats_sub, on="PLAYER", how="left")

    # Odds for this event
    odds_sub = o[(o["ENDING_DATE"] == end_date) & (o["TOURNAMENT"] == tournament)]
    df = df.merge(odds_sub[["PLAYER", "VEGAS_ODDS"]], on="PLAYER", how="left")

    # Rolling features
    roll = rolling_features_for_event(t, end_date, course,
                                      window_months=window_months, exclude_wd=exclude_wd)
    df = df.merge(roll["window"][["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS",
                                  "form_density", "CONSECUTIVE_CUTS",
                                  "RECENT_FORM", "adj_form"]],
                  on="PLAYER", how="left")
    df = df.merge(roll["course"], on="PLAYER", how="left")

    df["TOP_20"] = (df["FINAL_POS"] <= 20).astype(int)
    df["FIELD_SIZE"] = len(base)
    return df


def normalize(train: pd.DataFrame, test: pd.DataFrame = None):
    """Notebook's normalization: intentional bad-fills first, then mean-fill.
    Fill statistics are fit on train and applied to both frames."""
    frames = [train] if test is None else [train, test]
    owgr_min = train["OWGR"].min(skipna=True)
    for f in frames:
        f["VEGAS_ODDS"] = f["VEGAS_ODDS"].fillna(1000).clip(upper=1000)
        f["OWGR"] = f["OWGR"].fillna(owgr_min)
        f["OWGR_RANK"] = f["OWGR_RANK"].fillna(1000).astype(float).clip(upper=1000)
        f["RECENT_FORM"] = f["RECENT_FORM"].fillna(90)
        f["FEDEX_CUP_POINTS"] = f["FEDEX_CUP_POINTS"].fillna(0)
    num_cols = train.select_dtypes(include=[np.number]).columns
    means = train[num_cols].mean()
    for f in frames:
        f[num_cols] = f[num_cols].fillna(means)
    return (train, test) if test is not None else train


def feature_columns(df: pd.DataFrame, include_field_size: bool) -> list:
    exclude = set(META_COLS) | {"FIELD_SIZE"}
    cols = [c for c in df.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    if include_field_size:
        cols.append("FIELD_SIZE")
    return cols
