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
    # FINISH_PCT is this event's own outcome (used only inside rolling windows);
    # it must never be a feature.
    "FINISH_PCT",
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
    # Finish percentile within each event's field (0 = won, 1 = last).
    # FINAL_POS is 90-filled for CUT/WD, so cap at field size before scaling.
    field_n = t.groupby(["TOURNAMENT", "ENDING_DATE"])["PLAYER"].transform("size")
    t["FINISH_PCT"] = np.minimum(t["FINAL_POS"], field_n) / field_n

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


def _parse_round_score(v):
    s = str(v).strip()
    if s in ("None", "nan", "--", ""):
        return np.nan
    if s == "E":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return np.nan


def build_rounds(t: pd.DataFrame) -> pd.DataFrame:
    """Long table of (PLAYER, ENDING_DATE, SG) — one row per round played.

    SG = field average score that round minus the player's score. Scores are
    stored as raw strokes in some eras and par-relative in others, but never
    mixed within one event-round (verified), so the within-round difference is
    valid either way. Rounds are dated by the event's ENDING_DATE; day-level
    precision within a week is irrelevant at the decay horizon used.
    """
    frames = []
    for i in (1, 2, 3, 4):
        col = f"ROUNDS:{i}"
        sub = t[["PLAYER", "TOURNAMENT", "ENDING_DATE", col]].copy()
        sub["SCORE"] = sub[col].map(_parse_round_score)
        sub["RND"] = i
        frames.append(sub.drop(columns=[col]))
    rounds = pd.concat(frames, ignore_index=True).dropna(subset=["SCORE"])
    grp = rounds.groupby(["TOURNAMENT", "ENDING_DATE", "RND"])["SCORE"]
    rounds["SG"] = grp.transform("mean") - rounds["SCORE"]
    return rounds[["PLAYER", "ENDING_DATE", "SG"]].sort_values("ENDING_DATE").reset_index(drop=True)


SG_HALFLIFE_DAYS = 100
SG_SHRINK_WEIGHT = 2.0   # pseudo-weight pulling low-sample players toward field avg (0)
SG_MAX_LOOKBACK_DAYS = 730


def sg_features_for_event(rounds: pd.DataFrame, end_date) -> pd.DataFrame:
    """Recency-weighted strokes-gained form as of the day before the event.

    SG_FORM = sum(w * SG) / (sum(w) + SG_SHRINK_WEIGHT), w = 0.5^(days_ago/halflife).
    SG_ROUNDS_12M = raw count of rounds in the last 365 days.
    """
    win = rounds[(rounds["ENDING_DATE"] < end_date) &
                 (rounds["ENDING_DATE"] >= end_date - pd.Timedelta(days=SG_MAX_LOOKBACK_DAYS))]
    if win.empty:
        return pd.DataFrame(columns=["PLAYER", "SG_FORM", "SG_ROUNDS_12M"])
    days_ago = (end_date - win["ENDING_DATE"]).dt.days
    w = 0.5 ** (days_ago / SG_HALFLIFE_DAYS)
    tmp = pd.DataFrame({"PLAYER": win["PLAYER"], "w": w, "wsg": w * win["SG"],
                        "recent": (days_ago <= 365).astype(int)})
    g = tmp.groupby("PLAYER").sum()
    out = pd.DataFrame({
        "SG_FORM": (g["wsg"] / (g["w"] + SG_SHRINK_WEIGHT)).round(4),
        "SG_ROUNDS_12M": g["recent"],
    }).reset_index()
    return out


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
        # Stage 2: field-size-aware form with shrinkage toward the field mean (0.5).
        # PCT_FORM_SHRUNK = (sum of finish percentiles + K*0.5) / (n + K)
        K = 4
        pct = g["FINISH_PCT"].agg(["sum", "count"])
        agg["PCT_FORM_SHRUNK"] = ((pct["sum"] + K * 0.5) / (pct["count"] + K)).round(4)
        out["window"] = agg.reset_index()
    else:
        out["window"] = pd.DataFrame(columns=[
            "PLAYER", "TOTAL_EVENTS_PLAYED", "CUTS_MADE", "FEDEX_CUP_POINTS",
            "RECENT_FORM", "CUT_PERCENTAGE", "form_density", "adj_form", "CONSECUTIVE_CUTS",
            "PCT_FORM_SHRUNK"])

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
        K = 2
        chp = ch.groupby("PLAYER")["FINISH_PCT"].agg(["sum", "count"])
        chg["PCT_CH_SHRUNK"] = ((chp["sum"] + K * 0.5) / (chp["count"] + K)).round(4)
        out["course"] = chg.drop(columns=["CH_EVENTS"]).reset_index()
    else:
        out["course"] = pd.DataFrame(columns=["PLAYER", "COURSE_HISTORY", "adj_ch", "PCT_CH_SHRUNK"])
    return out


def add_market_share(event_df: pd.DataFrame) -> pd.DataFrame:
    """Stage 2 odds transform, computed within one event's field.

    Implied win prob p = 1/(odds+1); unlisted players get half the field's
    minimum listed p; normalize so the field sums to 1 (removes vig and makes
    values comparable across seasons/books/field sizes)."""
    p = 1.0 / (event_df["VEGAS_ODDS"] + 1.0)
    if p.notna().any():
        p = p.fillna(p.min(skipna=True) * 0.5)
    else:
        p = pd.Series(1.0, index=event_df.index)  # no odds at all: uniform
    event_df["ODDS_SHARE"] = p / p.sum()
    return event_df


def build_event_rows(t, s, o, event, stats_season_offset=1, exclude_wd=False,
                     window_months=9, rounds=None):
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
                                  "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK"]],
                  on="PLAYER", how="left")
    df = df.merge(roll["course"], on="PLAYER", how="left")
    df = add_market_share(df)
    if rounds is not None:
        df = df.merge(sg_features_for_event(rounds, end_date), on="PLAYER", how="left")

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
        # Stage 2 fills: no events in window -> inactive, punished (0.8);
        # no course history -> neutral prior (0.5). ODDS_SHARE never NaN.
        if "PCT_FORM_SHRUNK" in f.columns:
            f["PCT_FORM_SHRUNK"] = f["PCT_FORM_SHRUNK"].fillna(0.8)
        if "PCT_CH_SHRUNK" in f.columns:
            f["PCT_CH_SHRUNK"] = f["PCT_CH_SHRUNK"].fillna(0.5)
        # Stage 4 fills: never seen a round -> below-average form (train 25th pct)
        if "SG_FORM" in f.columns:
            f["SG_FORM"] = f["SG_FORM"].fillna(train["SG_FORM"].quantile(0.25))
        if "SG_ROUNDS_12M" in f.columns:
            f["SG_ROUNDS_12M"] = f["SG_ROUNDS_12M"].fillna(0)
    num_cols = train.select_dtypes(include=[np.number]).columns
    means = train[num_cols].mean()
    for f in frames:
        f[num_cols] = f[num_cols].fillna(means)
    return (train, test) if test is not None else train


STAGE2_NEW = ["ODDS_SHARE", "PCT_FORM_SHRUNK", "PCT_CH_SHRUNK"]
STAGE2_REPLACED = ["VEGAS_ODDS", "RECENT_FORM", "adj_form", "COURSE_HISTORY", "adj_ch"]
STAGE4_NEW = ["SG_FORM", "SG_ROUNDS_12M"]


def feature_columns(df: pd.DataFrame, include_field_size: bool, variant: str = "legacy") -> list:
    """variant='legacy': the notebook's feature set (excludes Stage 2/4 columns).
    variant='stage2': swap raw odds / avg-finish features for market share and
    shrunken finish-percentile features.
    variant='stage4': stage2 plus round-level strokes-gained form."""
    exclude = set(META_COLS) | {"FIELD_SIZE"}
    if variant == "legacy":
        exclude |= set(STAGE2_NEW) | set(STAGE4_NEW)
    elif variant == "stage2":
        exclude |= set(STAGE2_REPLACED) | set(STAGE4_NEW)
    elif variant == "stage4":
        exclude |= set(STAGE2_REPLACED)
    else:
        raise ValueError(variant)
    cols = [c for c in df.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    if include_field_size:
        cols.append("FIELD_SIZE")
    return cols
