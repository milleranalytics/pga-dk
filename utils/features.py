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
    courses = t[["TOURNAMENT", "ENDING_DATE", "COURSE"]].drop_duplicates()
    rounds = rounds.merge(courses, on=["TOURNAMENT", "ENDING_DATE"], how="left")
    return (rounds[["PLAYER", "ENDING_DATE", "RND", "TOURNAMENT", "COURSE", "SG"]]
            .sort_values("ENDING_DATE").reset_index(drop=True))


def current_streak(pos_recent_first: pd.Series):
    """(length, 'made'|'missed') of the current cut streak. Expects POS ordered
    most-recent start first.

    Lives in utils/ rather than in a UI so every consumer shares one
    definition — nothing should be able to disagree about what a streak is.
    """
    if len(pos_recent_first) == 0:
        return 0, None
    made = ~pos_recent_first.isin(["CUT", "W/D"])
    first = bool(made.iloc[0])
    run = 0
    for m in made:
        if bool(m) == first:
            run += 1
        else:
            break
    return run, ("made" if first else "missed")


SG_HALFLIFE_DAYS = 100
SG_SHRINK_WEIGHT = 2.0   # pseudo-weight pulling low-sample players toward field avg (0)
SG_MAX_LOOKBACK_DAYS = 730


SG_COURSE_SHRINK_ROUNDS = 2  # pseudo-rounds of field-average golf


def _empty_feature_frame(cols):
    """Typed empty frame: PLAYER stays object, features float64. A bare
    pd.DataFrame(columns=...) makes every column object dtype, which turns the
    merged feature columns object-NaN and trips pandas' fillna-downcasting
    FutureWarning in normalize()."""
    return pd.DataFrame({c: pd.Series(dtype="object" if c == "PLAYER" else "float64")
                         for c in cols})


def sg_at_course_for_event(rounds: pd.DataFrame, end_date, course, years: int = 7) -> pd.DataFrame:
    """Shrunken strokes gained per round AT this course over the past `years`.

    SG_CH_SHRUNK = sum(SG at course) / (n_rounds + K): thin samples pull toward
    0 (field average). Same role as PCT_CH_SHRUNK but measured in strokes
    instead of finish positions."""
    win = rounds[(rounds["COURSE"] == course) &
                 (rounds["ENDING_DATE"] >= end_date - pd.DateOffset(years=years)) &
                 (rounds["ENDING_DATE"] <= end_date - pd.Timedelta(days=1))]
    if win.empty:
        return _empty_feature_frame(["PLAYER", "SG_CH_SHRUNK"])
    g = win.groupby("PLAYER")["SG"].agg(["sum", "count"])
    out = pd.DataFrame({
        "SG_CH_SHRUNK": (g["sum"] / (g["count"] + SG_COURSE_SHRINK_ROUNDS)).round(4),
        # How much measurement is behind SG_CH_SHRUNK. Shrinkage pulls a 1-round
        # sample almost all the way to 0, and normalize() fills a player with NO
        # rounds here to 0 as well, so the value alone cannot distinguish "never
        # played here" from "played here and was exactly field average" — two
        # populations that finish top-20 at 15.2% and 17.9%.
        "CH_ROUNDS": g["count"].astype(float),
    }).reset_index()
    return out


def sg_features_for_event(rounds: pd.DataFrame, end_date) -> pd.DataFrame:
    """Recency-weighted strokes-gained form as of the day before the event.

    SG_FORM = sum(w * SG) / (sum(w) + SG_SHRINK_WEIGHT), w = 0.5^(days_ago/halflife).
    SG_ROUNDS_12M = raw count of rounds in the last 365 days.
    """
    win = rounds[(rounds["ENDING_DATE"] < end_date) &
                 (rounds["ENDING_DATE"] >= end_date - pd.Timedelta(days=SG_MAX_LOOKBACK_DAYS))]
    if win.empty:
        return _empty_feature_frame(["PLAYER", "SG_FORM", "SG_ROUNDS_12M"])
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


SG_MOMENTUM_DAYS = 90          # how far back the rolling-form line is compared
SG_VOL_ROUNDS = 40             # ~10 starts: enough rounds for a stable SD
SG_MIN_ROUNDS_FOR_SPREAD = 12  # below this, an SD is noise, so publish nothing
SG_FAST_HALFLIFE_DAYS = 30     # ~5 starts of memory, vs SG_FORM's ~100 days


def sg_trend_features_for_event(rounds: pd.DataFrame, end_date) -> pd.DataFrame:
    """Direction and spread of a player's form, as of the day before the event.

    SG_FORM is a LEVEL. These two are the other axes of the same round history:

      SG_MOMENTUM_90D  how far the rolling-form line has moved in the last 90
                       days — the derivative of SG_FORM.
      SG_VOL           SD of per-round SG over the last SG_VOL_ROUNDS rounds —
                       the round-to-round spread (floor/ceiling), not a level.
      SG_MOMENTUM_90D_EV  momentum with the 90-day window anchored at the EVENT
                       date instead of the player's last round (see below).
      SG_FORM_FAST     SG_FORM with a 30-day halflife instead of 100 — a level,
                       not a difference, but one with a much shorter memory.
                       SG_FORM is already recency-weighted, so "is he heating
                       up" is partly baked into it; if there is recency signal
                       left over, a faster level is a cleaner way to reach it
                       than differencing two EWMAs of the same halflife.

    These are the same three numbers the player card shows, with the same
    definitions, so the card and the model cannot disagree about them.

    Momentum without re-running an EWMA per player: a time-based EWMA value is
    sum(w*sg)/sum(w) with w = 0.5^(days/halflife), and rescaling every weight by
    a constant leaves that ratio unchanged. So the EWMA is flat between rounds
    and is independent of where it is anchored — the value "as of T" is just the
    weighted mean of the rounds up to T, computable with one groupby per window.
    That makes this exactly the card's trend.iloc[-1] - past.iloc[-1], vectorized.

    Rounds are dated the way the card dates them — round 4 on ENDING_DATE, round
    1 three days earlier — rather than by ENDING_DATE as elsewhere in this
    module. The weighting barely notices three days at a 100-day halflife, but
    the ORDER matters: without it the four rounds of an event tie, and whichever
    of them the sort happens to leave inside the last SG_VOL_ROUNDS moves the SD
    by up to 0.3 strokes. Dating each round separately makes the boundary
    deterministic and chronological instead of an artifact of the sort.

    Anchoring: the card measures the last 90 days OF THE PLAYER'S PLAY, because
    a card is a description of that player. For a model feature that is a
    lookahead-free but stale reading — a player who last teed it up in March is
    scored in August on how they were trending in March. SG_MOMENTUM_90D_EV
    anchors the window at the event instead, which reports exactly 0.0 for a
    player with no rounds in the last 90 days (same rounds in both windows =
    no change). Both are built so the eval can decide between them.
    """
    cols = ["PLAYER", "SG_MOMENTUM_90D", "SG_MOMENTUM_90D_EV", "SG_VOL", "SG_FORM_FAST"]
    win = rounds[(rounds["ENDING_DATE"] < end_date) &
                 (rounds["ENDING_DATE"] >= end_date - pd.Timedelta(days=SG_MAX_LOOKBACK_DAYS))]
    if win.empty:
        return _empty_feature_frame(cols)

    win = win.assign(
        DATE=win["ENDING_DATE"] - pd.to_timedelta(4 - win["RND"], unit="D")
    ).sort_values("DATE", kind="stable")
    days_ago = (end_date - win["DATE"]).dt.days
    w = 0.5 ** (days_ago / SG_HALFLIFE_DAYS)

    # Rounds on or before (player's last round - 90d), and before (event - 90d).
    last = win.groupby("PLAYER")["DATE"].transform("max")
    pre_player = win["DATE"] <= last - pd.Timedelta(days=SG_MOMENTUM_DAYS)
    pre_event = win["DATE"] <= end_date - pd.Timedelta(days=SG_MOMENTUM_DAYS)
    # Only the last SG_VOL_ROUNDS rounds count toward the spread.
    in_tail = win.groupby("PLAYER").cumcount(ascending=False) < SG_VOL_ROUNDS

    wf = 0.5 ** (days_ago / SG_FAST_HALFLIFE_DAYS)
    tmp = pd.DataFrame({
        "PLAYER": win["PLAYER"].to_numpy(),
        "n": 1.0,
        "w": w.to_numpy(), "wsg": (w * win["SG"]).to_numpy(),
        "w_p": np.where(pre_player, w, 0.0),
        "wsg_p": np.where(pre_player, w * win["SG"], 0.0),
        "w_e": np.where(pre_event, w, 0.0),
        "wsg_e": np.where(pre_event, w * win["SG"], 0.0),
        "wf": wf.to_numpy(), "wfsg": (wf * win["SG"]).to_numpy(),
    })
    g = tmp.groupby("PLAYER").sum()

    # 0/0 -> NaN rather than a fabricated zero: no rounds before the window
    # opened means there is no earlier form level to difference against.
    with np.errstate(invalid="ignore", divide="ignore"):
        now = g["wsg"] / g["w"]
        then_p = np.where(g["w_p"] > 0, g["wsg_p"] / g["w_p"].replace(0, np.nan), np.nan)
        then_e = np.where(g["w_e"] > 0, g["wsg_e"] / g["w_e"].replace(0, np.nan), np.nan)

    tail = win[in_tail].groupby("PLAYER")["SG"]
    # ddof=1: a sample of the player's rounds, not a population. Thin samples
    # publish nothing — an SD over a handful of rounds is noise, not a floor.
    vol = tail.std(ddof=1).where(tail.size() >= SG_MIN_ROUNDS_FOR_SPREAD)

    out = pd.DataFrame({
        "SG_MOMENTUM_90D": (now - then_p).round(4),
        "SG_MOMENTUM_90D_EV": (now - then_e).round(4),
        "SG_VOL": vol.round(4),
        # Same shrink as SG_FORM: at a 30-day halflife a player who has not
        # played in months has almost no weight left, and the pseudo-weight
        # correctly pulls them to field average rather than to a stale level.
        "SG_FORM_FAST": (g["wfsg"] / (g["wf"] + SG_SHRINK_WEIGHT)).round(4),
    })
    # The card gates momentum on the same round count it gates the SD on.
    thin = g.index[g["n"] < SG_MIN_ROUNDS_FOR_SPREAD]
    out.loc[thin, ["SG_MOMENTUM_90D", "SG_MOMENTUM_90D_EV"]] = np.nan
    return out.reset_index()


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
        out["window"] = _empty_feature_frame([
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
        # CH_EVENTS is kept rather than dropped: it is the sample size behind
        # every course-history number here, and the shrunken values cannot
        # express the difference between a thin sample and no sample at all.
        chg["CH_EVENTS"] = chg["CH_EVENTS"].astype(float)
        out["course"] = chg.reset_index()
    else:
        out["course"] = _empty_feature_frame(["PLAYER", "COURSE_HISTORY", "adj_ch",
                                              "PCT_CH_SHRUNK", "CH_EVENTS"])
    return out


REST_MAX_DAYS = 365       # a layoff longer than this is not meaningfully longer
REST_WINDOWS = (28, 90)   # "played last month" and "played last quarter"


def rest_features_for_event(t: pd.DataFrame, end_date) -> pd.DataFrame:
    """Schedule state as of the day before the event: rust and workload.

    DAYS_SINCE_LAST_START  days since the player last teed it up, capped.
    STARTS_28D / STARTS_90D  events started inside those windows.

    Nothing in the feature set currently knows when a player last played —
    SG_ROUNDS_12M is an annual count, which cannot tell a player who has made
    eight starts in nine weeks from one who made them in nine months. The effect
    is real and it changes sign with player quality: over 2019-2025, inside the
    same market tier, longshots off 10+ weeks finish top-20 at 4.2% against 9.5%
    for those playing weekly, while favourites off a long break go the other way
    (44.4% vs 39.1%). That interaction is what a forest is for.

    A start is any appearance, including a missed cut or a W/D — the question is
    whether the player has been competing recently, not how it went. (Contrast
    exclude_wd, which governs the form windows, where how it went is the point.)
    """
    cols = ["PLAYER", "DAYS_SINCE_LAST_START"] + [f"STARTS_{d}D" for d in REST_WINDOWS]
    prior = t[t["ENDING_DATE"] < end_date]
    if prior.empty:
        return _empty_feature_frame(cols)

    last = prior.groupby("PLAYER")["ENDING_DATE"].max()
    out = pd.DataFrame({
        "DAYS_SINCE_LAST_START": (end_date - last).dt.days.clip(upper=REST_MAX_DAYS).astype(float)
    })
    for d in REST_WINDOWS:
        w = prior[prior["ENDING_DATE"] >= end_date - pd.Timedelta(days=d)]
        out[f"STARTS_{d}D"] = (w.groupby("PLAYER")["ENDING_DATE"].nunique()
                               .reindex(out.index).fillna(0).astype(float))
    return out.reset_index()


def add_field_strength(event_df: pd.DataFrame) -> pd.DataFrame:
    """How strong is the field this player has to beat — computed within the event.

    TOP_20 is a relative outcome: it takes beating 80% of THIS field. Every
    skill feature in the set is absolute, and FIELD_SIZE is the model's only
    handle on the difficulty of the week — a bad one, since it runs backwards
    (bigger fields are weaker, r = -0.75) and barely tracks the count of elite
    players in them. Meanwhile the field's mean SG_FORM ranges from -0.57 to
    +1.04 across events, a swing as wide as the whole spread of players inside a
    typical field, so a split on absolute SG_FORM means a different thing every
    week.

    SG_FORM_Z is supplied explicitly rather than left to the trees: a forest
    splits on one column at a time and cannot subtract two of them, so giving it
    the mean and the level is not the same as giving it the difference.
    """
    f = pd.to_numeric(event_df["SG_FORM"], errors="coerce")
    mean = f.mean(skipna=True)
    sd = f.std(ddof=1, skipna=True)
    event_df["FIELD_SG_MEAN"] = mean
    event_df["FIELD_SG_SD"] = sd
    event_df["SG_FORM_Z"] = (f - mean) / sd if pd.notna(sd) and sd > 0 else np.nan
    return event_df


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
                     window_months=9, rounds=None, candidates=False):
    """One event's (player, features, label) rows, all point-in-time.

    candidates=True also builds the stage 7/8 columns, which no shipped variant
    selects — they were tested and did not earn a place (see experiments/). The
    eval scripts pass True; the weekly pipeline leaves them off so it does not
    spend time on features nothing reads.
    """
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
    win_cols = ["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density",
                "CONSECUTIVE_CUTS", "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK"]
    course_drop = [] if candidates else ["CH_EVENTS"]
    if candidates:
        win_cols.append("TOTAL_EVENTS_PLAYED")
    df = df.merge(roll["window"][win_cols], on="PLAYER", how="left")
    df = df.merge(roll["course"].drop(columns=course_drop, errors="ignore"),
                  on="PLAYER", how="left")
    df = add_market_share(df)
    if rounds is not None:
        df = df.merge(sg_features_for_event(rounds, end_date), on="PLAYER", how="left")
        ch = sg_at_course_for_event(rounds, end_date, course)
        df = df.merge(ch if candidates else ch.drop(columns=["CH_ROUNDS"], errors="ignore"),
                      on="PLAYER", how="left")
    if candidates:
        df["HAS_STATS"] = df["SGTTG"].notna().astype(float)
        df = df.merge(rest_features_for_event(t, end_date), on="PLAYER", how="left")
        df = add_field_strength(df)
        if rounds is not None:
            df = df.merge(sg_trend_features_for_event(rounds, end_date),
                          on="PLAYER", how="left")

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
        if "SG_CH_SHRUNK" in f.columns:
            f["SG_CH_SHRUNK"] = f["SG_CH_SHRUNK"].fillna(0.0)
        # Stage 7 fills: too few rounds to measure a direction -> not trending
        # (0.0, the honest "no information" value for a difference); too few to
        # measure a spread -> the train median, since an unknown SD is more
        # likely typical than extreme and 0 would read as a metronome.
        for c in ("SG_MOMENTUM_90D", "SG_MOMENTUM_90D_EV"):
            if c in f.columns:
                f[c] = f[c].fillna(0.0)
        if "SG_VOL" in f.columns:
            f["SG_VOL"] = f["SG_VOL"].fillna(train["SG_VOL"].median())
        if "SG_FORM_FAST" in f.columns:
            f["SG_FORM_FAST"] = f["SG_FORM_FAST"].fillna(train["SG_FORM_FAST"].quantile(0.25))
        # Stage 8 fills. The sample-size columns fill to 0 because that is the
        # literal truth — no rounds here, no events in the window — and it is
        # the whole point of carrying them: 0 is the value that tells the model
        # the neighbouring shrunken feature is a prior, not a measurement.
        for c in ("CH_ROUNDS", "CH_EVENTS", "TOTAL_EVENTS_PLAYED", "HAS_STATS"):
            if c in f.columns:
                f[c] = f[c].fillna(0.0)
        # Never seen teeing it up -> as rusty as the cap allows, no recent starts.
        if "DAYS_SINCE_LAST_START" in f.columns:
            f["DAYS_SINCE_LAST_START"] = f["DAYS_SINCE_LAST_START"].fillna(REST_MAX_DAYS)
        for d in REST_WINDOWS:
            c = f"STARTS_{d}D"
            if c in f.columns:
                f[c] = f[c].fillna(0.0)
        # A player with no SG_FORM sits at the field average by construction.
        if "SG_FORM_Z" in f.columns:
            f["SG_FORM_Z"] = f["SG_FORM_Z"].fillna(0.0)
    num_cols = train.select_dtypes(include=[np.number]).columns
    means = train[num_cols].mean()
    for f in frames:
        # current-week frames lack outcome/meta columns; fill what's present
        cols = [c for c in num_cols if c in f.columns]
        f[cols] = f[cols].fillna(means[cols])
    return (train, test) if test is not None else train


STAGE2_NEW = ["ODDS_SHARE", "PCT_FORM_SHRUNK", "PCT_CH_SHRUNK"]
STAGE2_REPLACED = ["VEGAS_ODDS", "RECENT_FORM", "adj_form", "COURSE_HISTORY", "adj_ch"]
STAGE4_NEW = ["SG_FORM", "SG_ROUNDS_12M"]
STAGE6_NEW = ["SG_CH_SHRUNK"]
# Candidates under evaluation, kept out of every shipped variant until they earn
# a place. SG_MOMENTUM_90D_EV is the event-anchored rival of SG_MOMENTUM_90D;
# the two measure the same thing and must never both be in a feature set.
STAGE7_NEW = ["SG_MOMENTUM_90D", "SG_MOMENTUM_90D_EV", "SG_VOL", "SG_FORM_FAST"]
# Stage 8 candidates, also held out of every shipped variant until the eval
# says otherwise: schedule state, field strength, and the sample-size columns
# that say whether a shrunken feature was measured or imputed.
STAGE8_REST = ["DAYS_SINCE_LAST_START"] + [f"STARTS_{d}D" for d in REST_WINDOWS]
STAGE8_FIELD = ["FIELD_SG_MEAN", "FIELD_SG_SD", "SG_FORM_Z"]
STAGE8_COUNTS = ["CH_ROUNDS", "CH_EVENTS", "TOTAL_EVENTS_PLAYED", "HAS_STATS"]
STAGE8_NEW = STAGE8_REST + STAGE8_FIELD + STAGE8_COUNTS


def feature_columns(df: pd.DataFrame, include_field_size: bool, variant: str = "legacy") -> list:
    """variant='legacy': the notebook's feature set (excludes Stage 2/4 columns).
    variant='stage2': swap raw odds / avg-finish features for market share and
    shrunken finish-percentile features.
    variant='stage4': stage2 plus round-level strokes-gained form.
    variant='stage6': stage4 with SG-at-course REPLACING PCT_CH_SHRUNK.
    variant='stage6b': stage4 plus SG-at-course (keeps both course features)."""
    exclude = set(META_COLS) | {"FIELD_SIZE"} | set(STAGE7_NEW) | set(STAGE8_NEW)
    if variant == "legacy":
        exclude |= set(STAGE2_NEW) | set(STAGE4_NEW) | set(STAGE6_NEW)
    elif variant == "stage2":
        exclude |= set(STAGE2_REPLACED) | set(STAGE4_NEW) | set(STAGE6_NEW)
    elif variant == "stage4":
        exclude |= set(STAGE2_REPLACED) | set(STAGE6_NEW)
    elif variant == "stage6":
        exclude |= set(STAGE2_REPLACED) | {"PCT_CH_SHRUNK"}
    elif variant == "stage6b":
        exclude |= set(STAGE2_REPLACED)
    else:
        raise ValueError(variant)
    cols = [c for c in df.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    if include_field_size:
        cols.append("FIELD_SIZE")
    return cols
