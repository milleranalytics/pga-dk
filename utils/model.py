# utils/model.py
# Production pipeline for the weekly notebook: pooled point-in-time training
# set, current-week feature rows, percentile-regression model, odds blend.
#
# Validated by experiments/forward_eval.py (forward-chained 2021-2025):
# this configuration ("s5_blend") scored 6.49 hits@15 / 0.728 AUC vs the
# old per-event notebook pipeline at 5.92 / 0.696 and odds-only at 6.38 / 0.722.

import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine

from utils.features import (
    load_tables, list_events, build_event_rows, build_rounds,
    rolling_features_for_event, sg_features_for_event, add_market_share,
    normalize, feature_columns,
)

RNG = 42



def validate_new_tournament_config(config: dict, allow_stale: bool = False):
    """Refuse current-week operations when the config is stale.

    Catches the 'forgot to update the User Input cell' mistake: the live odds
    scrape always returns THIS week's tournament, so labeling it with a past
    event's name/date corrupts the odds table. The ending date must be today
    or within the next 9 days unless allow_stale=True is passed explicitly."""
    end_date = pd.Timestamp(config["new"]["ending_date"]).normalize()
    today = pd.Timestamp.today().normalize()
    if allow_stale:
        return
    if end_date < today:
        raise ValueError(
            f"Config looks stale: '{config['new']['name']}' ends {end_date.date()}, "
            f"which is in the past (today is {today.date()}). Update the User Input "
            f"cell before scraping/saving odds, or pass allow_stale=True to override.")
    if end_date > today + pd.Timedelta(days=9):
        raise ValueError(
            f"Config ending date {end_date.date()} is more than 9 days out — "
            f"check the User Input cell (or pass allow_stale=True).")


def build_pooled_training(db_path: str, first_season: int, as_of, verbose: bool = True):
    """All events from first_season up to (but excluding) the as_of date,
    each with point-in-time features. Returns (training_df, context) where
    context carries the loaded tables for reuse by the current-week builder."""
    as_of = pd.Timestamp(as_of)
    t, s, o = load_tables(db_path)
    rounds = build_rounds(t)

    seasons = list(range(first_season, int(as_of.year) + 1))
    events = list_events(t, seasons)
    events = events[events["ENDING_DATE"] < as_of]

    frames = []
    for _, ev in events.iterrows():
        rows = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
        if not rows.empty:
            frames.append(rows)
    training_df = pd.concat(frames, ignore_index=True)

    if verbose:
        n_ev = len(events)
        odds_cov = training_df["VEGAS_ODDS"].notna().mean()
        print(f"✅ {len(training_df)} training rows from {n_ev} events "
              f"({events['SEASON'].min()}–{events['SEASON'].max()}), "
              f"odds coverage {odds_cov:.0%}")

    context = {"t": t, "s": s, "o": o, "rounds": rounds}
    return training_df, context


def build_current_week_rows(context: dict, dk_df: pd.DataFrame, odds_current: pd.DataFrame,
                            config: dict, verbose: bool = True) -> pd.DataFrame:
    """Feature rows for this week's DK field. Mirrors build_event_rows but the
    field comes from DKSalaries and the odds from the live scrape."""
    validate_new_tournament_config(config)
    t, s, rounds = context["t"], context["s"], context["rounds"]
    end_date = pd.Timestamp(config["new"]["ending_date"])
    course = config["new"]["course"]
    season = int(config["new"]["season"])

    df = dk_df[["PLAYER", "SALARY"]].copy()
    df["PLAYER"] = df["PLAYER"].astype(str).str.strip()

    # Prior-season stats (strict anti-leakage, same as training)
    stats_sub = s[s["SEASON"] == season - 1].drop_duplicates(subset=["PLAYER"], keep="first")
    stats_sub = stats_sub.drop(columns=["SEASON"], errors="ignore")
    df = df.merge(stats_sub, on="PLAYER", how="left")

    # Live odds
    oc = odds_current[["PLAYER", "VEGAS_ODDS"]].copy()
    oc["PLAYER"] = oc["PLAYER"].astype(str).str.strip()
    oc["VEGAS_ODDS"] = pd.to_numeric(oc["VEGAS_ODDS"], errors="coerce")
    df = df.merge(oc.drop_duplicates(subset=["PLAYER"]), on="PLAYER", how="left")

    # Rolling features as of this week (same windows/fills as training)
    roll = rolling_features_for_event(t, end_date, course, exclude_wd=True)
    df = df.merge(roll["window"][["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS",
                                  "form_density", "CONSECUTIVE_CUTS",
                                  "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK"]],
                  on="PLAYER", how="left")
    df = df.merge(roll["course"], on="PLAYER", how="left")
    df = df.merge(sg_features_for_event(rounds, end_date), on="PLAYER", how="left")
    df = add_market_share(df)
    df["FIELD_SIZE"] = len(df)

    if verbose:
        for label, col in [("odds", "VEGAS_ODDS"), ("stats", "SGTTG"), ("SG form", "SG_FORM")]:
            cov = df[col].notna().mean()
            flag = "✅" if cov >= 0.9 else "⚠️"
            print(f"{flag} {label} coverage: {cov:.0%} of DK field")
        miss = df[df["VEGAS_ODDS"].isna() & df["SG_FORM"].isna()]["PLAYER"].tolist()
        if miss:
            print(f"⚠️ No odds AND no SG history (likely name mismatches): {miss}")
    return df


def save_current_week_odds(db_path: str, odds_current: pd.DataFrame, config: dict):
    """Persist the live odds scrape into the odds table so future seasons are
    fully priced without waiting for the year-end archive."""
    validate_new_tournament_config(config)
    df = odds_current[["SEASON", "TOURNAMENT", "PLAYER", "ODDS", "VEGAS_ODDS"]].copy()
    df.insert(2, "ENDING_DATE", pd.Timestamp(config["new"]["ending_date"]).date())
    df = df.drop_duplicates(subset=["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER"])

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        existing = pd.read_sql(
            "SELECT SEASON, TOURNAMENT, ENDING_DATE, PLAYER FROM odds WHERE SEASON = %d" % int(df["SEASON"].iloc[0]),
            conn)
        existing["ENDING_DATE"] = pd.to_datetime(existing["ENDING_DATE"]).dt.date
        new_df = df.merge(existing, on=["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER"],
                          how="left", indicator=True)
        new_df = new_df[new_df["_merge"] == "left_only"].drop(columns="_merge")
        if new_df.empty:
            print("ℹ️ This week's odds already saved — nothing to insert.")
        else:
            new_df.to_sql("odds", conn, index=False, if_exists="append")
            print(f"✅ Saved {len(new_df)} odds rows for "
                  f"{df['TOURNAMENT'].iloc[0]} ({df['ENDING_DATE'].iloc[0]})")
    engine.dispose()


def train_and_score(training_df: pd.DataFrame, this_week: pd.DataFrame):
    """Fit the percentile regressor on all training rows, score this week's
    field, and blend with the market. Returns (scored this_week, importances).

    SCORE (1 = best): within-field average of the model's rank and the market
    share's rank, rescaled to (0, 1]. MODEL_SCORE = 1 - predicted finish pct."""
    train_n, test_n = normalize(training_df.copy(), this_week.copy())
    fcols = feature_columns(train_n, include_field_size=True, variant="stage4")

    missing = [c for c in fcols if c not in test_n.columns]
    if missing:
        raise ValueError(f"Current-week rows are missing feature columns: {missing}")
    assert not train_n[fcols].isna().any().any(), "NaNs remain in training features"
    assert not test_n[fcols].isna().any().any(), "NaNs remain in current-week features"

    reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                random_state=RNG, n_jobs=-1)
    reg.fit(train_n[fcols], train_n["FINISH_PCT"])

    test_n["MODEL_SCORE"] = 1.0 - reg.predict(test_n[fcols])
    blend = (rankdata(test_n["MODEL_SCORE"]) + rankdata(test_n["ODDS_SHARE"])) / 2
    test_n["SCORE"] = (blend / len(blend)).round(4)

    importances = (pd.Series(reg.feature_importances_, index=fcols)
                   .sort_values(ascending=False))
    return test_n.sort_values("SCORE", ascending=False).reset_index(drop=True), importances
