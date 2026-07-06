# utils/model.py
# Production pipeline for the weekly notebook: pooled point-in-time training
# set, current-week feature rows, percentile-regression model, odds blend.
#
# Validated by experiments/forward_eval.py (forward-chained 2021-2025):
# this configuration ("s6_blend": SG-at-course replaces finish-based course
# history) scored 6.49 hits@15 / 0.730 AUC, beating s5_blend (6.475) in 4 of
# 5 test seasons, the old per-event notebook pipeline (5.93 / 0.697), and
# odds-only (6.49 hits but with model arms ahead on full-field AUC).

import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine

from utils.features import (
    load_tables, list_events, build_event_rows, build_rounds,
    rolling_features_for_event, sg_features_for_event, sg_at_course_for_event,
    add_market_share, normalize, feature_columns,
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



def _norm_tourn_name(s: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def validate_scrape_matches_config(odds_current: pd.DataFrame, config: dict,
                                   allow_stale: bool = False):
    """Content-based guard: trust what the odds page says it is serving.

    If the page header parsed, require the config tournament name to match it
    (and the ending date to be within a day, catching date typos). This works
    even when the board hasn't rolled over yet (e.g. running Monday after the
    event). If the header could not be parsed, fall back to the date-window
    check in validate_new_tournament_config."""
    import difflib
    scraped = odds_current.attrs.get("scraped_tournament")
    scraped_end = odds_current.attrs.get("scraped_end_date")
    cfg_name = config["new"]["name"]
    cfg_end = pd.Timestamp(config["new"]["ending_date"]).normalize()

    if scraped:
        a, b = _norm_tourn_name(scraped), _norm_tourn_name(cfg_name)
        name_ok = a and b and (a in b or b in a or
                               difflib.SequenceMatcher(None, a, b).ratio() >= 0.75)
        if not name_ok:
            raise ValueError(
                f"Odds page is serving '{scraped}' but the User Input cell says "
                f"'{cfg_name}'. Update the config before saving/building — saving "
                f"this scrape under the wrong tournament corrupts the odds table.")
        if scraped_end is not None and abs((pd.Timestamp(scraped_end) - cfg_end).days) > 1:
            raise ValueError(
                f"Odds page says '{scraped}' ends {scraped_end}, but the User Input "
                f"cell says {cfg_end.date()} — check new_ending_date for a typo.")
        return  # content-verified: name and date agree with the page

    # Header parse failed (site layout change?) — fall back to date-window guard
    validate_new_tournament_config(config, allow_stale=allow_stale)


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
                            config: dict, verbose: bool = True,
                            allow_stale: bool = False) -> pd.DataFrame:
    """Feature rows for this week's DK field. Mirrors build_event_rows but the
    field comes from DKSalaries and the odds from the live scrape.

    allow_stale=True permits a past-week config (workflow testing)."""
    validate_scrape_matches_config(odds_current, config, allow_stale=allow_stale)
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
    df = df.merge(sg_at_course_for_event(rounds, end_date, course), on="PLAYER", how="left")
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


def save_current_week_odds(db_path: str, odds_current: pd.DataFrame, config: dict,
                           dry_run: bool = False):
    """Persist the live odds scrape into the odds table so future seasons are
    fully priced without waiting for the year-end archive.

    dry_run=True (test mode): show what would be inserted but write nothing.
    Use this when checking the workflow against a past week's config — the
    scrape still returns THIS week's odds, so actually saving them under a
    past event's label would mislabel the odds table."""
    validate_scrape_matches_config(odds_current, config, allow_stale=dry_run)
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
        if dry_run:
            print(f"🧪 TEST MODE — would insert {len(new_df)} odds rows as "
                  f"'{df['TOURNAMENT'].iloc[0]}' ({df['ENDING_DATE'].iloc[0]}); nothing written.")
            engine.dispose()
            return
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
    fcols = feature_columns(train_n, include_field_size=True, variant="stage6")

    missing = [c for c in fcols if c not in test_n.columns]
    if missing:
        raise ValueError(f"Current-week rows are missing feature columns: {missing}")
    assert not train_n[fcols].isna().any().any(), "NaNs remain in training features"
    assert not test_n[fcols].isna().any().any(), "NaNs remain in current-week features"

    reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                random_state=RNG, n_jobs=-1)
    reg.fit(train_n[fcols], train_n["FINISH_PCT"])

    test_n["MODEL_SCORE"] = 1.0 - reg.predict(test_n[fcols])
    model_rank = rankdata(test_n["MODEL_SCORE"])
    market_rank = rankdata(test_n["ODDS_SHARE"])
    test_n["SCORE"] = ((model_rank + market_rank) / 2 / len(test_n)).round(4)
    # positive = model ranks the player higher than the market does
    test_n["LEVERAGE"] = (model_rank - market_rank).round(1)

    importances = (pd.Series(reg.feature_importances_, index=fcols)
                   .sort_values(ascending=False))
    return test_n.sort_values("SCORE", ascending=False).reset_index(drop=True), importances


def save_predictions(db_path: str, export_df: pd.DataFrame, config: dict,
                     dry_run: bool = False):
    """Append this week's scored field to the predictions table so the model
    accumulates a live out-of-sample track record. One row per player per
    event; re-running the same week is a no-op."""
    validate_new_tournament_config(config, allow_stale=dry_run)
    cols = [c for c in ["PLAYER", "SALARY", "SCORE", "MODEL_SCORE", "ODDS_SHARE",
                        "LEVERAGE", "VEGAS_ODDS", "SG_FORM"] if c in export_df.columns]
    df = export_df[cols].copy()
    df.insert(0, "SEASON", int(config["new"]["season"]))
    df.insert(1, "TOURNAMENT", config["new"]["name"])
    df.insert(2, "ENDING_DATE", str(pd.Timestamp(config["new"]["ending_date"]).date()))
    df["PREDICTED_AT"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        try:
            existing = pd.read_sql(
                "SELECT DISTINCT TOURNAMENT, ENDING_DATE FROM predictions", conn)
            already = ((existing["TOURNAMENT"] == df["TOURNAMENT"].iloc[0]) &
                       (existing["ENDING_DATE"] == df["ENDING_DATE"].iloc[0])).any()
        except Exception:
            already = False
        if already:
            print(f"ℹ️ Predictions for {df['TOURNAMENT'].iloc[0]} "
                  f"({df['ENDING_DATE'].iloc[0]}) already logged — nothing added.")
        elif dry_run:
            print(f"🧪 TEST MODE — would log {len(df)} predictions for "
                  f"{df['TOURNAMENT'].iloc[0]} ({df['ENDING_DATE'].iloc[0]}); nothing written.")
        else:
            df.to_sql("predictions", conn, index=False, if_exists="append")
            print(f"✅ Logged {len(df)} predictions for {df['TOURNAMENT'].iloc[0]} "
                  f"({df['ENDING_DATE'].iloc[0]}) — track record grows.")
    engine.dispose()


def grade_predictions(db_path: str, last_n: int = 10):
    """Report card: join logged predictions against imported results.

    Shows, per logged week, how many of the model's top-15 SCOREs finished
    top-20 (forward-chained eval baseline: ~6.5) and their cut rate."""
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        try:
            preds = pd.read_sql("SELECT * FROM predictions", conn)
        except Exception:
            preds = pd.DataFrame()
        results = pd.read_sql(
            "SELECT TOURNAMENT, ENDING_DATE, PLAYER, POS, FINAL_POS FROM tournaments", conn)
    engine.dispose()

    if preds.empty:
        print("ℹ️ No predictions logged yet — the Export cell starts the log.")
        return None

    j = preds.merge(results, on=["TOURNAMENT", "ENDING_DATE", "PLAYER"], how="left")
    weeks = []
    for (tourn, date), grp in j.groupby(["TOURNAMENT", "ENDING_DATE"]):
        top15 = grp.nlargest(15, "SCORE")
        graded = grp["FINAL_POS"].notna().any()
        weeks.append({
            "ENDING_DATE": date, "TOURNAMENT": tourn,
            "top15_in_top20": int((top15["FINAL_POS"] <= 20).sum()) if graded else None,
            "top15_cut_rate": (round(float((~top15["POS"].isin(["CUT", "W/D"])).mean()), 2)
                               if graded else None),
            "status": "graded" if graded else "awaiting results",
        })
    wk = pd.DataFrame(weeks).sort_values("ENDING_DATE", ascending=False).head(last_n)
    done = wk[wk["status"] == "graded"]
    if len(done):
        print(f"📋 {len(done)} graded week(s) · avg top-15→top-20 hits: "
              f"{done['top15_in_top20'].mean():.2f} (eval baseline ≈ 6.5)")
    return wk.reset_index(drop=True)
