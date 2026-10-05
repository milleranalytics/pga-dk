"""Checks that the pipeline reads nothing from the future, and its forward
record. Each prints its reading; none writes.

    from pga_api import validate
    validate.truncation()         # a feature for event T is unchanged when data from T on is removed
    validate.sg_truncation()      # the same for sg_form's ratings, Korn Ferry included
    validate.forward_eval()       # the production model's out-of-sample record
    validate.dry_run("R2026557")  # a finished week replayed through the notebook's steps, graded
    validate.odds_sources()       # golfodds.com v the Tour's FanDuel feed, and the model's record on each

The comparisons with golf.db that proved this pipeline out (feature_parity, a
two-pipeline forward_eval) are in git history before golf.db was retired.
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from pga_api import build, legacy
from utils.features import build_event_rows, build_rounds, list_events

EXPERIMENTS = build.DB_PATH.parent.parent / "experiments"

# The features truncation() checks.
FEATURES = ["CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density", "CONSECUTIVE_CUTS",
            "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK", "COURSE_HISTORY", "adj_ch",
            "PCT_CH_SHRUNK", "SG_FORM", "SG_ROUNDS_12M", "SG_CH_SHRUNK", "VEGAS_ODDS",
            "ODDS_SHARE", "FINISH_PCT", "TOP_20", "FIELD_SIZE"]


def truncation(n_events: int = 40, seed: int = 7) -> pd.DataFrame:
    """Anything computed for event T must survive deleting every row from T on.

    For a sample of events: features from the full tables vs from tables cut to
    (events before T) + (T's own field and label). Stats: only seasons before
    T's. Any difference means a feature read the future."""
    t, s, o = legacy.tables()
    events = list_events(t, list(range(2016, 2027)))
    sample = events.sample(n_events, random_state=seed)
    full_rounds = build_rounds(t)
    rows = []
    for _, ev in sample.iterrows():
        end, tid = ev["ENDING_DATE"], ev["TOURNAMENT"]
        keep = (t["ENDING_DATE"] < end) | (t["TOURNAMENT"] == tid)
        tt = t[keep]
        ss = s[s["SEASON"] < ev["SEASON"]]
        oo = o[(o["ENDING_DATE"] < end) | (o["TOURNAMENT"] == tid)]
        a = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=full_rounds)
        b = build_event_rows(tt, ss, oo, ev, exclude_wd=True, rounds=build_rounds(tt))
        cols = [c for c in FEATURES if c in a.columns and c not in ("FINISH_PCT", "TOP_20")]
        a = a.set_index("PLAYER")[cols]
        b = b.set_index("PLAYER")[cols].reindex(a.index)
        diff = ~((a.isna() & b.isna()) | ((a - b).abs() <= 1e-9))
        rows.append({"event": tid, "end": str(end.date()), "players": len(a),
                     "cells_changed": int(diff.values.sum()),
                     "features_changed": ", ".join(c for c in cols if diff[c].any())})
    out = pd.DataFrame(rows)
    bad = out[out["cells_changed"] > 0]
    print(f"{len(out)} events, {out['players'].sum():,} golfer-rows: "
          + ("NO feature changed when the future was removed." if bad.empty
             else f"{len(bad)} events CHANGED:\n{bad.to_string(index=False)}"))
    return out


def sg_truncation(n_events: int = 40, seed: int = 7) -> pd.DataFrame:
    """sg_form's ratings for event T, recomputed from tables holding only events
    (Tour and Korn Ferry) that ended before T started, must equal the stored row.

    The negative control plants a leak: ratings as of a week later, which see T's
    own rounds. It must change them, or the check could not see a leak at all."""
    from pga_api import sg
    with sqlite3.connect(build.DB_PATH) as con:
        tb = {k: pd.read_sql(f"SELECT * FROM {k}", con)
              for k in ("events", "rounds", "kft_events", "kft_rounds", "sg_rounds", "sg_form")}
    ev, kev = tb["events"], tb["kft_events"]
    full = sg.round_rows(ev, tb["rounds"], kev, tb["kft_rounds"], tb["sg_rounds"])
    pick = ev[(ev["kind"] == "stroke") & (ev["season"] >= 2016)].sample(n_events, random_state=seed)
    rows = []
    for e in pick.itertuples():
        start = pd.Timestamp(e.start_date)
        ev_t = ev[pd.to_datetime(ev["end_date"]) < start]
        kev_t = kev[pd.to_datetime(kev["end_date"]) < start]
        cut = sg.round_rows(ev_t, tb["rounds"][tb["rounds"]["tournament_id"].isin(ev_t["tournament_id"])],
                            kev_t, tb["kft_rounds"][tb["kft_rounds"]["tournament_id"].isin(kev_t["tournament_id"])],
                            tb["sg_rounds"][tb["sg_rounds"]["tournament_id"].isin(ev_t["tournament_id"])])
        stored = tb["sg_form"][tb["sg_form"]["tournament_id"] == e.tournament_id].set_index("player_id")[sg.FORM_COLS]
        diffs = {}
        for label, r in (("truncated", sg.ratings(cut, start)),
                         ("planted leak", sg.ratings(full, start + pd.Timedelta(days=7)))):
            r = r.reindex(stored.index)
            same = (r.isna() & stored.isna()) | ((r - stored).abs() <= 1e-9)
            diffs[label] = int((~same).values.sum())
        rows.append({"event": e.tournament_id, "start": str(start.date()), "players": len(stored),
                     "changed_when_truncated": diffs["truncated"],
                     "changed_by_planted_leak": diffs["planted leak"]})
    out = pd.DataFrame(rows)
    bad, blind = out[out["changed_when_truncated"] > 0], out[out["changed_by_planted_leak"] == 0]
    print(f"{len(out)} events, {out['players'].sum():,} golfer-rows: "
          + ("NO rating changed when the future was removed." if bad.empty
             else f"{len(bad)} events CHANGED:\n{bad.to_string(index=False)}"))
    print("planted leak caught in every event." if blind.empty
          else f"planted leak MISSED in {len(blind)} events: the check is blind there.")
    return out


def _season_scores(t, s, o, seasons, test_seasons, variant: str = "stage7"):
    """The production model (utils.model.train_and_score's arms) replayed season
    by season, trained only on earlier seasons. -> one row per test golfer.
    stage7 reads sg_form's point-in-time ratings, as pga_api.model.training does."""
    from scipy.stats import rankdata
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.isotonic import IsotonicRegression
    from utils.features import feature_columns, normalize

    rounds = build_rounds(t)
    events = list_events(t, sorted(set(seasons) | set(test_seasons)))
    form = None
    if variant == "stage7":
        with sqlite3.connect(build.DB_PATH) as con:
            form = (pd.read_sql("SELECT * FROM sg_form", con).drop(columns="as_of")
                    .rename(columns={"tournament_id": "TOURNAMENT", "player_id": "PLAYER"}))
    cache = {}
    for _, ev in events.iterrows():
        rows = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
        if form is not None:
            rows = rows.merge(form, on=["TOURNAMENT", "PLAYER"], how="left")
        cache[(ev["TOURNAMENT"], ev["ENDING_DATE"])] = rows
    out = []
    for season in test_seasons:
        train = pd.concat([cache[(e.TOURNAMENT, e.ENDING_DATE)] for e in
                           events[events["SEASON"] < season].itertuples()], ignore_index=True)
        tests = events[events["SEASON"] == season]
        test = pd.concat([cache[(e.TOURNAMENT, e.ENDING_DATE)] for e in tests.itertuples()],
                         ignore_index=True)
        train_n, test_n = normalize(train.copy(), test.copy())
        f = feature_columns(train_n, include_field_size=True, variant=variant)
        reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                    random_state=42, n_jobs=-1).fit(train_n[f], train_n["FINISH_PCT"])
        test_n["MODEL_SCORE"] = 1.0 - reg.predict(test_n[f])
        clf = CalibratedClassifierCV(RandomForestClassifier(
            n_estimators=500, max_depth=8, min_samples_leaf=10, class_weight="balanced_subsample",
            random_state=42, n_jobs=-1), method="isotonic", cv=3).fit(train_n[f], train_n["TOP_20"])
        iso = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(
            train_n["ODDS_SHARE"] * train_n["FIELD_SIZE"], train_n["TOP_20"])
        test_n["P_TOP20"] = (clf.predict_proba(test_n[f])[:, 1]
                             + iso.predict(test_n["ODDS_SHARE"] * test_n["FIELD_SIZE"])) / 2
        g = test_n.groupby(["TOURNAMENT", "ENDING_DATE"])
        test_n["SCORE"] = (g["MODEL_SCORE"].rank() + g["ODDS_SHARE"].rank()) / 2
        test_n["SEASON_TEST"] = season
        out.append(test_n)
        print(f"  season {season}: trained on {len(train_n):,} rows, scored {len(test_n):,}")
    return pd.concat(out, ignore_index=True)


def odds_sources(test_seasons=(2024, 2025)) -> dict:
    """golfodds.com against the Tour's FanDuel feed on the events both priced:
    how far apart their odds shares are, how much of each field each prices,
    and the production model's forward record with each as its market input
    (FanDuel from 2024 on, golfodds before, as the switch would leave it)."""
    import sys
    from scipy.stats import spearmanr
    from pga_api import odds as api_odds
    sys.path.insert(0, str(EXPERIMENTS))
    from metrics import score_event
    from utils.features import add_market_share

    t, s, o = legacy.tables()
    fd = api_odds.history()
    fd = fd.dropna(subset=["fraction"]).drop_duplicates(["tournament_id", "player_id"])
    ends = t.drop_duplicates("TOURNAMENT").set_index("TOURNAMENT")
    o_fd = pd.DataFrame({"SEASON": fd["tournament_id"].map(ends["SEASON"]),
                         "TOURNAMENT": fd["tournament_id"],
                         "ENDING_DATE": fd["tournament_id"].map(ends["ENDING_DATE"]),
                         "PLAYER": fd["player_id"], "VEGAS_ODDS": fd["fraction"]}).dropna(subset=["SEASON"])
    fd_events = set(o_fd["TOURNAMENT"])

    # 1. agreement, event by event, over each event's actual field
    rows = []
    for tid in sorted(fd_events & set(o["TOURNAMENT"])):
        field = t.loc[t["TOURNAMENT"] == tid, ["PLAYER"]]
        a = add_market_share(field.merge(o[o["TOURNAMENT"] == tid][["PLAYER", "VEGAS_ODDS"]], how="left"))
        b = add_market_share(field.merge(o_fd[o_fd["TOURNAMENT"] == tid][["PLAYER", "VEGAS_ODDS"]], how="left"))
        rows.append({"tournament_id": tid, "field": len(field),
                     "golfodds_priced": a["VEGAS_ODDS"].notna().mean(),
                     "fanduel_priced": b["VEGAS_ODDS"].notna().mean(),
                     "spearman": spearmanr(a["ODDS_SHARE"], b["ODDS_SHARE"]).statistic,
                     "mean_abs_share_diff": (a["ODDS_SHARE"] - b["ODDS_SHARE"]).abs().mean(),
                     "top10_overlap": len(set(a.nlargest(10, "ODDS_SHARE")["PLAYER"])
                                          & set(b.nlargest(10, "ODDS_SHARE")["PLAYER"])) / 10})
    agree = pd.DataFrame(rows)
    print(f"events priced by both: {len(agree)}")
    print(agree[["golfodds_priced", "fanduel_priced", "spearman", "top10_overlap"]]
          .describe().loc[["mean", "min", "50%"]].round(3).to_string())

    # 2. the forward record with each market input
    o_switch = pd.concat([o[~o["TOURNAMENT"].isin(fd_events)], o_fd], ignore_index=True)
    out = {}
    for label, oo in (("golfodds", o), ("FanDuel from 2024", o_switch)):
        print(f"{label}:")
        sc = _season_scores(t, s, oo, range(2016, max(test_seasons) + 1), test_seasons)
        res = []
        for tid, g in sc.groupby("TOURNAMENT"):
            for arm in ("SCORE", "P_TOP20"):
                res.append({"arm": arm, "tournament_id": tid,
                            **score_event(g, g[arm].to_numpy(), is_prob=(arm == "P_TOP20"))})
            res.append({"arm": "odds only", "tournament_id": tid,
                        **score_event(g, -g["VEGAS_ODDS"].fillna(1000).clip(upper=1000).to_numpy())})
        out[label] = pd.DataFrame(res).assign(market=label)
    res = pd.concat(out.values(), ignore_index=True)
    res = res[res["tournament_id"].isin(fd_events)]
    summ = res.groupby(["arm", "market"]).agg(events=("hits15", "size"), hits15=("hits15", "mean"),
                                              auc=("auc", "mean")).round(4)
    print(f"\n=== {sorted(test_seasons)} events FanDuel priced ===")
    print(summ.to_string())
    return {"agreement": agree, "forward": res, "summary": summ}


def dry_run(tournament_id: str = "R2026557", market: str = "golfodds") -> pd.DataFrame:
    """A finished week replayed through the notebook's own steps, as of before
    it started, next to the forecast logged that week (pga.db's predictions:
    the old notebook's log up to Bank of Utah 2026), both graded on the result.
    Writes nothing (no export, no log).

    market: 'golfodds' (the board saved that week) or 'fanduel' (the Tour's
    feed an hour before the first tee)."""
    from pga_api import model, odds as api_odds, weekly
    from pga_api.model import train_and_score

    week = weekly.this_week(pick=tournament_id)
    dk = weekly.prices(week)
    train, ctx = model.training(as_of=week.end_date)
    with sqlite3.connect(build.DB_PATH) as con:
        if market == "golfodds":
            board = pd.read_sql("SELECT player_id, decimal_minus_one AS VEGAS_ODDS FROM odds "
                                "WHERE tournament_id = ?", con, params=(tournament_id,))
        else:
            b = api_odds.fetch(tournament_id, api_odds.pre_event_time(tournament_id, week.start_date))
            board = b.rename(columns={"fraction": "VEGAS_ODDS"})[["player_id", "VEGAS_ODDS"]]
        res = pd.read_sql("SELECT player_id, position, finish_rank FROM results WHERE tournament_id = ?",
                          con, params=(tournament_id,))
        old = pd.read_sql("SELECT * FROM predictions WHERE tournament_id = ?", con, params=(tournament_id,))
    rows = model.week_rows(ctx, week, dk, board)
    scored, _ = train_and_score(train, rows)
    new = scored[["PLAYER", "P_TOP20", "SCORE"]].rename(columns={"PLAYER": "player_id"})

    both = new.merge(old[["player_id", "p_top20"]].rename(columns={"p_top20": "P_TOP20_old"}),
                     on="player_id", how="outer").merge(res, on="player_id", how="left")
    names = model.display_names()
    both.insert(0, "PLAYER", both["player_id"].map(names))
    top_new = set(both.nlargest(15, "P_TOP20")["player_id"])
    top_old = set(both.nlargest(15, "P_TOP20_old")["player_id"])
    hits = lambda ids: int((both[both["player_id"].isin(ids)]["finish_rank"] <= 20).sum())
    from scipy.stats import spearmanr
    ok = both.dropna(subset=["P_TOP20", "P_TOP20_old"])
    print(f"\n{week.name}: {len(new)} scored now ({market} odds), "
          f"{len(old)} logged that week")
    print(f"  rank agreement between the two (Spearman): {spearmanr(ok['P_TOP20'], ok['P_TOP20_old']).statistic:.3f}")
    print(f"  top 15 in common: {len(top_new & top_old)} of 15")
    print(f"  top 15 who finished top 20:  now {hits(top_new)}   logged {hits(top_old)}")
    return both.sort_values("P_TOP20", ascending=False).reset_index(drop=True)


def forward_eval(test_seasons=(2021, 2022, 2023, 2024, 2025)) -> dict:
    """The production model's out-of-sample record: each test season scored by
    a model trained only on the seasons before it, with the forward tests'
    metrics (experiments/metrics.py: hits@15 = actual top-20s among the top 15
    by score, AUC for top 20, Brier), per event. About five minutes."""
    import sys
    sys.path.insert(0, str(EXPERIMENTS))
    from metrics import score_event

    t, s, o = legacy.tables()
    scored = _season_scores(t, s, o, range(2016, max(test_seasons)), test_seasons)
    rows = []
    for tid, g in scored.groupby("TOURNAMENT"):
        for arm in ("SCORE", "P_TOP20"):
            m = score_event(g, g[arm].to_numpy(), is_prob=(arm == "P_TOP20"))
            rows.append({"arm": arm, "tournament_id": tid, "season": int(g["SEASON_TEST"].iloc[0]), **m})
    res = pd.DataFrame(rows)
    summ = res.groupby("arm").agg(events=("hits15", "size"), hits15=("hits15", "mean"),
                                  auc=("auc", "mean"), brier=("brier", "mean")).round(4)
    by = res.groupby(["season", "arm"])["hits15"].mean().unstack().round(3)
    print(summ.to_string())
    print()
    print("hits@15 by season:")
    print(by.to_string())
    return {"results": res, "summary": summ, "by_season": by}
