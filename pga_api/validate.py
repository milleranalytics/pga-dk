"""Checks that the API pipeline computes what the golf.db pipeline does, or
better, and reads nothing from the future. Each prints its reading; none writes.

    from pga_api import validate
    validate.feature_parity()     # same features, golfer by golfer
    validate.truncation()         # a feature for event T is unchanged when data from T on is removed
    validate.forward_eval()       # the production model's out-of-sample record, both pipelines
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from pga_api import build, compare, legacy
from pga_api.identity import Resolver
from utils.features import build_event_rows, build_rounds, list_events, load_tables

FEATURES = ["CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density", "CONSECUTIVE_CUTS",
            "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK", "COURSE_HISTORY", "adj_ch",
            "PCT_CH_SHRUNK", "SG_FORM", "SG_ROUNDS_12M", "SG_CH_SHRUNK", "VEGAS_ODDS",
            "ODDS_SHARE", "SGTTG", "SGP", "DRIVING_DISTANCE", "OWGR", "OWGR_RANK",
            "FINISH_PCT", "TOP_20", "FIELD_SIZE"]


def _rows(t, s, o, events, rounds) -> pd.DataFrame:
    frames = [build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
              for _, ev in events.iterrows()]
    return pd.concat([f for f in frames if not f.empty], ignore_index=True)


def _golf_player_ids(gt: pd.DataFrame) -> pd.Series:
    """golf.db (TOURN_ID, PLAYER) -> player_id, resolved within each event's field."""
    with sqlite3.connect(build.DB_PATH) as con:
        R = Resolver(pd.read_sql("SELECT * FROM events", con), pd.read_sql("SELECT * FROM results", con),
                     pd.read_sql("SELECT * FROM players", con))
    pairs = gt[["TOURN_ID", "PLAYER"]].drop_duplicates()
    ids = [R.resolve(p, t)[0] for t, p in zip(pairs["TOURN_ID"], pairs["PLAYER"])]
    return pd.Series(ids, index=pd.MultiIndex.from_frame(pairs))


def feature_parity(seasons=range(2016, 2027), tol: float = 1e-6) -> dict:
    """Every feature, both pipelines, joined on (event id, player id).

    -> {"summary": agreement per feature, "joined": the rows, "coverage": rows each side}"""
    gt, gs, go = load_tables(str(compare.GOLF_DB))
    grounds = build_rounds(gt)
    gev = list_events(gt, list(seasons))
    old = _rows(gt, gs, go, gev, grounds)
    ids = _golf_player_ids(gt)
    tid = gt.drop_duplicates(["TOURNAMENT", "ENDING_DATE"]).set_index(["TOURNAMENT", "ENDING_DATE"])["TOURN_ID"]
    old["tournament_id"] = [tid[(a, b)] for a, b in zip(old["TOURNAMENT"], old["ENDING_DATE"])]
    old["player_id"] = [ids.get((t, p)) for t, p in zip(old["tournament_id"], old["PLAYER"])]

    t, s, o = legacy.tables()
    rounds = build_rounds(t)
    new = _rows(t, s, o, list_events(t, list(seasons)), rounds)
    new = new.rename(columns={"TOURNAMENT": "tournament_id", "PLAYER": "player_id"})

    j = old.merge(new, on=["tournament_id", "player_id"], how="outer",
                  suffixes=("_old", "_new"), indicator=True)
    both = j[j["_merge"] == "both"]
    rows = []
    for c in FEATURES:
        a, b = both[f"{c}_old"].astype(float), both[f"{c}_new"].astype(float)
        same = (a.isna() & b.isna()) | ((a - b).abs() <= tol)
        rows.append({"feature": c, "rows": len(both), "identical": round(float(same.mean()), 4),
                     "differ": int((~same).sum()),
                     "median_abs_diff": float((a - b).abs()[~same].median()) if (~same).any() else 0.0})
    summary = pd.DataFrame(rows)
    coverage = {"golf.db rows": len(old), "pga.db rows": len(new),
                "matched": len(both), "golf.db only": int((j["_merge"] == "left_only").sum()),
                "pga.db only": int((j["_merge"] == "right_only").sum())}
    print(coverage)
    print(summary.to_string(index=False))
    return {"summary": summary, "joined": j, "coverage": coverage}


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


def _season_scores(t, s, o, seasons, test_seasons):
    """The production model (utils.model.train_and_score's arms) replayed season
    by season, trained only on earlier seasons. -> one row per test golfer."""
    from scipy.stats import rankdata
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.isotonic import IsotonicRegression
    from utils.features import feature_columns, normalize

    rounds = build_rounds(t)
    events = list_events(t, list(seasons))
    cache = {}
    for _, ev in events.iterrows():
        cache[(ev["TOURNAMENT"], ev["ENDING_DATE"])] = build_event_rows(
            t, s, o, ev, exclude_wd=True, rounds=rounds)
    out = []
    for season in test_seasons:
        train = pd.concat([cache[(e.TOURNAMENT, e.ENDING_DATE)] for e in
                           events[events["SEASON"] < season].itertuples()], ignore_index=True)
        tests = events[events["SEASON"] == season]
        test = pd.concat([cache[(e.TOURNAMENT, e.ENDING_DATE)] for e in tests.itertuples()],
                         ignore_index=True)
        train_n, test_n = normalize(train.copy(), test.copy())
        f = feature_columns(train_n, include_field_size=True, variant="stage6")
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
    sys.path.insert(0, str(compare.DATA.parent / "experiments"))
    from forward_eval import score_event
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
    """A finished week replayed through the API notebook's own steps, as of
    before it started, next to what the golf.db notebook logged that week, both
    graded on the result. Writes nothing (no export, no log).

    market: 'golfodds' (the board saved that week) or 'fanduel' (the Tour's
    feed an hour before the first tee)."""
    from pga_api import model, odds as api_odds, weekly
    from utils.model import train_and_score

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
    print(f"\n{week.name}: {len(new)} scored by the API notebook ({market} odds), "
          f"{len(old)} logged by the golf.db notebook")
    print(f"  rank agreement between the two (Spearman): {spearmanr(ok['P_TOP20'], ok['P_TOP20_old']).statistic:.3f}")
    print(f"  top 15 in common: {len(top_new & top_old)} of 15")
    print(f"  top 15 who finished top 20:  API notebook {hits(top_new)}   golf.db notebook {hits(top_old)}")
    return both.sort_values("P_TOP20", ascending=False).reset_index(drop=True)


def forward_eval(test_seasons=(2021, 2022, 2023, 2024, 2025)) -> dict:
    """Both pipelines through the same production model and the same metrics as
    experiments/forward_eval.py (hits@15 = actual top-20s among the top 15 by
    score, AUC for top 20), per event. Compared on the events both have, and
    on everything the API pipeline has."""
    import sys
    sys.path.insert(0, str(compare.DATA.parent / "experiments"))
    from forward_eval import score_event

    print("golf.db pipeline:")
    gt, gs, go = load_tables(str(compare.GOLF_DB))
    old = _season_scores(gt, gs, go, range(2016, 2026), test_seasons)
    tid = gt.drop_duplicates(["TOURNAMENT", "ENDING_DATE"]).set_index(["TOURNAMENT", "ENDING_DATE"])["TOURN_ID"]
    old["tournament_id"] = [tid[(a, b)] for a, b in zip(old["TOURNAMENT"], old["ENDING_DATE"])]
    print("pga.db pipeline:")
    t, s, o = legacy.tables()
    new = _season_scores(t, s, o, range(2016, 2026), test_seasons)
    new["tournament_id"] = new["TOURNAMENT"]

    rows = []
    for label, df in (("golf.db", old), ("pga.db", new)):
        for tid_, g in df.groupby("tournament_id"):
            for arm in ("SCORE", "P_TOP20"):
                m = score_event(g, g[arm].to_numpy(), is_prob=(arm == "P_TOP20"))
                rows.append({"pipeline": label, "arm": arm, "tournament_id": tid_,
                             "season": int(g["SEASON_TEST"].iloc[0]), **m})
    res = pd.DataFrame(rows)
    common = set(res.loc[res["pipeline"] == "golf.db", "tournament_id"]) & \
        set(res.loc[res["pipeline"] == "pga.db", "tournament_id"])
    summ = lambda d: d.groupby(["arm", "pipeline"]).agg(
        events=("hits15", "size"), hits15=("hits15", "mean"), auc=("auc", "mean"),
        brier=("brier", "mean")).round(4)
    print(f"\n=== events both pipelines have ({len(common)}) ===")
    both = summ(res[res["tournament_id"].isin(common)])
    print(both.to_string())
    print("\n=== by season, events both have ===")
    by = (res[res["tournament_id"].isin(common)].groupby(["season", "arm", "pipeline"])
          ["hits15"].mean().unstack().round(3))
    print(by.to_string())
    print("\n=== everything the pga.db pipeline scores (adds events golf.db never had) ===")
    print(summ(res[res["pipeline"] == "pga.db"]).to_string())
    return {"results": res, "common": both, "by_season": by}
