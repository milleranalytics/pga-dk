"""Before/after: last season's stats and SG_FORM vs the point-in-time ratings in
pga.db's sg_form table (field-strength adjusted, Korn Ferry included).

The production model (utils.model.train_and_score's arms) replayed season by
season, trained only on earlier seasons, once per feature set; same metrics as
experiments/forward_eval.py, per event:

    .venv/Scripts/python.exe experiments/sg_form_eval.py

Feature sets (each a swap on the production 'stage6' set):
    base         production today
    categories   the 30 prior-season stat columns out; SGA_OTT/APP/ARG/PUTT/T2G in
    adj_form     SG_FORM, SG_ROUNDS_12M out; SGA_TOTAL, SGA_ROUNDS_12M in
    both         categories + adj_form
    both+owgr    both, keeping last season's OWGR and OWGR_RANK
    both_eb      both, with shrinkage at shrinkage_weights()' full level (LAMBDA_EB)
                 instead of SG_FORM's tested level (LAMBDA)
    both+sgform  both, with SG_FORM and SG_ROUNDS_12M added back
    both+owgr_now  both, plus the world ranking as it stood before each event
                 (pga.db's owgr table: through the last event that finished before it)

    .venv/Scripts/python.exe experiments/sg_form_eval.py --addback   # those two only
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "experiments"))

from forward_eval import score_event  # noqa: E402
from pga_api import build, legacy, sg  # noqa: E402
from pga_api.sg import FORM_COLS  # noqa: E402
from utils.features import build_event_rows, build_rounds, feature_columns, list_events, normalize  # noqa: E402

SEASONS = range(2016, 2027)
TEST_SEASONS = [2021, 2022, 2023, 2024, 2025, 2026]
STAT_COLS = legacy.STATS + [f"{c}_RANK" for c in legacy.STATS]
CATS = ["SGA_OTT", "SGA_APP", "SGA_ARG", "SGA_PUTT", "SGA_T2G"]
EB_COLS = [f"{c}_EB" for c in FORM_COLS]
NEW_COLS = FORM_COLS + EB_COLS
OUT = ROOT / "experiments" / "sg_form_eval_results.csv"


def feature_sets(base: list) -> dict:
    no_stats = [c for c in base if c not in STAT_COLS]
    swap_form = lambda cols: [c for c in cols if c not in ("SG_FORM", "SG_ROUNDS_12M")] + \
        ["SGA_TOTAL", "SGA_ROUNDS_12M"]
    both = swap_form(no_stats) + CATS
    return {
        "base": base,
        "categories": no_stats + CATS,
        "adj_form": swap_form(base),
        "both": both,
        "both+owgr": both + ["OWGR", "OWGR_RANK"],
        "both_eb": [f"{c}_EB" if c in FORM_COLS else c for c in both],
        "both+sgform": both + ["SG_FORM", "SG_ROUNDS_12M"],
        "both+owgr_now": both + ["OWGR_NOW", "OWGR_NOW_RANK"],
    }


def fill_sga(train: pd.DataFrame, test: pd.DataFrame) -> None:
    """Never rated -> below average, the SG_FORM rule: the training rows' 25th
    percentile. Fit on train, applied to both. Rounds: 0."""
    for c in NEW_COLS:
        if c.startswith("SGA_ROUNDS_12M"):
            fill = 0.0
        else:
            fill = train[c].quantile(0.25)
        train[c] = train[c].fillna(fill)
        test[c] = test[c].fillna(fill)


def event_rows() -> pd.DataFrame:
    t, s, o = legacy.tables()
    rounds = build_rounds(t)
    events = list_events(t, list(SEASONS))
    rows = pd.concat([build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
                      for _, ev in events.iterrows()], ignore_index=True)
    with sqlite3.connect(build.DB_PATH) as con:
        form = pd.read_sql("SELECT * FROM sg_form", con)
        frames = {k: pd.read_sql(f"SELECT * FROM {k}", con) for k in
                  ("events", "rounds", "kft_events", "kft_rounds", "sg_rounds", "results", "field")}
    eb = sg.form_table(frames, lam=sg.LAMBDA_EB).rename(columns=dict(zip(FORM_COLS, EB_COLS)))
    with sqlite3.connect(build.DB_PATH) as con:
        ow = pd.read_sql("SELECT tournament_id, player_id, owgr_rank AS OWGR_NOW_RANK, "
                         "owgr_points AS OWGR_NOW FROM owgr", con)
    rows = rows.merge(ow.rename(columns={"tournament_id": "TOURNAMENT", "player_id": "PLAYER"}),
                      on=["TOURNAMENT", "PLAYER"], how="left")
    # Unranked at an event that has a snapshot: 1000 and no points, as OWGR_RANK
    # is filled. An event with no snapshot in reach (most of 2026) stays NaN, so
    # normalize() fills it with the training mean rather than calling everyone
    # unranked.
    snap = rows["TOURNAMENT"].isin(ow["tournament_id"])
    rows.loc[snap, "OWGR_NOW_RANK"] = rows.loc[snap, "OWGR_NOW_RANK"].fillna(1000)
    rows.loc[snap, "OWGR_NOW"] = rows.loc[snap, "OWGR_NOW"].fillna(0.0)
    for f in (form, eb):
        rows = rows.merge(f.rename(columns={"tournament_id": "TOURNAMENT", "player_id": "PLAYER"})
                          .drop(columns="as_of"), on=["TOURNAMENT", "PLAYER"], how="left")
    print(f"{len(rows):,} golfer-events, {rows['TOURNAMENT'].nunique()} events; "
          f"rated by sg_form: total {rows['SGA_TOTAL'].notna().mean():.1%}, "
          f"categories {rows['SGA_APP'].notna().mean():.1%}, "
          f"SG_FORM {rows['SG_FORM'].notna().mean():.1%}")
    return rows


def season_scores(rows: pd.DataFrame, fsets: list[str]) -> pd.DataFrame:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.isotonic import IsotonicRegression

    out, importances = [], {}
    for season in TEST_SEASONS:
        train = rows[rows["SEASON"] < season].copy()
        test = rows[rows["SEASON"] == season].copy()
        if test.empty:
            continue
        fill_sga(train, test)             # before normalize(), which mean-fills every gap
        train_n, test_n = normalize(train, test)
        # feature_columns() takes every numeric column, the new ones included:
        # production's set is what it picks minus sg_form's columns.
        prod = [c for c in feature_columns(train_n, include_field_size=True, variant="stage6")
                if c not in NEW_COLS + ["OWGR_NOW", "OWGR_NOW_RANK"]]
        sets = feature_sets(prod)
        iso = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(
            train_n["ODDS_SHARE"] * train_n["FIELD_SIZE"], train_n["TOP_20"])
        p_market = iso.predict(test_n["ODDS_SHARE"] * test_n["FIELD_SIZE"])
        for name in fsets:
            f = sets[name]
            assert not train_n[f].isna().any().any() and not test_n[f].isna().any().any(), name
            reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                        random_state=42, n_jobs=-1).fit(train_n[f], train_n["FINISH_PCT"])
            clf = CalibratedClassifierCV(RandomForestClassifier(
                n_estimators=500, max_depth=8, min_samples_leaf=10, class_weight="balanced_subsample",
                random_state=42, n_jobs=-1), method="isotonic", cv=3).fit(train_n[f], train_n["TOP_20"])
            sc = test_n[["TOURNAMENT", "ENDING_DATE", "SEASON", "PLAYER", "TOP_20", "FINAL_POS",
                         "ODDS_SHARE"]].copy()
            sc["PGA_ROUNDS_12M"] = test["SG_ROUNDS_12M"].fillna(0).values   # before any fill
            sc["MODEL_SCORE"] = 1.0 - reg.predict(test_n[f])
            sc["P_TOP20"] = (clf.predict_proba(test_n[f])[:, 1] + p_market) / 2
            g = sc.groupby("TOURNAMENT")
            sc["SCORE"] = (g["MODEL_SCORE"].rank() + g["ODDS_SHARE"].rank()) / 2
            sc["fset"] = name
            out.append(sc)
            if season == TEST_SEASONS[-1]:
                importances[name] = pd.Series(reg.feature_importances_, index=f)
        print(f"  season {season}: trained on {len(train_n):,} rows, scored {len(test_n):,}", flush=True)
    return pd.concat(out, ignore_index=True), importances


def main(fsets=("base", "categories", "adj_form", "both", "both+owgr", "both_eb"), save_scores=None):
    rows = event_rows()
    sc, imp = season_scores(rows, list(fsets))
    if save_scores:
        sc.to_csv(save_scores, index=False)
    res = []
    for (name, tid), g in sc.groupby(["fset", "TOURNAMENT"]):
        for arm in ("P_TOP20", "SCORE"):
            m = score_event(g, g[arm].to_numpy(), is_prob=(arm == "P_TOP20"))
            res.append({"fset": name, "arm": arm, "tournament_id": tid,
                        "season": int(g["SEASON"].iloc[0]), **m})
    res = pd.DataFrame(res)
    # A run of some feature sets replaces those in the saved results and keeps
    # the rest: every run is deterministic (fixed seeds, same rows), so base
    # from an earlier run is the same base.
    if OUT.exists():
        old = pd.read_csv(OUT)
        res = pd.concat([old[~old["fset"].isin(res["fset"].unique())], res], ignore_index=True)
    res.to_csv(OUT, index=False)
    report(res)
    print("\nfeature importance, last test season's regressor (top 12):")
    for name in ("base", "both"):
        if name in imp:
            print(f"  {name}: " + ", ".join(f"{k} {v:.3f}" for k, v in imp[name].nlargest(12).items()))
    return res, imp


def report(res: pd.DataFrame) -> None:
    for arm in ("P_TOP20", "SCORE"):
        r = res[res["arm"] == arm]
        summ = r.groupby("fset").agg(events=("hits15", "size"), hits15=("hits15", "mean"),
                                     auc=("auc", "mean"), brier=("brier", "mean"))
        base = r[r["fset"] == "base"].set_index("tournament_id")
        for name in summ.index:
            d = r[r["fset"] == name].set_index("tournament_id")
            dh = (d["hits15"] - base["hits15"]).dropna()
            da = (d["auc"] - base["auc"]).dropna()
            summ.loc[name, "d_hits15"] = dh.mean()
            summ.loc[name, "se_d_hits15"] = dh.std(ddof=1) / np.sqrt(len(dh)) if name != "base" else 0
            summ.loc[name, "d_auc"] = da.mean()
            summ.loc[name, "se_d_auc"] = da.std(ddof=1) / np.sqrt(len(da)) if name != "base" else 0
            if arm == "P_TOP20":
                db = (d["brier"] - base["brier"]).dropna()
                summ.loc[name, "d_brier_e4"] = db.mean() * 1e4
                summ.loc[name, "t_brier"] = db.mean() / (db.std(ddof=1) / np.sqrt(len(db))) if name != "base" else 0
        print(f"\n=== {arm}, test seasons {sorted(r['season'].unique())} ===")
        print(summ.round(4).to_string())
        by = r.groupby(["season", "fset"])["hits15"].mean().unstack().round(3)
        print(f"\n{arm} hits@15 by season:")
        print(by.to_string())
        byauc = r.groupby(["season", "fset"])["auc"].mean().unstack()
        bybr = r.groupby(["season", "fset"])["brier"].mean().unstack()
        wins = {n: f"{int((by[n] > by['base']).sum())}/{len(by)} hits, "
                   f"{int((byauc[n] > byauc['base']).sum())}/{len(by)} AUC"
                   + (f", {int((bybr[n] < bybr['base']).sum())}/{len(by)} Brier" if arm == "P_TOP20" else "")
                for n in by.columns if n != "base"}
        print("seasons beating base: " + "; ".join(f"{k}: {v}" for k, v in wins.items()))


if __name__ == "__main__":
    if sys.argv[1:] == ["--addback"]:
        main(("both+sgform", "both+owgr_now"))
    elif sys.argv[1:] == ["--thin"]:
        # base vs both, golfer by golfer, split by how many Tour rounds each had
        main(("base", "both"), save_scores=ROOT / "experiments" / "sg_form_eval_scores.csv")
    else:
        main()
