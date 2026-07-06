# experiments/forward_eval.py
# Forward-chaining evaluation: baseline (per-event training, current notebook
# methodology) vs pooled (train on all prior-season events).
#
# For every test event in TEST_SEASONS:
#   baseline: train only on prior editions of the same course/tournament
#             (>= MIN_PRIOR_EDITIONS required), exactly like the notebook.
#   pooled:   one model per test season, trained on ALL events from seasons
#             strictly before it (W/D rows excluded from rolling windows,
#             FIELD_SIZE added as a feature).
#   odds:     rank by VEGAS_ODDS alone (market reference, no model).
#
# Metrics per test event: hits@15 (actual top-20s among model's top 15),
# per-event AUC, Spearman of score vs FINAL_POS.

import time
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.features import (load_tables, list_events, build_event_rows, normalize,
                            feature_columns, build_rounds, META_COLS)

DB = "data/golf.db"
ALL_SEASONS = list(range(2016, 2026))
TEST_SEASONS = [2021, 2022, 2023, 2024, 2025]
MIN_PRIOR_EDITIONS = 4
RNG = 42


def make_pipeline(y=None):
    rf = RandomForestClassifier(
        n_estimators=500, max_depth=8, min_samples_leaf=10,
        random_state=RNG, n_jobs=-1)
    # SMOTE(0.5) requires the minority/majority ratio to be below 0.5; small-field
    # events (e.g. Sentry) can have top-20 rates above that. Skip resampling there.
    if y is not None:
        pos = int(np.sum(y)); neg = len(y) - pos
        if min(pos, neg) / max(pos, neg) >= 0.5 or min(pos, neg) < 3:
            return Pipeline([("scaler", StandardScaler()), ("rf", rf)])
    return Pipeline([
        ("scaler", StandardScaler()),
        ("smote", SMOTE(sampling_strategy=0.5, k_neighbors=2, random_state=RNG)),
        ("under", RandomUnderSampler(sampling_strategy=0.5, random_state=RNG)),
        ("rf", rf),
    ])


def event_key(ev):
    return (ev["TOURNAMENT"], str(pd.Timestamp(ev["ENDING_DATE"]).date()))


def score_event(test_df, score, label_col="TOP_20", is_prob=None):
    """Per-event metrics for a score where higher = better player."""
    y = test_df[label_col].to_numpy()
    score = np.asarray(score, dtype=float)
    n_pos = int(y.sum())
    # Fair tie-breaking: rows arrive in finish order (tournaments table is
    # stored by POS), so a plain argsort resolves tied scores toward the
    # actual result — inflating hits@15 for tie-heavy scores like raw odds.
    # Average hits over random permutations instead.
    rng = np.random.default_rng(len(y) * 7919 + n_pos)
    hits = []
    for _ in range(20):
        perm = rng.permutation(len(score))
        order = perm[np.argsort(-score[perm], kind="stable")]
        hits.append(int(y[order[:15]].sum()))
    hits15 = float(np.mean(hits))
    auc = roc_auc_score(y, score) if 0 < n_pos < len(y) else np.nan
    rho = spearmanr(score, test_df["FINAL_POS"]).statistic  # want negative
    if is_prob is None:
        is_prob = score.min() >= 0 and score.max() <= 1
    return {"hits15": hits15, "auc": auc, "spearman_vs_pos": rho, "n_pos": n_pos,
            "field": len(test_df),
            "brier": float(np.mean((score - y) ** 2)) if is_prob else np.nan,
            "prob_sum": float(score.sum()) if is_prob else np.nan}


def main():
    t0 = time.time()
    t, s, o = load_tables(DB)
    rounds = build_rounds(t)
    events = list_events(t, ALL_SEASONS)
    print(f"{len(events)} events {ALL_SEASONS[0]}-{ALL_SEASONS[-1]}")

    # ---- Cache per-event rows for both feature variants ----
    cache_base, cache_wd = {}, {}
    for _, ev in events.iterrows():
        k = event_key(ev)
        cache_base[k] = build_event_rows(t, s, o, ev, exclude_wd=False, rounds=rounds)
        cache_wd[k] = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
    print(f"event rows cached in {time.time()-t0:.0f}s")

    events["KEY"] = [event_key(ev) for _, ev in events.iterrows()]
    test_events = events[events["SEASON"].isin(TEST_SEASONS)]

    results = []
    prediction_dump = []

    # ---- Baseline arm: per-event models ----
    for _, ev in test_events.iterrows():
        prior = events[
            ((events["COURSE"] == ev["COURSE"]) | (events["TOURNAMENT"] == ev["TOURNAMENT"]))
            & (events["SEASON"] < ev["SEASON"])
        ]
        if len(prior) < MIN_PRIOR_EDITIONS:
            continue
        train = pd.concat([cache_base[k] for k in prior["KEY"]], ignore_index=True)
        test = cache_base[ev["KEY"]].copy()
        if test.empty or train["TOP_20"].nunique() < 2:
            continue
        train, test = normalize(train.copy(), test)
        fcols = feature_columns(train, include_field_size=False)
        pipe = make_pipeline(train["TOP_20"].to_numpy())
        pipe.fit(train[fcols], train["TOP_20"])
        prob = pipe.predict_proba(test[fcols])[:, 1]
        m = score_event(test, prob)
        m.update(arm="baseline", SEASON=ev["SEASON"], TOURNAMENT=ev["TOURNAMENT"],
                 ENDING_DATE=str(pd.Timestamp(ev["ENDING_DATE"]).date()),
                 n_train=len(train))
        results.append(m)

    print(f"baseline done {time.time()-t0:.0f}s ({sum(1 for r in results if r['arm']=='baseline')} events)")

    # ---- Pooled arm: one model per test season ----
    for season in TEST_SEASONS:
        train_keys = events[events["SEASON"] < season]["KEY"]
        train = pd.concat([cache_wd[k] for k in train_keys], ignore_index=True)
        season_tests = events[events["SEASON"] == season]
        # Normalize once per season: fit fills on the pooled training set
        train_n = train.copy()
        tests_n = {k: cache_wd[k].copy() for k in season_tests["KEY"] if not cache_wd[k].empty}
        merged_test = pd.concat(tests_n.values(), keys=tests_n.keys())
        train_n, merged_test = normalize(train_n, merged_test.reset_index(drop=True))
        merged_test["KEY2"] = list(np.repeat(
            [str(k) for k in tests_n.keys()], [len(v) for v in tests_n.values()]))

        y_tr = train_n["TOP_20"].to_numpy()

        # Arm: pooled (legacy features, current model config)
        fcols = feature_columns(train_n, include_field_size=True, variant="legacy")
        pipe = make_pipeline(y_tr)
        pipe.fit(train_n[fcols], train_n["TOP_20"])
        merged_test["PROB"] = pipe.predict_proba(merged_test[fcols])[:, 1]

        # Arm: pooled_s2 (Stage 2 features: market share + shrunken percentiles)
        fcols2 = feature_columns(train_n, include_field_size=True, variant="stage2")
        pipe2 = make_pipeline(y_tr)
        pipe2.fit(train_n[fcols2], train_n["TOP_20"])
        merged_test["PROB_S2"] = pipe2.predict_proba(merged_test[fcols2])[:, 1]

        # Arm: pooled_s3 (Stage 2 features + no SMOTE + isotonic calibration)
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.ensemble import RandomForestRegressor
        from scipy.stats import rankdata

        def calibrated_rf():
            rf = RandomForestClassifier(
                n_estimators=500, max_depth=8, min_samples_leaf=10,
                class_weight="balanced_subsample", random_state=RNG, n_jobs=-1)
            return CalibratedClassifierCV(rf, method="isotonic", cv=3)

        pipe3 = calibrated_rf()
        pipe3.fit(train_n[fcols2], train_n["TOP_20"])
        merged_test["PROB_S3"] = pipe3.predict_proba(merged_test[fcols2])[:, 1]

        # Arm: pooled_s4 (Stage 4: + round-level strokes-gained form)
        fcols4 = feature_columns(train_n, include_field_size=True, variant="stage4")
        pipe4 = calibrated_rf()
        pipe4.fit(train_n[fcols4], train_n["TOP_20"])
        merged_test["PROB_S4"] = pipe4.predict_proba(merged_test[fcols4])[:, 1]

        # Arm: pooled_s5 (Stage 5: regression on finish percentile; 1 = best)
        reg5 = RandomForestRegressor(
            n_estimators=500, max_depth=8, min_samples_leaf=10,
            random_state=RNG, n_jobs=-1)
        reg5.fit(train_n[fcols4], train_n["FINISH_PCT"])
        merged_test["SCORE_S5"] = 1.0 - reg5.predict(merged_test[fcols4])

        # Feature importances for the final test season (odds vs SG question)
        if season == TEST_SEASONS[-1]:
            rf_imp = RandomForestClassifier(
                n_estimators=500, max_depth=8, min_samples_leaf=10,
                class_weight="balanced_subsample", random_state=RNG, n_jobs=-1)
            rf_imp.fit(train_n[fcols4], train_n["TOP_20"])
            imp = pd.Series(rf_imp.feature_importances_, index=fcols4).sort_values(ascending=False)
            print(f"\n=== Stage 4 feature importances (train < {season}) ===")
            print(imp.head(12).round(4).to_string())

        dump = merged_test[["KEY2", "PLAYER", "TOP_20", "FINAL_POS", "FINISH_PCT",
                            "ODDS_SHARE", "PROB_S3", "PROB_S4", "SCORE_S5"]].copy()
        dump["SEASON"] = season
        prediction_dump.append(dump)

        for _, ev in season_tests.iterrows():
            sub = merged_test[merged_test["KEY2"] == str(ev["KEY"])].copy()
            if sub.empty or sub["TOP_20"].nunique() < 1:
                continue
            # Blend: average of within-event ranks of model score and market share
            blend = (rankdata(sub["SCORE_S5"]) + rankdata(sub["ODDS_SHARE"])) / 2
            arms = [("pooled", sub["PROB"], None), ("pooled_s2", sub["PROB_S2"], None),
                    ("pooled_s3", sub["PROB_S3"], None), ("pooled_s4", sub["PROB_S4"], None),
                    ("pooled_s5", sub["SCORE_S5"], False), ("s5_blend", blend, False)]
            for arm, sc, is_prob in arms:
                m = score_event(sub, np.asarray(sc), is_prob=is_prob)
                m.update(arm=arm, SEASON=season, TOURNAMENT=ev["TOURNAMENT"],
                         ENDING_DATE=str(pd.Timestamp(ev["ENDING_DATE"]).date()),
                         n_train=len(train_n))
                results.append(m)
        print(f"pooled season {season} done {time.time()-t0:.0f}s (train n={len(train_n)})")

    # ---- Odds-only reference ----
    for _, ev in test_events.iterrows():
        test = cache_base[ev["KEY"]]
        if test.empty:
            continue
        odds = test["VEGAS_ODDS"].fillna(1000).clip(upper=1000)
        m = score_event(test, -odds.to_numpy())  # lower odds = better
        m.update(arm="odds_only", SEASON=ev["SEASON"], TOURNAMENT=ev["TOURNAMENT"],
                 ENDING_DATE=str(pd.Timestamp(ev["ENDING_DATE"]).date()), n_train=0)
        results.append(m)

    res = pd.DataFrame(results)
    import os
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forward_eval_results.csv")
    res.to_csv(out_path, index=False)
    if prediction_dump:
        pred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "predictions.csv")
        pd.concat(prediction_dump, ignore_index=True).to_csv(pred_path, index=False)

    # ---- Summary ----
    # Fair comparison: only events every arm scored
    common = None
    for arm in ["baseline", "pooled", "pooled_s2", "pooled_s3", "pooled_s4",
                "pooled_s5", "s5_blend", "odds_only"]:
        keys = set(map(tuple, res[res.arm == arm][["TOURNAMENT", "ENDING_DATE"]].values))
        common = keys if common is None else common & keys
    resc = res[res[["TOURNAMENT", "ENDING_DATE"]].apply(tuple, axis=1).isin(common)]

    print(f"\n=== Common test events: {len(common)} ===")
    summ = resc.groupby("arm").agg(
        events=("hits15", "count"),
        hits15_mean=("hits15", "mean"),
        auc_mean=("auc", "mean"),
        spearman=("spearman_vs_pos", "mean"),
        brier=("brier", "mean"),
        prob_sum=("prob_sum", "mean"),
        actual_top20=("n_pos", "mean"),
    ).round(3)
    print(summ.to_string())

    print("\n=== By season (common events) ===")
    print(resc.groupby(["SEASON", "arm"]).agg(
        events=("hits15", "count"), hits15=("hits15", "mean"), auc=("auc", "mean")
    ).round(3).to_string())

    print("\n=== Pooled on ALL test events (incl. those baseline can't score) ===")
    print(res[res.arm == "pooled"].groupby("SEASON").agg(
        events=("hits15", "count"), hits15=("hits15", "mean"), auc=("auc", "mean")
    ).round(3).to_string())

    print(f"\nTotal time: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
