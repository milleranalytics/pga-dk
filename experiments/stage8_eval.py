# experiments/stage8_eval.py
# Three candidate groups, tested the way stage 6 was: same forward chain as
# forward_eval.py (train on every event before the test season, one model per
# season, 2021-2025), same score_event() metrics, shipped s6 feature set as the
# control, one thing changed at a time.
#
#   +rest    DAYS_SINCE_LAST_START, STARTS_28D, STARTS_90D
#            Nothing in s6 knows when a player last teed it up. Inside a market
#            tier the effect is large and changes sign with player quality.
#   +field   FIELD_SG_MEAN, FIELD_SG_SD, SG_FORM_Z
#            TOP_20 means beating 80% of THIS field, but every skill feature is
#            absolute and FIELD_SIZE runs backwards as a strength proxy.
#   +counts  CH_ROUNDS, CH_EVENTS, TOTAL_EVENTS_PLAYED, HAS_STATS
#            Sample size behind each shrunken feature, so the model can tell an
#            imputed value from a measured average one.
#
# Reported model-alone and market-blended: the blend ships, but the model is
# currently near coin-flip conditional on market rank, so a feature that only
# helps the model is still worth seeing.

import time
import sys
import os
import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.ensemble import RandomForestRegressor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.features import (load_tables, list_events, build_event_rows, build_rounds,
                            normalize, feature_columns,
                            STAGE8_REST, STAGE8_FIELD, STAGE8_COUNTS)
from experiments.forward_eval import score_event, event_key, DB, ALL_SEASONS, TEST_SEASONS, RNG

CANDIDATES = {
    "s6": [],
    "+rest": STAGE8_REST,
    "+field": STAGE8_FIELD,
    "+counts": STAGE8_COUNTS,
    "+all": STAGE8_REST + STAGE8_FIELD + STAGE8_COUNTS,
}


def main():
    t0 = time.time()
    t, s, o = load_tables(DB)
    rounds = build_rounds(t)
    events = list_events(t, ALL_SEASONS)
    print(f"{len(events)} events {ALL_SEASONS[0]}-{ALL_SEASONS[-1]}")

    cache = {}
    for _, ev in events.iterrows():
        cache[event_key(ev)] = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds,
                                                candidates=True)
    events["KEY"] = [event_key(ev) for _, ev in events.iterrows()]
    print(f"event rows cached in {time.time()-t0:.0f}s")

    allrows = pd.concat(cache.values(), ignore_index=True)
    cand = STAGE8_REST + STAGE8_FIELD + STAGE8_COUNTS
    dem = allrows.copy()
    key = dem["TOURNAMENT"] + dem["ENDING_DATE"].astype(str)
    for c in cand + ["FINISH_PCT"]:
        dem[c] = dem[c] - dem.groupby(key)[c].transform("mean")
    print("\n=== correlation with FINISH_PCT (negative = predicts a better finish) ===")
    print(pd.DataFrame({
        "pooled": allrows[cand + ["FINISH_PCT"]].corr()["FINISH_PCT"][cand],
        "within_event": dem[cand + ["FINISH_PCT"]].corr()["FINISH_PCT"][cand],
        "missing": allrows[cand].isna().mean(),
    }).round(3).to_string())

    results = []
    for season in TEST_SEASONS:
        train = pd.concat([cache[k] for k in events[events["SEASON"] < season]["KEY"]],
                          ignore_index=True)
        season_tests = events[events["SEASON"] == season]
        tests = {k: cache[k].copy() for k in season_tests["KEY"] if not cache[k].empty}
        merged = pd.concat(tests.values(), ignore_index=True)
        train_n, merged = normalize(train.copy(), merged)
        merged["KEY2"] = np.repeat([str(k) for k in tests.keys()],
                                   [len(v) for v in tests.values()])

        base = feature_columns(train_n, include_field_size=True, variant="stage6")
        for arm, extra in CANDIDATES.items():
            fcols = base + extra
            reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                        random_state=RNG, n_jobs=-1)
            reg.fit(train_n[fcols], train_n["FINISH_PCT"])
            merged[f"M_{arm}"] = 1.0 - reg.predict(merged[fcols])
            if extra and season == TEST_SEASONS[-1]:
                imp = pd.Series(reg.feature_importances_, index=fcols).sort_values(ascending=False)
                ranks = list(imp.index)
                print(f"  [{season}] {arm}: " + ", ".join(
                    f"{c} {imp[c]:.4f} (#{ranks.index(c)+1}/{len(fcols)})" for c in extra))

        for _, ev in season_tests.iterrows():
            sub = merged[merged["KEY2"] == str(ev["KEY"])]
            if sub.empty or sub["TOP_20"].nunique() < 1:
                continue
            mkt = rankdata(sub["ODDS_SHARE"])
            for arm in CANDIDATES:
                for mode, sc in [("model", sub[f"M_{arm}"].to_numpy()),
                                 ("blend", (rankdata(sub[f"M_{arm}"]) + mkt) / 2)]:
                    m = score_event(sub, sc, is_prob=False)
                    m.update(arm=arm, mode=mode, SEASON=season,
                             TOURNAMENT=ev["TOURNAMENT"],
                             ENDING_DATE=str(pd.Timestamp(ev["ENDING_DATE"]).date()))
                    results.append(m)
        print(f"season {season} done {time.time()-t0:.0f}s (train n={len(train_n)})")

    res = pd.DataFrame(results)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stage8_eval_results.csv")
    res.to_csv(out, index=False)

    order = list(CANDIDATES)
    for mode in ("blend", "model"):
        sub = res[res["mode"] == mode]
        summ = sub.groupby("arm").agg(events=("hits15", "count"),
                                      hits15=("hits15", "mean"),
                                      auc=("auc", "mean"),
                                      spearman=("spearman_vs_pos", "mean")).loc[order]
        summ["d_hits15"] = (summ["hits15"] - summ.loc["s6", "hits15"]).round(3)
        summ["d_auc"] = (summ["auc"] - summ.loc["s6", "auc"]).round(4)
        wins, tstat = [], []
        b = sub[sub.arm == "s6"].set_index(["SEASON", "TOURNAMENT", "ENDING_DATE"])["hits15"]
        for arm in order:
            a = sub[sub.arm == arm].set_index(["SEASON", "TOURNAMENT", "ENDING_DATE"])["hits15"]
            d = (a - b).dropna()
            wins.append(f"{int((d > 0).sum())}-{int((d < 0).sum())}-{int((d == 0).sum())}")
            tstat.append(round(float(d.mean() / (d.std(ddof=1) / np.sqrt(len(d)))), 2)
                         if arm != "s6" and d.std(ddof=1) > 0 else np.nan)
        summ["w-l-t"] = wins
        summ["t"] = tstat
        print(f"\n=== {mode.upper()} · {len(sub)//len(order)} test events, "
              f"forward-chained {TEST_SEASONS[0]}-{TEST_SEASONS[-1]} ===")
        print(summ.round(3).to_string())

    print("\n=== hits@15 by season (blend) ===")
    print(res[res["mode"] == "blend"].pivot_table(
        index="SEASON", columns="arm", values="hits15", aggfunc="mean")[order].round(3).to_string())

    print(f"\nTotal time: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
