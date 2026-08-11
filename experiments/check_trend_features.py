# experiments/check_trend_features.py
# Does the vectorized sg_trend_features_for_event() reproduce, player by
# player, the loop the player card runs? The card's momentum/volatility are
# already on screen; if the feature disagrees with them the dashboard and the
# model are describing two different players.

import sys, os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.features import (load_tables, build_rounds, sg_trend_features_for_event,
                            SG_HALFLIFE_DAYS, SG_MAX_LOOKBACK_DAYS,
                            SG_MOMENTUM_DAYS, SG_VOL_ROUNDS, SG_MIN_ROUNDS_FOR_SPREAD)

DB = "data/golf.db"


def card_reference(rounds, end_date):
    """The card's loop, transcribed from utils/dashboard.py::_history_payload."""
    win = rounds[(rounds["ENDING_DATE"] < end_date) &
                 (rounds["ENDING_DATE"] >= end_date - pd.Timedelta(days=SG_MAX_LOOKBACK_DAYS))]
    rows = []
    for name, pr in win.sort_values("ENDING_DATE").groupby("PLAYER"):
        mom = vol = np.nan
        # The card spreads an event's four rounds across four days before doing
        # anything else, so every later step sees this frame, in this order.
        pr = pr.assign(
            DATE=pr["ENDING_DATE"] - pd.to_timedelta(4 - pr["RND"], unit="D")
        ).sort_values("DATE")
        if len(pr) >= SG_MIN_ROUNDS_FOR_SPREAD:
            vol = float(pr["SG"].tail(SG_VOL_ROUNDS).std(ddof=1))
            ser = pr.set_index("DATE")["SG"]
            trend = ser.ewm(halflife=pd.Timedelta(days=SG_HALFLIFE_DAYS),
                            times=ser.index).mean()
            past = trend[trend.index <= trend.index[-1] - pd.Timedelta(days=SG_MOMENTUM_DAYS)]
            if len(past):
                mom = float(trend.iloc[-1] - past.iloc[-1])
        rows.append({"PLAYER": name, "MOM_REF": mom, "VOL_REF": vol})
    return pd.DataFrame(rows)


def main():
    t, s, o = load_tables(DB)
    rounds = build_rounds(t)
    for end_date in [pd.Timestamp("2024-06-16"), pd.Timestamp("2025-08-10"),
                     pd.Timestamp("2022-03-13")]:
        fast = sg_trend_features_for_event(rounds, end_date)
        ref = card_reference(rounds, end_date)
        j = fast.merge(ref, on="PLAYER", how="outer")
        for col, refcol in [("SG_MOMENTUM_90D", "MOM_REF"), ("SG_VOL", "VOL_REF")]:
            a, b = j[col], j[refcol]
            both_nan = a.isna() & b.isna()
            gap = (a - b).abs()
            worst = float(gap.max()) if gap.notna().any() else 0.0
            mismatch_nan = int((a.isna() != b.isna()).sum())
            print(f"{end_date.date()} {col:20s} n={len(j):4d} "
                  f"measured={int((~both_nan).sum()):4d} "
                  f"max|diff|={worst:.2e} nan_disagreements={mismatch_nan}")
            assert mismatch_nan == 0, f"{col}: NaN pattern differs from the card"
            # The feature is stored round(4); the reference is not. Anything
            # under half a unit in the last place is that rounding, not a
            # difference in definition.
            assert worst <= 5e-5, f"{col}: values differ from the card"
    print("\nOK - the vectorized features match the card's loop.")


if __name__ == "__main__":
    main()
