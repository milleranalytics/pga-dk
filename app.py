# app.py — read-only Streamlit sidecar for the PGA DK model
# Run from the repo root:  python -m streamlit run app.py
#
# Reads data/golf.db and data/current_week_export.csv. Writes NOTHING —
# the notebook remains the only thing that touches the database.

import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from scipy.stats import rankdata

from utils.features import load_tables, build_rounds, sg_features_for_event

DB_PATH = "data/golf.db"
EXPORT_PATH = "data/current_week_export.csv"

st.set_page_config(page_title="PGA DK Model", layout="wide", page_icon="⛳")


# ---------- cached data loading (keyed on file mtime so edits refresh) ----------

@st.cache_data(show_spinner="Loading database…")
def load_db(db_mtime: float):
    t, s, o = load_tables(DB_PATH)
    rounds = build_rounds(t)
    return t, s, o, rounds


@st.cache_data(show_spinner=False)
def load_export(export_mtime: float):
    return pd.read_csv(EXPORT_PATH)


@st.cache_data(show_spinner="Computing SG form…")
def sg_rankings(db_mtime: float, as_of: str):
    _, _, _, rounds = load_db(db_mtime)
    sg = sg_features_for_event(rounds, pd.Timestamp(as_of))
    # last-20-round SG sequence per player for sparklines
    recent = rounds[rounds["ENDING_DATE"] >= pd.Timestamp(as_of) - pd.Timedelta(days=730)]
    spark = (recent.sort_values("ENDING_DATE").groupby("PLAYER")["SG"]
             .apply(lambda x: [round(v, 2) for v in x.tail(20)]))
    sg = sg.merge(spark.rename("LAST_20_ROUNDS"), on="PLAYER", how="left")
    return sg


db_mtime = os.path.getmtime(DB_PATH)
t, s, o, rounds = load_db(db_mtime)

st.title("⛳ PGA DraftKings Model")
tab_week, tab_sg, tab_player = st.tabs(["This Week", "SG Rankings", "Player Detail"])


# =============================== THIS WEEK ===============================

with tab_week:
    if not os.path.exists(EXPORT_PATH):
        st.info("No current_week_export.csv yet — run the notebook pipeline first.")
    else:
        export = load_export(os.path.getmtime(EXPORT_PATH))
        st.caption(f"{len(export)} players · export last updated "
                   f"{pd.Timestamp(os.path.getmtime(EXPORT_PATH), unit='s'):%Y-%m-%d %H:%M}")

        left, right = st.columns([3, 2], gap="large")

        with left:
            st.subheader("Rankings")
            show = export.copy()
            fmt = {
                "SCORE": st.column_config.ProgressColumn(
                    "SCORE", help="Blend of model rank and market rank; 1 = best in field",
                    min_value=0.0, max_value=1.0, format="%.3f"),
                "SALARY": st.column_config.NumberColumn(format="$%d"),
                "LEVERAGE": st.column_config.NumberColumn(
                    help="model rank − market rank; positive = model likes him more than the market"),
            }
            st.dataframe(show, hide_index=True, height=650, column_config=fmt)

        with right:
            st.subheader("Model vs Market")
            lv = export.dropna(subset=["MODEL_SCORE", "ODDS_SHARE"]).copy()
            lv["market_rank"] = rankdata(lv["ODDS_SHARE"])
            lv["model_rank"] = rankdata(lv["MODEL_SCORE"])
            fig = px.scatter(
                lv, x="market_rank", y="model_rank",
                color="SALARY", color_continuous_scale="Viridis",
                hover_name="PLAYER",
                hover_data={"SALARY": ":$,d", "VEGAS_ODDS": True, "LEVERAGE": True,
                            "market_rank": False, "model_rank": False},
                labels={"market_rank": "Market rank (right = market likes)",
                        "model_rank": "Model rank (up = model likes)"},
                template="plotly_dark", height=420)
            n = len(lv)
            fig.add_trace(go.Scatter(x=[0, n], y=[0, n], mode="lines",
                                     line=dict(dash="dot", color="gray"),
                                     showlegend=False, hoverinfo="skip"))
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Above the dotted line = model higher than market (value candidates). "
                       "Below = market higher (fade candidates in GPPs).")

            st.subheader("Value vs Salary")
            fig2 = px.scatter(
                export, x="SALARY", y="SCORE", hover_name="PLAYER",
                color="LEVERAGE", color_continuous_scale="RdYlGn",
                color_continuous_midpoint=0,
                hover_data={"VEGAS_ODDS": True, "SG_FORM": ":.2f"},
                template="plotly_dark", height=420)
            st.plotly_chart(fig2, use_container_width=True)
            st.caption("High SCORE at low SALARY (upper left) is where the optimizer shops. "
                       "Green = model likes more than market.")


# =============================== SG RANKINGS ===============================

with tab_sg:
    st.subheader("Strokes-Gained Form (recency-weighted, all active players)")
    col1, col2 = st.columns([1, 3])
    with col1:
        as_of = st.date_input("As of", value=pd.Timestamp.today().date())
        min_rounds = st.slider("Min rounds (last 12 months)", 0, 60, 12)
    sg = sg_rankings(db_mtime, str(as_of))
    sg = sg[sg["SG_ROUNDS_12M"] >= min_rounds].sort_values("SG_FORM", ascending=False)
    sg.insert(0, "RANK", range(1, len(sg) + 1))
    st.caption(f"{len(sg)} players · SG_FORM = strokes/round vs field avg, "
               "halflife 100 days, shrunk toward 0 for thin samples")
    st.dataframe(
        sg, hide_index=True, height=700,
        column_config={
            "SG_FORM": st.column_config.NumberColumn(format="%+.2f"),
            "SG_ROUNDS_12M": st.column_config.NumberColumn("Rounds (12m)"),
            "LAST_20_ROUNDS": st.column_config.LineChartColumn(
                "Last 20 rounds (SG)", y_min=-6, y_max=6),
        })


# =============================== PLAYER DETAIL ===============================

with tab_player:
    active = (rounds[rounds["ENDING_DATE"] >= rounds["ENDING_DATE"].max() - pd.Timedelta(days=365)]
              ["PLAYER"].value_counts().index.tolist())
    player = st.selectbox("Player", active, index=0)

    pr = rounds[rounds["PLAYER"] == player].sort_values("ENDING_DATE")
    pt = t[t["PLAYER"] == player].sort_values("ENDING_DATE", ascending=False)

    c1, c2, c3, c4 = st.columns(4)
    sg_now = sg_rankings(db_mtime, str(pd.Timestamp.today().date()))
    me = sg_now[sg_now["PLAYER"] == player]
    c1.metric("SG Form", f"{me['SG_FORM'].iloc[0]:+.2f}" if len(me) else "—")
    c2.metric("Rounds (12m)", int(me["SG_ROUNDS_12M"].iloc[0]) if len(me) else 0)
    made = (~pt.head(20)["POS"].isin(["CUT", "W/D"])).mean() if len(pt) else 0
    c3.metric("Cuts made (last 20 starts)", f"{made:.0%}")
    c4.metric("Career rounds in DB", len(pr))

    st.subheader("Strokes gained per round")
    window = st.radio("Window", ["1 year", "2 years", "5 years", "All"], index=1, horizontal=True)
    days = {"1 year": 365, "2 years": 730, "5 years": 1825, "All": 100000}[window]
    prw = pr[pr["ENDING_DATE"] >= pr["ENDING_DATE"].max() - pd.Timedelta(days=days)]
    fig = px.scatter(prw, x="ENDING_DATE", y="SG", template="plotly_dark",
                     opacity=0.45, height=380, labels={"SG": "SG vs field (strokes)"})
    if len(prw) >= 8:
        roll = prw.set_index("ENDING_DATE")["SG"].rolling("90D").mean()
        fig.add_trace(go.Scatter(x=roll.index, y=roll.values, mode="lines",
                                 name="90-day avg", line=dict(width=3)))
    fig.add_hline(y=0, line_dash="dot", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Recent results")
        res = pt.head(25)[["ENDING_DATE", "TOURNAMENT", "POS", "FINAL_POS"]].copy()
        res["ENDING_DATE"] = pd.to_datetime(res["ENDING_DATE"]).dt.date
        st.dataframe(res, hide_index=True, height=420)
    with right:
        st.subheader("Course history")
        ch = (pt.groupby("COURSE")
              .agg(events=("FINAL_POS", "count"),
                   avg_finish=("FINAL_POS", "mean"),
                   best=("FINAL_POS", "min"),
                   cuts_made=("POS", lambda x: (~x.isin(["CUT", "W/D"])).mean()))
              .sort_values("events", ascending=False).round(1).reset_index())
        ch["cuts_made"] = (ch["cuts_made"] * 100).round(0).astype(int).astype(str) + "%"
        st.dataframe(ch, hide_index=True, height=420)
