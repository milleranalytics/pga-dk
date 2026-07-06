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
    # Approximate each round's calendar day: round 4 on the ending date,
    # round 1 three days earlier. Off by <=1 day for Sat/Mon finishes.
    rounds["DATE"] = rounds["ENDING_DATE"] - pd.to_timedelta(4 - rounds["RND"], unit="D")
    return t, s, o, rounds


@st.cache_data(show_spinner=False)
def load_export(export_mtime: float):
    return pd.read_csv(EXPORT_PATH)


@st.cache_data(show_spinner="Computing SG form…")
def sg_rankings(db_mtime: float, as_of: str):
    _, _, _, rounds = load_db(db_mtime)
    sg = sg_features_for_event(rounds, pd.Timestamp(as_of))
    recent = rounds[rounds["ENDING_DATE"] >= pd.Timestamp(as_of) - pd.Timedelta(days=730)]
    spark = (recent.sort_values("DATE").groupby("PLAYER")["SG"]
             .apply(lambda x: [round(v, 2) for v in x.tail(20)]))
    sg = sg.merge(spark.rename("LAST_20_ROUNDS"), on="PLAYER", how="left")
    return sg


@st.cache_data(show_spinner=False)
def odds_coverage(db_mtime: float, season: int):
    t, _, o, _ = load_db(db_mtime)
    tt = t[t["SEASON"] == season][["TOURNAMENT", "ENDING_DATE", "PLAYER"]]
    oo = o[["TOURNAMENT", "ENDING_DATE", "PLAYER"]].drop_duplicates()
    m = tt.merge(oo, on=["TOURNAMENT", "ENDING_DATE", "PLAYER"], how="left", indicator=True)
    g = (m.groupby(["TOURNAMENT", "ENDING_DATE"])
         .agg(players=("PLAYER", "size"),
              matched=("_merge", lambda x: int((x == "both").sum())))
         .reset_index())
    g["odds_pct"] = g["matched"] / g["players"]
    g["ENDING_DATE"] = g["ENDING_DATE"].dt.date
    return g.sort_values("ENDING_DATE")


db_mtime = os.path.getmtime(DB_PATH)
t, s, o, rounds = load_db(db_mtime)

st.title("⛳ PGA DraftKings Model")
tab_lev, tab_sg, tab_player, tab_health = st.tabs(
    ["Leverage Board", "SG Rankings", "Player Detail", "Data Health"])


# =============================== LEVERAGE BOARD ===============================
# Visuals you can't get from the CSV: the model/market relationship and the
# value landscape. The rankings table itself lives in the CSV export.

with tab_lev:
    if not os.path.exists(EXPORT_PATH):
        st.info("No current_week_export.csv yet — run the notebook pipeline first.")
    else:
        export = load_export(os.path.getmtime(EXPORT_PATH))
        st.caption(f"{len(export)} players · export last updated "
                   f"{pd.Timestamp(os.path.getmtime(EXPORT_PATH), unit='s'):%Y-%m-%d %H:%M}")

        left, right = st.columns(2, gap="large")
        with left:
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
                template="plotly_dark", height=520)
            n = len(lv)
            fig.add_trace(go.Scatter(x=[0, n], y=[0, n], mode="lines",
                                     line=dict(dash="dot", color="gray"),
                                     showlegend=False, hoverinfo="skip"))
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Above the dotted line = model higher than market (value candidates). "
                       "Below = market higher (fade candidates in GPPs).")
        with right:
            st.subheader("Value vs Salary")
            fig2 = px.scatter(
                export, x="SALARY", y="SCORE", hover_name="PLAYER",
                color="LEVERAGE", color_continuous_scale="RdYlGn",
                color_continuous_midpoint=0,
                hover_data={"VEGAS_ODDS": True, "SG_FORM": ":.2f"},
                template="plotly_dark", height=520)
            st.plotly_chart(fig2, use_container_width=True)
            st.caption("High SCORE at low SALARY (upper left) is where the optimizer shops. "
                       "Green = model likes more than market.")


# =============================== SG RANKINGS ===============================

with tab_sg:
    st.subheader("Strokes-Gained Form (recency-weighted, all active players)")
    col1, col2, _ = st.columns([1, 1, 2])
    with col1:
        as_of = st.date_input("As of", value=pd.Timestamp.today().date())
    with col2:
        min_rounds = st.slider(
            "Min rounds (last 12 months)", 0, 60, 12,
            help="Hides small-sample players whose SG_FORM rests on a handful of "
                 "rounds (e.g. major-only LIV players). Slide to 0 to see everyone.")
    sg = sg_rankings(db_mtime, str(as_of))
    sg = sg[sg["SG_ROUNDS_12M"] >= min_rounds].sort_values("SG_FORM", ascending=False)
    sg.insert(0, "RANK", range(1, len(sg) + 1))
    st.caption(f"{len(sg)} players · SG_FORM = strokes/round vs field avg, "
               "halflife 100 days, shrunk toward 0 for thin samples")
    st.dataframe(
        sg, hide_index=True, height=700,
        column_config={
            "RANK": st.column_config.NumberColumn("Rank", width="small"),
            "PLAYER": st.column_config.TextColumn("Player", width="medium"),
            "SG_FORM": st.column_config.NumberColumn("SG Form", format="%+.2f", width="small"),
            "SG_ROUNDS_12M": st.column_config.NumberColumn("Rounds (12m)", width="small"),
            "LAST_20_ROUNDS": st.column_config.LineChartColumn(
                "Last 20 rounds (SG)", y_min=-6, y_max=6, width="large"),
        })


# =============================== PLAYER DETAIL ===============================

with tab_player:
    active = (rounds[rounds["ENDING_DATE"] >= rounds["ENDING_DATE"].max() - pd.Timedelta(days=365)]
              ["PLAYER"].value_counts().index.tolist())
    player = st.selectbox("Player", active, index=0)

    pr = rounds[rounds["PLAYER"] == player].sort_values("DATE")
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
    prw = pr[pr["DATE"] >= pr["DATE"].max() - pd.Timedelta(days=days)].copy()
    prw["Round"] = "R" + prw["RND"].astype(str)
    fig = px.scatter(prw, x="DATE", y="SG", color="Round",
                     category_orders={"Round": ["R1", "R2", "R3", "R4"]},
                     hover_data={"TOURNAMENT": True, "SG": ":.2f", "DATE": "|%b %d, %Y"},
                     template="plotly_dark", opacity=0.55, height=400,
                     labels={"SG": "SG vs field (strokes)", "DATE": ""})
    if len(prw) >= 8:
        roll = prw.set_index("DATE")["SG"].rolling("90D").mean()
        fig.add_trace(go.Scatter(x=roll.index, y=roll.values, mode="lines",
                                 name="90-day avg", line=dict(width=3, color="#fa8072")))
    fig.add_hline(y=0, line_dash="dot", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Recent results")
        res = pt[["ENDING_DATE", "TOURNAMENT", "POS"]].copy()
        res["ENDING_DATE"] = pd.to_datetime(res["ENDING_DATE"]).dt.date
        res_filter = st.text_input("Filter by tournament", "",
                                   placeholder="e.g. Masters, Deere…", key="res_filter")
        if res_filter:
            res = res[res["TOURNAMENT"].str.contains(res_filter, case=False, na=False)]
        st.dataframe(res.head(50), hide_index=True, height=400,
                     column_config={"ENDING_DATE": st.column_config.DateColumn("Date", width="small"),
                                    "POS": st.column_config.TextColumn("Pos", width="small")})
    with right:
        st.subheader("Course history")
        ch = (pt.groupby("COURSE")
              .agg(events=("FINAL_POS", "count"),
                   avg_finish=("FINAL_POS", "mean"),
                   best=("FINAL_POS", "min"),
                   cuts_made=("POS", lambda x: (~x.isin(["CUT", "W/D"])).mean()))
              .sort_values("events", ascending=False).round(1).reset_index())
        ch["cuts_made"] = (ch["cuts_made"] * 100).round(0).astype(int).astype(str) + "%"
        st.dataframe(ch, hide_index=True, height=453)


# =============================== DATA HEALTH ===============================
# The weekend-audit queries as a permanent UI: browse the DB and spot join
# failures without opening DB Browser. Still 100% read-only.

with tab_health:
    st.subheader("Odds join coverage by event")
    st.caption("Events below 90% usually mean a tournament name/date mismatch between "
               "the odds table and the tournaments table (see TOURNAMENT_NAME_MAP / "
               "the audit workflow). Small dips are Monday qualifiers the books don't list.")
    seasons = sorted(t["SEASON"].unique(), reverse=True)
    season = st.selectbox("Season", seasons, index=0)
    cov = odds_coverage(db_mtime, int(season))
    st.dataframe(
        cov, hide_index=True, height=420,
        column_config={
            "TOURNAMENT": st.column_config.TextColumn("Tournament", width="large"),
            "ENDING_DATE": st.column_config.DateColumn("Ends", width="small"),
            "players": st.column_config.NumberColumn("Players", width="small"),
            "matched": st.column_config.NumberColumn("Matched", width="small"),
            "odds_pct": st.column_config.ProgressColumn(
                "Odds matched", min_value=0.0, max_value=1.0, format="%.0f%%"),
        })
    low = cov[cov["odds_pct"] < 0.9]
    if len(low):
        st.warning(f"{len(low)} event(s) under 90% odds coverage this season — "
                   "check for name/date mismatches.")
    else:
        st.success("All events ≥90% odds coverage this season.")

    st.subheader("Ending-date weekday audit")
    st.caption("Tournaments end Sun (Sat for Farmers-style, Mon for weather). "
               "Anything ending Tue–Fri is a start-date typo.")
    ev = t[["SEASON", "TOURNAMENT", "ENDING_DATE"]].drop_duplicates().copy()
    ev["weekday"] = ev["ENDING_DATE"].dt.day_name()
    bad = ev[~ev["weekday"].isin(["Sunday", "Saturday", "Monday"])].copy()
    if len(bad):
        bad["ENDING_DATE"] = bad["ENDING_DATE"].dt.date
        st.error(f"{len(bad)} event(s) with impossible ending dates:")
        st.dataframe(bad, hide_index=True)
    else:
        st.success("All ending dates are Sun/Sat/Mon. ✅")

    st.subheader("Results browser")
    f1, f2, f3 = st.columns(3)
    with f1:
        q_player = st.text_input("Player contains", "", placeholder="e.g. Hojgaard")
    with f2:
        q_tourn = st.text_input("Tournament contains", "", placeholder="e.g. Deere")
    with f3:
        q_seasons = st.multiselect("Seasons", seasons, default=[])
    browse = t.copy()
    if q_player:
        browse = browse[browse["PLAYER"].str.contains(q_player, case=False, na=False)]
    if q_tourn:
        browse = browse[browse["TOURNAMENT"].str.contains(q_tourn, case=False, na=False)]
    if q_seasons:
        browse = browse[browse["SEASON"].isin(q_seasons)]
    browse = browse.sort_values("ENDING_DATE", ascending=False)
    show_cols = ["SEASON", "ENDING_DATE", "TOURNAMENT", "COURSE", "PLAYER", "POS",
                 "ROUNDS:1", "ROUNDS:2", "ROUNDS:3", "ROUNDS:4"]
    out = browse[show_cols].head(2000).copy()
    out["ENDING_DATE"] = out["ENDING_DATE"].dt.date
    st.caption(f"{len(browse):,} matching rows (showing up to 2,000)")
    st.dataframe(out, hide_index=True, height=500)
