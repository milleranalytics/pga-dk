# app.py — read-only Streamlit sidecar for the PGA DK model
# Run from the repo root:  python -m streamlit run app.py
#
# Reads data/golf.db. Writes NOTHING — the notebook remains the only thing
# that touches the database.

import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.features import load_tables, build_rounds, sg_features_for_event

DB_PATH = "data/golf.db"

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


@st.cache_data(show_spinner="Computing SG form…")
def sg_snapshot(db_mtime: float, as_of: str):
    """SG form for every player as of a date, with rank over the full pool."""
    _, _, _, rounds = load_db(db_mtime)
    sg = sg_features_for_event(rounds, pd.Timestamp(as_of))
    sg = sg.sort_values("SG_FORM", ascending=False).reset_index(drop=True)
    sg["POOL_RANK"] = np.arange(1, len(sg) + 1)
    return sg


@st.cache_data(show_spinner=False)
def sg_rankings(db_mtime: float, as_of: str, trend_days: int = 30):
    """Current snapshot + last-20-round sparklines + rank trend vs lookback."""
    _, _, _, rounds = load_db(db_mtime)
    now = sg_snapshot(db_mtime, as_of)
    prev = sg_snapshot(db_mtime, str(pd.Timestamp(as_of) - pd.Timedelta(days=trend_days)))

    recent = rounds[rounds["ENDING_DATE"] >= pd.Timestamp(as_of) - pd.Timedelta(days=730)]
    spark = (recent.sort_values("DATE").groupby("PLAYER")["SG"]
             .apply(lambda x: [round(v, 2) for v in x.tail(20)]))
    now = now.merge(spark.rename("LAST_20_ROUNDS"), on="PLAYER", how="left")

    now = now.merge(prev[["PLAYER", "POOL_RANK"]].rename(columns={"POOL_RANK": "PREV_RANK"}),
                    on="PLAYER", how="left")
    move = now["PREV_RANK"] - now["POOL_RANK"]

    def fmt(m):
        if pd.isna(m):
            return "NEW"
        m = int(m)
        if m > 0:
            return f"🟢▲ +{m}"
        if m < 0:
            return f"🔴▼ {m}"
        return "—"

    now["TREND"] = move.map(fmt)
    return now


db_mtime = os.path.getmtime(DB_PATH)
t, s, o, rounds = load_db(db_mtime)

st.title("⛳ PGA DraftKings Model")
tab_sg, tab_player, tab_browse = st.tabs(["SG Rankings", "Player Detail", "Results Browser"])


# =============================== SG RANKINGS ===============================

with tab_sg:
    st.subheader("Strokes-Gained Form (recency-weighted, all active players)")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        as_of = st.date_input("As of", value=pd.Timestamp.today().date())
    with col2:
        min_rounds = st.slider(
            "Min rounds (last 12 months)", 0, 60, 12,
            help="Hides small-sample players whose SG_FORM rests on a handful of "
                 "rounds (e.g. major-only LIV players). Slide to 0 to see everyone.")
    with col3:
        trend_days = st.slider(
            "Trend lookback (days)", 7, 120, 30, step=7,
            help="Rank movement is measured against the SG rankings this many days ago.")
    with col4:
        q_sg = st.text_input("Player contains", "", placeholder="e.g. Griffin", key="sg_search")
    sg = sg_rankings(db_mtime, str(as_of), trend_days)
    sg = sg[sg["SG_ROUNDS_12M"] >= min_rounds].copy()
    sg.insert(0, "RANK", range(1, len(sg) + 1))
    if q_sg:
        sg = sg[sg["PLAYER"].str.contains(q_sg, case=False, na=False)]
    st.caption(f"{len(sg)} players · SG_FORM = strokes/round vs field avg, halflife 100 days, "
               f"shrunk toward 0 for thin samples · Trend = pool-rank movement vs "
               f"{trend_days} days ago")
    st.dataframe(
        sg[["RANK", "PLAYER", "SG_FORM", "TREND", "SG_ROUNDS_12M", "LAST_20_ROUNDS"]],
        hide_index=True, height=700,
        column_config={
            "RANK": st.column_config.NumberColumn("#", width="small"),
            "PLAYER": st.column_config.TextColumn("Player", width="medium"),
            "SG_FORM": st.column_config.NumberColumn("SG", format="%+.2f", width="small"),
            "TREND": st.column_config.TextColumn("Trend", width="small",
                                                 help=f"Rank movement vs {trend_days} days ago"),
            "SG_ROUNDS_12M": st.column_config.NumberColumn("Rds", width="small",
                                                           help="Rounds in the last 12 months"),
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
    window = st.radio("Window", ["6 months", "1 year", "2 years", "5 years", "All"],
                      index=2, horizontal=True)
    days = {"6 months": 183, "1 year": 365, "2 years": 730,
            "5 years": 1825, "All": 100000}[window]
    prw = pr[pr["DATE"] >= pr["DATE"].max() - pd.Timedelta(days=days)].copy()
    prw["Round"] = "R" + prw["RND"].astype(str)
    fig = px.scatter(prw, x="DATE", y="SG",
                     hover_data={"TOURNAMENT": True, "Round": True,
                                 "SG": ":.2f", "DATE": "|%b %d, %Y"},
                     template="plotly_dark", opacity=0.55, height=400,
                     labels={"SG": "SG vs field (strokes)", "DATE": ""})
    fig.update_traces(marker=dict(color="#8ab4f8", size=7))
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
        ch_filter = st.text_input("Filter by course", "",
                                  placeholder="e.g. TPC, Augusta…", key="ch_filter")
        if ch_filter:
            ch = ch[ch["COURSE"].str.contains(ch_filter, case=False, na=False)]
        st.dataframe(ch, hide_index=True, height=400)


# =============================== RESULTS BROWSER ===============================
# The debugging view: browse raw tournament rows with their odds joined in.
# Still 100% read-only — fixes happen in the notebook / DB Browser.

with tab_browse:
    st.subheader("Results browser")
    f1, f2, f3 = st.columns(3)
    with f1:
        q_player = st.text_input("Player contains", "", placeholder="e.g. Hojgaard")
    with f2:
        q_tourn = st.text_input("Tournament contains", "", placeholder="e.g. Deere")
    with f3:
        seasons = sorted(t["SEASON"].unique(), reverse=True)
        q_seasons = st.multiselect("Seasons", seasons, default=[])

    browse = t.merge(o[["TOURNAMENT", "ENDING_DATE", "PLAYER", "VEGAS_ODDS"]].drop_duplicates(),
                     on=["TOURNAMENT", "ENDING_DATE", "PLAYER"], how="left")
    if q_player:
        browse = browse[browse["PLAYER"].str.contains(q_player, case=False, na=False)]
    if q_tourn:
        browse = browse[browse["TOURNAMENT"].str.contains(q_tourn, case=False, na=False)]
    if q_seasons:
        browse = browse[browse["SEASON"].isin(q_seasons)]
    browse = browse.sort_values("ENDING_DATE", ascending=False)

    show_cols = ["SEASON", "ENDING_DATE", "TOURNAMENT", "COURSE", "PLAYER", "POS",
                 "VEGAS_ODDS", "ROUNDS:1", "ROUNDS:2", "ROUNDS:3", "ROUNDS:4"]
    out = browse[show_cols].head(2000).copy()
    out["ENDING_DATE"] = out["ENDING_DATE"].dt.date
    st.caption(f"{len(browse):,} matching rows (showing up to 2,000) · "
               "blank odds = player not listed / name mismatch")
    st.dataframe(
        out, hide_index=True, height=560,
        column_config={
            "SEASON": st.column_config.NumberColumn("Season", format="%d", width="small"),
            "ENDING_DATE": st.column_config.DateColumn("Ends", width="small"),
            "POS": st.column_config.TextColumn("Pos", width="small"),
            "VEGAS_ODDS": st.column_config.NumberColumn("Odds ( /1)", format="%.0f", width="small"),
        })
