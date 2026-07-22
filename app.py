# app.py — read-only Streamlit sidecar for the PGA DK model
# Run from the repo root:  python -m streamlit run app.py
#
# Reads data/golf.db. Writes NOTHING — the notebook remains the only thing
# that touches the database.

import json
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.features import (load_tables, build_rounds, sg_features_for_event,
                            sg_at_course_for_event)

DB_PATH = "data/golf.db"
CURRENT_WEEK_META = "data/current_week.json"


def _current_week_meta():
    try:
        with open(CURRENT_WEEK_META, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def current_week_course():
    """This week's course, per the marker the notebook writes on export."""
    return _current_week_meta().get("course")


def current_week_end():
    """This week's tournament ending date (what the export uses as the SG
    as-of), so the app can reconcile with the CSV by default."""
    d = _current_week_meta().get("ending_date")
    return pd.Timestamp(d).date() if d else None

st.set_page_config(page_title="PGA Data Explorer", layout="wide")


# ---------- cached data loading (keyed on file mtime so edits refresh) ----------

@st.cache_data(show_spinner="Loading database…")
def load_db(db_mtime: float):
    t, s, o = load_tables(DB_PATH)
    rounds = build_rounds(t)
    if "COURSE" not in rounds.columns:
        # Belt-and-braces: a long-running Streamlit process can hold a stale
        # utils.features module (Streamlit reruns app.py but does not
        # re-import modules). Restarting the server is the real fix.
        courses = t[["TOURNAMENT", "ENDING_DATE", "COURSE"]].drop_duplicates()
        rounds = rounds.merge(courses, on=["TOURNAMENT", "ENDING_DATE"], how="left")
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
            return f"🟢 +{m}"
        if m < 0:
            return f"🔴 {m}"
        return "—"

    now["TREND"] = move.map(fmt)
    return now


@st.cache_data(show_spinner=False)
def load_current_week_field(csv_mtime: float):
    """This week's scored field from the notebook's export, so Player Detail can
    show the model's verdict (P_TOP20/SCORE/LEVERAGE) next to the form evidence."""
    try:
        return pd.read_csv("data/current_week_export.csv")
    except (FileNotFoundError, ValueError, pd.errors.EmptyDataError):
        return pd.DataFrame()


def top_field_player():
    """This week's highest-P_TOP20 player — the default Player Detail lands on,
    so the tab opens on someone with data rather than whoever sorts first."""
    csv = "data/current_week_export.csv"
    if not os.path.exists(csv):
        return None
    f = load_current_week_field(os.path.getmtime(csv))
    if not len(f) or "P_TOP20" not in f.columns:
        return None
    return f.sort_values("P_TOP20", ascending=False)["PLAYER"].iloc[0]


def current_streak(pos_recent_first: pd.Series):
    """(length, 'made'|'missed') of the current cut streak. Expects POS ordered
    most-recent start first."""
    if len(pos_recent_first) == 0:
        return 0, None
    made = ~pos_recent_first.isin(["CUT", "W/D"])
    first = bool(made.iloc[0])
    run = 0
    for m in made:
        if bool(m) == first:
            run += 1
        else:
            break
    return run, ("made" if first else "missed")


def _field_pctile(value, arr):
    """Fraction of the field the player beats on an SG metric (higher = better),
    plus the field size actually used. Returns (None, n) when the value is
    missing or the field is too thin (<10 with data) to rank against."""
    arr = arr[~np.isnan(arr)]
    if pd.isna(value) or len(arr) < 10:
        return None, len(arr)
    return float((arr < value).mean()), len(arr)


def player_flags(pt, sp_cur, field_dist, sg_form, rounds_12m, top20_rate, this_course):
    """Rules-based green/red/yellow flags for the lineup one-pager. Conservative:
    only fires on clear signals, stays quiet when the data is thin. Returns a list
    of (level, text) with level in {'R','G','Y'}.

    SG-category and ball-striking flags are FIELD-RELATIVE — they fire on the
    top/bottom 15% of THIS week's field (field_dist = {col: np.array of the
    field's current-season SG}), so "strong/weak" self-adjusts to who a player is
    actually up against. The PGA-wide season rank is still shown in the text as an
    absolute anchor / DB-vs-site checksum. sp_cur is the player's current-season
    stats row (or None). Thresholds are meant to be tuned after watching them fire."""
    flags = []

    # --- recent form: current cut streak ---
    run, kind = current_streak(pt["POS"])
    if kind == "missed" and run >= 2:
        flags.append(("R", f"Cold: {run} straight missed cuts"))
    elif kind == "made" and run >= 6:
        flags.append(("G", f"Steady: {run} straight cuts made"))

    # --- SG form (recency-weighted, model's form metric) ---
    if pd.notna(sg_form):
        if sg_form >= 1.0:
            flags.append(("G", f"Hot form: SG {sg_form:+.2f}/rd"))
        elif sg_form <= -0.5:
            flags.append(("R", f"Poor form: SG {sg_form:+.2f}/rd"))

    # --- course fit at THIS week's course ---
    if this_course:
        cc = pt[pt["COURSE"] == this_course]
        n = int(cc["FINAL_POS"].notna().sum())
        if n == 0:
            flags.append(("Y", f"No history at {this_course}"))
        else:
            cuts = (~cc["POS"].isin(["CUT", "W/D"])).mean()
            best = int(cc["FINAL_POS"].min())
            if n >= 3 and cuts >= 0.6 and best <= 15:
                flags.append(("G", f"Course horse: {this_course} — best "
                                   f"T{best}, {cuts:.0%} cuts ({n} events)"))
            elif n >= 3 and cuts < 0.4:
                flags.append(("R", f"Poor course fit: {this_course} — "
                                   f"{cuts:.0%} cuts ({n} events)"))
            elif n < 3:
                flags.append(("Y", f"Thin course history at {this_course} "
                                   f"({n} event{'s' if n != 1 else ''})"))

    # --- SG by category, ranked within THIS week's field ---
    if sp_cur is None:
        flags.append(("Y", "No current-season SG data (rookie / thin sample)"))
    else:
        for col, name in [("SGP", "putting"), ("SGATG", "short game"),
                          ("SGOTT", "driving"), ("SGAPR", "approach")]:
            v, rk = sp_cur.get(col), sp_cur.get(col + "_RANK")
            p, _ = _field_pctile(v, field_dist.get(col, np.array([])))
            if p is None:
                continue
            tag = f", PGA rank {int(rk)}" if pd.notna(rk) else ""
            if p <= 0.15:
                flags.append(("R", f"Weak {name} — bottom {p:.0%} of field "
                                   f"(SG {v:+.2f}{tag})"))
            elif p >= 0.85:
                flags.append(("G", f"Strong {name} — top {1 - p:.0%} of field "
                                   f"(SG {v:+.2f}{tag})"))
        v, rk = sp_cur.get("SGTTG"), sp_cur.get("SGTTG_RANK")
        p, _ = _field_pctile(v, field_dist.get("SGTTG", np.array([])))
        if p is not None:
            tag = f", PGA rank {int(rk)}" if pd.notna(rk) else ""
            if p >= 0.85:
                flags.append(("G", f"Elite ball-striker — top {1 - p:.0%} of field "
                                   f"(SG T2G {v:+.2f}{tag})"))
            elif p <= 0.15:
                flags.append(("R", f"Ball-striking cold — bottom {p:.0%} of field "
                                   f"(SG T2G {v:+.2f}{tag})"))

    # --- ceiling / consistency over last 20 starts ---
    if len(pt) >= 8 and pd.notna(top20_rate):
        if top20_rate <= 0.10:
            flags.append(("Y", f"Low ceiling: {top20_rate:.0%} top-20 in last 20"))
        elif top20_rate >= 0.35:
            flags.append(("G", f"High ceiling: {top20_rate:.0%} top-20 in last 20"))

    # --- thin sample warning ---
    if pd.notna(rounds_12m) and rounds_12m < 20:
        flags.append(("Y", f"Thin sample: {int(rounds_12m)} rounds in last 12m"))

    order = {"R": 0, "G": 1, "Y": 2}
    return sorted(flags, key=lambda f: order[f[0]])


db_mtime = os.path.getmtime(DB_PATH)
t, s, o, rounds = load_db(db_mtime)

st.title("PGA Data Explorer")

NAV = ["This Week", "Player Detail", "Course Explorer",
       "Prediction Tracker", "Results Browser", "SG Rankings"]
if "nav" not in st.session_state:
    st.session_state.nav = NAV[0]

# Player Detail dropdown: everyone with rounds in the DB, most recent first
# (course horses are often inactive players, and they must be jumpable too)
active = (rounds.groupby("PLAYER")["ENDING_DATE"].max()
          .sort_values(ascending=False).index.tolist())

# Click-through: a row selected in the SG Rankings or Course Explorer table
# jumps to Player Detail. Must run BEFORE the nav/selectbox widgets exist.
for _tbl, _shown_key in [("sg_table", "sg_display_players"),
                         ("ce_table", "ce_display_players"),
                         ("tw_table", "tw_display_players")]:
    _handled = _tbl + "_handled"
    sel_state = st.session_state.get(_tbl)
    sel_rows = list(sel_state.selection.rows) if sel_state is not None else []
    shown = st.session_state.get(_shown_key, [])
    if not sel_rows:
        st.session_state[_handled] = []
    elif (sel_rows != st.session_state.get(_handled) and sel_rows[0] < len(shown)
          and shown[sel_rows[0]] in active):
        st.session_state[_handled] = sel_rows
        st.session_state.player_select = shown[sel_rows[0]]
        st.session_state.nav = "Player Detail"

# Remember the Player Detail selection across tab switches. Streamlit deletes
# widget state for widgets that don't render in a run, so on the first run after
# leaving Player Detail we mirror the pick into a plain (non-widget) key that
# persists for the whole session.
if "player_select" in st.session_state:
    st.session_state.last_player = st.session_state.player_select

nav = st.segmented_control("nav", NAV, key="nav", label_visibility="collapsed")
if nav is None:
    nav = NAV[0]


# =============================== THIS WEEK ===============================
# The notebook's scored export as a browsable, click-through field — easier
# than typing names into Player Detail.

if nav == "This Week":
    meta = _current_week_meta()
    csv_path = "data/current_week_export.csv"
    field = (load_current_week_field(os.path.getmtime(csv_path))
             if os.path.exists(csv_path) else pd.DataFrame())
    if not len(field):
        st.info("No current-week export found. Run the notebook's **Export** cell "
                "to generate `data/current_week_export.csv`.")
    else:
        title = meta.get("name", "Current week")
        end = meta.get("ending_date", "")
        st.subheader(title + (f" — {end}" if end else ""))

        q_tw = st.text_input("Player contains", "", placeholder="e.g. Scheffler",
                             key="tw_search")
        cols = [c for c in ["PLAYER", "SALARY", "P_TOP20", "SCORE", "LEVERAGE",
                            "VEGAS_ODDS", "SG_FORM", "SG_CH_SHRUNK", "CUT_PERCENTAGE",
                            "OWGR_RANK"]
                if c in field.columns]
        show = field[cols].copy()
        if "P_TOP20" in show.columns:  # fraction -> percent points for display
            show["P_TOP20"] = (show["P_TOP20"] * 100).round(1)
        if q_tw:
            show = show[show["PLAYER"].str.contains(q_tw, case=False, na=False)]
        show = show.reset_index(drop=True)
        st.session_state.tw_display_players = show["PLAYER"].tolist()
        st.caption(f"{len(show)} players · order = model P(top-20) · "
                   "click a row to open that player in Player Detail.")
        st.dataframe(
            show, hide_index=True, height=700,
            on_select="rerun", selection_mode="single-row", key="tw_table",
            column_config={
                "PLAYER": st.column_config.TextColumn("Player", width="medium"),
                "SALARY": st.column_config.NumberColumn("Salary", format="$%d", width="small"),
                "P_TOP20": st.column_config.NumberColumn("P(top-20) %", format="%.1f", width="small"),
                "SCORE": st.column_config.NumberColumn("Score", format="%.2f", width="small"),
                "LEVERAGE": st.column_config.NumberColumn("Lev", format="%+.1f", width="small",
                            help="Model rank − market rank. + = value, − = fade."),
                "VEGAS_ODDS": st.column_config.NumberColumn("Vegas", format="%.0f", width="small"),
                "SG_FORM": st.column_config.NumberColumn("SG Form", format="%+.2f", width="small"),
                "SG_CH_SHRUNK": st.column_config.NumberColumn("SG Course", format="%+.2f", width="small",
                            help="SG/round at this week's course (shrunk toward field avg). + = course fit."),
                "CUT_PERCENTAGE": st.column_config.NumberColumn("Cut %", format="%.0f", width="small"),
                "OWGR_RANK": st.column_config.NumberColumn("OWGR", format="%d", width="small"),
            })


# =============================== SG RANKINGS ===============================

if nav == "SG Rankings":
    st.subheader("Strokes-Gained Form (recency-weighted, all active players)")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        as_of = st.date_input("As of", value=current_week_end() or pd.Timestamp.today().date(),
                              help="Defaults to this week's tournament date so the SG "
                                   "values match the CSV export. Change it to explore other dates.")
    with col2:
        min_rounds = st.slider(
            "Min rounds (last 12 months)", 0, 60, 16,
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
    disp = sg[["RANK", "PLAYER", "SG_FORM", "TREND", "SG_ROUNDS_12M", "LAST_20_ROUNDS"]].reset_index(drop=True)
    st.session_state.sg_display_players = disp["PLAYER"].tolist()
    st.caption("Click a row to open that player in Player Detail.")
    st.dataframe(
        disp,
        hide_index=True, height=700,
        on_select="rerun", selection_mode="single-row", key="sg_table",
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

if nav == "Player Detail":
    # Default to this week's top-ranked player on first visit; a click-through or
    # manual pick takes over after that, and `last_player` (mirrored above) brings
    # the pick back after a trip to another tab. Setting the initial value via
    # `index` rather than pre-seeding session_state avoids a widget-label desync
    # when landing here from another tab.
    if "player_select" in st.session_state:
        player = st.selectbox("Player", active, key="player_select")
    else:
        _default = st.session_state.get("last_player") or top_field_player()
        _idx = active.index(_default) if _default in active else 0
        player = st.selectbox("Player", active, index=_idx, key="player_select")

    pr = rounds[rounds["PLAYER"] == player].sort_values("DATE")
    pt = t[t["PLAYER"] == player].sort_values("ENDING_DATE", ascending=False)
    this_course = current_week_course()

    # This week's scored field: drives the strip below AND the field-relative SG
    # percentiles in the flags.
    meta = _current_week_meta()
    csv_path = "data/current_week_export.csv"
    field = (load_current_week_field(os.path.getmtime(csv_path))
             if os.path.exists(csv_path) else pd.DataFrame())

    # Current-season SG: the player's own row + the field's distribution to rank
    # him within (falls back to the whole current-season population when no
    # export is loaded). The model trains on PRIOR-season stats, so this is a
    # fresh discretionary overlay, not a model input.
    ref_season = int(s["SEASON"].max()) if len(s) else None
    sp_cur, field_dist = None, {}
    if ref_season is not None:
        ref = s[s["SEASON"] == ref_season]
        cur = ref[ref["PLAYER"] == player]
        sp_cur = cur.iloc[0] if len(cur) else None
        fld = set(field["PLAYER"]) if len(field) else set(ref["PLAYER"])
        fsg = ref[ref["PLAYER"].isin(fld)]
        field_dist = {c: fsg[c].to_numpy(dtype=float)
                      for c in ["SGOTT", "SGAPR", "SGATG", "SGP", "SGTTG"]}
    field_n = int((~np.isnan(field_dist.get("SGP", np.array([])))).sum()) if field_dist else 0

    sg_now = sg_rankings(db_mtime, str(pd.Timestamp.today().date()))
    me = sg_now[sg_now["PLAYER"] == player]
    sg_form = me["SG_FORM"].iloc[0] if len(me) else np.nan
    rounds_12m = me["SG_ROUNDS_12M"].iloc[0] if len(me) else np.nan
    made = (~pt.head(20)["POS"].isin(["CUT", "W/D"])).mean() if len(pt) else 0
    top20_rate = (pt.head(20)["FINAL_POS"] <= 20).mean() if len(pt) else np.nan
    run, kind = current_streak(pt["POS"])

    # ---- This week (section header owning the verdict, flags, and form cards) ----
    st.subheader(f"This week — {meta.get('name', 'current event')}")
    frow = field[field["PLAYER"] == player] if len(field) else field
    if len(frow):
        r = frow.iloc[0]
        w = st.columns(5)
        w[0].metric("P(top-20)", f"{r['P_TOP20']:.1%}" if pd.notna(r.get("P_TOP20")) else "—")
        w[1].metric("Score", f"{r['SCORE']:.3f}" if pd.notna(r.get("SCORE")) else "—")
        lev = r.get("LEVERAGE", np.nan)
        w[2].metric("Leverage", f"{lev:+.1f}" if pd.notna(lev) else "—",
                    help="Model rank − market rank. Positive = model likes him "
                         "more than Vegas (value); negative = fade.")
        w[3].metric("Vegas odds", f"{r['VEGAS_ODDS']:.0f}/1" if pd.notna(r.get("VEGAS_ODDS")) else "—")
        w[4].metric("Salary", f"${int(r['SALARY']):,}" if pd.notna(r.get("SALARY")) else "—")
    else:
        st.caption("Not in this week's DK field (or the export hasn't been generated yet).")

    # ---- Auto flags: the this-week read at a glance. Shown only for players in
    # the field — the flags are a this-week verdict (course fit, field-relative
    # SG), so they'd be out of place on someone who isn't playing. ----
    if len(frow):
        flags = player_flags(pt, sp_cur, field_dist, sg_form, rounds_12m, top20_rate, this_course)
        with st.container(border=True):
            st.markdown("**🚩 Flags**")
            if flags:
                icon = {"R": "🔴", "G": "🟢", "Y": "🟡"}
                st.markdown("\n".join(f"{icon[lvl]} {txt}  " for lvl, txt in flags))
            else:
                st.caption("No notable flags — middling profile on the signals checked.")
            base = f" · SG flags ranked vs {field_n} field players" if field_n else ""
            st.caption("Heuristic scan of form, this-week course fit, and current-season "
                       f"SG. A guide, not gospel — eyeball the detail below.{base}")

    # ---- Form profile: player-intrinsic form metrics + current-season SG.
    # Generic title because it spans more than SG (cuts, top-20 rate) and the
    # form window can reach into last season. ----
    st.subheader("Form profile")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SG Form", f"{sg_form:+.2f}" if pd.notna(sg_form) else "—")
    c2.metric("Rounds (12m)", int(rounds_12m) if pd.notna(rounds_12m) else 0)
    c3.metric("Cuts made (last 20)", f"{made:.0%}")
    c4.metric("Current streak",
              (f"{run} made" if kind == "made" else f"{run} missed") if kind else "—")
    c5.metric("Top-20 rate (last 20)", f"{top20_rate:.0%}" if pd.notna(top20_rate) else "—",
              help="Share of last 20 starts finishing top-20 — the model's target outcome.")

    # ---- Current-season SG profile (discretionary overlay) ----
    if sp_cur is not None:
        cats = [("Driving", "SGOTT"), ("Approach", "SGAPR"),
                ("Around green", "SGATG"), ("Putting", "SGP")]
        cat_df = pd.DataFrame({
            "Category": [c[0] for c in cats],
            "SG": [sp_cur.get(c[1]) for c in cats],
            "Rank": [sp_cur.get(c[1] + "_RANK") for c in cats],
        }).dropna(subset=["SG"])
        if len(cat_df):
            st.markdown(f"**SG by phase — {ref_season} season**")
            st.caption("Where this season's strokes are won or lost. Discretionary "
                       "overlay: the model trains on prior-season stats, not these.")
            figc = go.Figure(go.Bar(
                x=cat_df["SG"], y=cat_df["Category"], orientation="h",
                marker_color=["#4caf50" if v >= 0 else "#e57373" for v in cat_df["SG"]],
                text=[f"{v:+.2f}" + (f"  (rank {int(rk)})" if pd.notna(rk) else "")
                      for v, rk in zip(cat_df["SG"], cat_df["Rank"])],
                textposition="outside", cliponaxis=False))
            # Outside labels hang off the free end of each bar — right for
            # positive, left for negative. The right side only needs a nudge
            # (the r=80 margin absorbs the longest label via cliponaxis=False),
            # but left-side labels would run into the category names, so give
            # the left real room whenever a negative bar exists. Label width is
            # fixed pixels while padding is data units, hence the asymmetry.
            lo = min(cat_df["SG"].min(), 0)
            hi = max(cat_df["SG"].max(), 0)
            span = max(hi - lo, 0.1)
            # 0.10: the label itself needs ~7% of the span at typical window
            # widths, so this leaves a slim gap without stranding dead space.
            lpad = 0.10 * span if lo < 0 else 0.05 * span
            figc.update_layout(template="plotly_dark", height=230,
                               margin=dict(l=10, r=80, t=10, b=10),
                               xaxis_title="SG per round vs field",
                               xaxis_range=[lo - lpad, hi + 0.05 * span],
                               yaxis=dict(autorange="reversed"))
            figc.add_vline(x=0, line_dash="dot", line_color="gray")
            st.plotly_chart(figc, use_container_width=True)

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
    def form_line(df):
        # same weighting family as the model's SG_FORM: exponential decay,
        # 100-day halflife — smooth, and each round's influence fades gradually
        ser = df.set_index("DATE")["SG"]
        return ser.ewm(halflife=pd.Timedelta(days=100), times=ser.index).mean()

    if len(prw) >= 8:
        roll = form_line(prw)
        fig.add_trace(go.Scatter(x=roll.index, y=roll.values, mode="lines",
                                 name=f"{player} (form)",
                                 line=dict(width=3, color="#fa8072")))
    compare = st.multiselect("Compare form with…", [p for p in active if p != player],
                             max_selections=4)
    for cp in compare:
        cpr = rounds[(rounds["PLAYER"] == cp) &
                     (rounds["DATE"] >= prw["DATE"].min())].sort_values("DATE")
        if len(cpr) >= 8:
            croll = form_line(cpr)
            fig.add_trace(go.Scatter(x=croll.index, y=croll.values, mode="lines",
                                     name=f"{cp} (form)", line=dict(width=2)))
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
              .sort_values("events", ascending=False).reset_index())
        ch["avg_finish"] = ch["avg_finish"].round(1)
        # Convert to percent BEFORE rounding — a frame-wide .round(1) on the
        # fraction was quantizing this to the nearest 10% (0.667 -> 0.7 -> 70%).
        ch["cuts_made"] = (ch["cuts_made"] * 100).round(0).astype(int).astype(str) + "%"
        # Pin this week's course to the top (kept browsable via the filter). The
        # 📍 marks it; clearing/typing in the filter lets you roam other venues.
        pinned = bool(this_course) and this_course in ch["COURSE"].values
        if pinned:
            ch.insert(0, "", np.where(ch["COURSE"] == this_course, "📍", ""))
            ch = ch.sort_values([ch.columns[0], "events"],
                                ascending=[False, False]).reset_index(drop=True)
            st.caption(f"📍 = this week's course ({this_course})")
        ch_filter = st.text_input("Filter by course", "",
                                  placeholder="e.g. TPC, Augusta…", key="ch_filter")
        if ch_filter:
            ch = ch[ch["COURSE"].str.contains(ch_filter, case=False, na=False)]
        if pinned and not ch_filter:
            styled = ch.style.apply(
                lambda row: ["background-color: rgba(250,128,114,0.18)"
                             if row["COURSE"] == this_course else "" for _ in row],
                axis=1).format({"avg_finish": "{:.1f}"})
            st.dataframe(styled, hide_index=True, height=400)
        else:
            st.dataframe(ch, hide_index=True, height=400)


# =============================== COURSE EXPLORER ===============================
# Horses for courses, measured properly: SG per round AT this course, not
# just finish positions (which mix in field strength and luck).

if nav == "Course Explorer":
    course_counts = (t[["COURSE", "ENDING_DATE"]].drop_duplicates()
                     .groupby("COURSE")["ENDING_DATE"].agg(["count", "max"])
                     .sort_values("max", ascending=False))
    options = course_counts.index.tolist()
    this_week = current_week_course()
    default_idx = options.index(this_week) if this_week in options else 0
    c1, c2 = st.columns([2, 1])
    with c1:
        course = st.selectbox("Course", options, index=default_idx,
                              help="Defaults to this week's course (from the notebook).")
    with c2:
        min_course_rounds = st.slider("Min rounds at course", 2, 20, 8,
                              help="8 = at least two full events of data")

    sub_t = t[t["COURSE"] == course]
    n_events = sub_t[["TOURNAMENT", "ENDING_DATE"]].drop_duplicates().shape[0]
    st.caption(f"{n_events} events at {course} in the database "
               f"({sub_t['ENDING_DATE'].min():%Y}–{sub_t['ENDING_DATE'].max():%Y})")

    res_agg = (sub_t.groupby("PLAYER")
               .agg(events=("FINAL_POS", "count"),
                    best=("FINAL_POS", "min"),
                    avg_finish_pct=("FINISH_PCT", "mean"),
                    cuts_made=("POS", lambda x: (~x.isin(["CUT", "W/D"])).mean()),
                    last_played=("ENDING_DATE", "max")))

    sg_agg = (rounds[rounds["COURSE"] == course].groupby("PLAYER")
              .agg(course_rounds=("SG", "count"), sg_at_course=("SG", "mean")))

    # The exact model feature (SG_CH_SHRUNK: 7-yr window, shrunk toward 0 by
    # K=2 pseudo-rounds) so this table reconciles with the CSV export.
    model_sg = sg_at_course_for_event(rounds, pd.Timestamp.today(), course)

    ce = res_agg.join(sg_agg).reset_index()
    ce = ce.merge(model_sg, on="PLAYER", how="left")
    ce = ce[ce["course_rounds"] >= min_course_rounds]
    ce["last_played"] = ce["last_played"].dt.year
    ce = ce.sort_values("SG_CH_SHRUNK", ascending=False).reset_index(drop=True)
    ce.insert(0, "RANK", range(1, len(ce) + 1))

    q_course = st.text_input("Player contains", "", placeholder="e.g. Spieth", key="ce_search")
    if q_course:
        ce = ce[ce["PLAYER"].str.contains(q_course, case=False, na=False)]

    st.caption("**SG (model)** is the exact SG_CH_SHRUNK feature the model uses "
               "(last 7 years, shrunk toward field-average for small samples) — it "
               "matches the CSV export. **SG (raw)** is the plain all-time average "
               "at the course, unshrunk, for intuition.")
    ce_disp = ce[["RANK", "PLAYER", "SG_CH_SHRUNK", "sg_at_course", "course_rounds",
                  "avg_finish_pct", "cuts_made", "best", "last_played"]].reset_index(drop=True)
    st.session_state.ce_display_players = ce_disp["PLAYER"].tolist()
    st.caption("Click a row to open that player in Player Detail.")
    st.dataframe(
        ce_disp,
        hide_index=True, height=650,
        on_select="rerun", selection_mode="single-row", key="ce_table",
        column_config={
            "RANK": st.column_config.NumberColumn("#", width="small"),
            "PLAYER": st.column_config.TextColumn("Player", width="medium"),
            "SG_CH_SHRUNK": st.column_config.NumberColumn(
                "SG (model)", format="%+.2f", width="small",
                help="The SG_CH_SHRUNK feature the model uses — 7-yr window, "
                     "shrunk toward field-average. Matches the CSV export."),
            "sg_at_course": st.column_config.NumberColumn(
                "SG (raw)", format="%+.2f", width="small",
                help="Plain all-time average strokes gained at this course (unshrunk)"),
            "course_rounds": st.column_config.NumberColumn("Rds", width="small"),
            "avg_finish_pct": st.column_config.NumberColumn(
                "Avg finish pct", format="%.2f", width="small",
                help="0 = won, 1 = last; cut bucket ≈ 0.58"),
            "cuts_made": st.column_config.NumberColumn("Cuts", format="percent", width="small"),
            "best": st.column_config.NumberColumn("Best", width="small"),
            "last_played": st.column_config.NumberColumn("Last", format="%d", width="small"),
        })


# =============================== PREDICTION TRACKER ===============================
# The model's live out-of-sample track record: each week's logged SCOREs
# joined against what actually happened once the results are imported.

if nav == "Prediction Tracker":
    import sqlite3
    try:
        con = sqlite3.connect(DB_PATH)
        preds = pd.read_sql("SELECT * FROM predictions", con)
        con.close()
    except Exception:
        preds = pd.DataFrame()

    if preds.empty:
        st.info("No predictions logged yet. The notebook's Export cell appends each "
                "week's scored field to the predictions table — the track record "
                "starts with your next live run.")
    else:
        preds["ENDING_DATE"] = pd.to_datetime(preds["ENDING_DATE"])
        results = t[["TOURNAMENT", "ENDING_DATE", "PLAYER", "POS", "FINAL_POS"]]
        j = preds.merge(results, on=["TOURNAMENT", "ENDING_DATE", "PLAYER"], how="left")

        weeks = []
        for (tourn, date), grp in j.groupby(["TOURNAMENT", "ENDING_DATE"]):
            has_results = grp["FINAL_POS"].notna().any()
            key = ("P_TOP20" if "P_TOP20" in grp.columns and grp["P_TOP20"].notna().any()
                   else "SCORE")
            top15 = grp.nlargest(15, key)
            weeks.append({
                "TOURNAMENT": tourn, "ENDING_DATE": date.date(),
                "players": len(grp),
                "scored": "✔" if has_results else "pending",
                "top15_in_top20": int((top15["FINAL_POS"] <= 20).sum()) if has_results else None,
                "top15_cuts_made": (f"{(~top15['POS'].isin(['CUT','W/D'])).mean():.0%}"
                                    if has_results else None),
            })
        wk = pd.DataFrame(weeks).sort_values("ENDING_DATE", ascending=False)
        st.subheader("Weekly track record")
        done = wk[wk["scored"] == "✔"]
        if len(done):
            st.caption(f"{len(done)} scored week(s) · avg top-20 hits from the model's "
                       f"top 15: {done['top15_in_top20'].mean():.2f} "
                       f"(forward-chained eval baseline: 6.49)")
        st.dataframe(wk, hide_index=True,
                     column_config={"top15_in_top20": st.column_config.NumberColumn(
                         "Top-15 → top-20 hits",
                         help="Of the model's 15 highest SCOREs, how many finished top-20")})

        st.subheader("Week detail")
        pick = st.selectbox("Week", wk["TOURNAMENT"] + " — " + wk["ENDING_DATE"].astype(str))
        tourn, date = pick.rsplit(" — ", 1)
        sort_key = "P_TOP20" if "P_TOP20" in j.columns else "SCORE"
        det = j[(j["TOURNAMENT"] == tourn) &
                (j["ENDING_DATE"] == pd.Timestamp(date))].sort_values(sort_key, ascending=False)
        show = det[[c for c in ["PLAYER", "SALARY", "P_TOP20", "SCORE", "LEVERAGE",
                                "VEGAS_ODDS", "POS", "FINAL_POS"] if c in det.columns]]
        st.dataframe(show, hide_index=True, height=500,
                     column_config={"POS": st.column_config.TextColumn("Actual Pos")})


# =============================== RESULTS BROWSER ===============================
# The debugging view: browse raw tournament rows with their odds joined in.
# Still 100% read-only — fixes happen in the notebook / DB Browser.

if nav == "Results Browser":
    st.subheader("Results browser")
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        q_player = st.text_input("Player contains", "", placeholder="e.g. Hojgaard")
    with f2:
        q_tourn = st.text_input("Tournament contains", "", placeholder="e.g. Deere")
    with f3:
        q_course = st.text_input("Course contains", "", placeholder="e.g. Birkdale")
    with f4:
        seasons = sorted(t["SEASON"].unique(), reverse=True)
        q_seasons = st.multiselect("Seasons", seasons, default=[])

    browse = t.merge(o[["TOURNAMENT", "ENDING_DATE", "PLAYER", "VEGAS_ODDS"]].drop_duplicates(),
                     on=["TOURNAMENT", "ENDING_DATE", "PLAYER"], how="left")
    if q_player:
        browse = browse[browse["PLAYER"].str.contains(q_player, case=False, na=False)]
    if q_tourn:
        browse = browse[browse["TOURNAMENT"].str.contains(q_tourn, case=False, na=False)]
    if q_course:
        browse = browse[browse["COURSE"].str.contains(q_course, case=False, na=False)]
    if q_seasons:
        browse = browse[browse["SEASON"].isin(q_seasons)]
    # newest event first; within an event, winners at the top (cuts/WDs sink
    # to the bottom via their 90-filled FINAL_POS)
    browse = browse.sort_values(["ENDING_DATE", "FINAL_POS"], ascending=[False, True])

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
