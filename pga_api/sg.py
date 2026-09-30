"""Strokes gained round by round, Korn Ferry rounds, and the point-in-time
ratings built from them. Each is its own table in pga.db, beside season_stats:

    sg_rounds   one row per ShotLink round of a PGA TOUR stroke-play event, missed
                cuts included: sg_ott, sg_app, sg_arg, sg_putt, sg_total
    kft_events  Korn Ferry Tour events: id, name, course, dates
    kft_rounds  one row per Korn Ferry round: strokes

Augusta and a few other venues run no ShotLink, so their events have scores but
no sg_rounds; multi-course events have SG only for the ShotLink course's rounds.
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import scipy.linalg
import scipy.sparse as sp

from pga_api import build
from pga_api.client import PgaApiError, _post, gql, read_cache, write_cache

# The Tour's stat ids inside scorecardStatsV3's strokesGained list.
SG_STATS = {"02567": "sg_ott", "02568": "sg_app", "02569": "sg_arg", "02564": "sg_putt",
            "02675": "sg_total"}
SG_COLS = list(SG_STATS.values())
# Players per request. The query names each player, and 60 of them draws a 403
# from the gateway in front of the API; 40 answers in about half a second.
BATCH = 40
SG_OP = "ScorecardStatsV3"


# ---------------------------------------------------------------- scorecards

def _sg_query(pids: list[str]) -> str:
    fields = " ".join(
        f'p{p}: scorecardStatsV3(id: $id, playerId: "{p}") '
        '{ rounds { round strokesGained { statId totalNum } } }' for p in pids)
    return "query ScorecardStatsV3($id: ID!) { " + fields + " }"


def _is_graphql_error(e: PgaApiError) -> bool:
    """The API refused a player (no scorecard: 'Cannot return null for non-nullable
    type'), as opposed to the network or the gateway failing."""
    s = str(e)
    return not any(k in s for k in ("HTTP ", "non-JSON", "retries exhausted", "Timeout", "Connection"))


def _fetch_batch(tid: str, pids: list[str]) -> dict:
    """{player_id: rounds list, or None when the API has no scorecard for him}.

    One player without a scorecard nulls the whole reply (the field is non-null),
    so a failed batch is split in half until the culprit stands alone."""
    try:
        d = _post(SG_OP, _sg_query(pids), {"id": tid})
        return {p: (d.get(f"p{p}") or {}).get("rounds") for p in pids}
    except PgaApiError as e:
        if len(pids) == 1:
            if _is_graphql_error(e):
                return {pids[0]: None}
            raise
        mid = len(pids) // 2
        return {**_fetch_batch(tid, pids[:mid]), **_fetch_batch(tid, pids[mid:])}


def fetch_event_sg(tid: str, player_ids, refresh: bool = False) -> dict:
    """Every player's per-round strokes gained at one finished event, cached as one
    file: {"shotlink": bool, "players": {player_id: [rounds] or None}}.

    Kept per round: only numbered rounds that carry strokes gained (round '-1'
    is the event total). If the first 40 players have no SG round between them,
    the event ran no ShotLink (Augusta) and the rest are not asked."""
    if not refresh:
        cached = read_cache(SG_OP, tid)
        if cached is not None:
            return cached
    pids = sorted(set(player_ids))
    players: dict = {}
    shotlink = True
    for i in range(0, len(pids), BATCH):
        got = _fetch_batch(tid, pids[i:i + BATCH])
        players.update({p: [r for r in (v or []) if r.get("strokesGained") and int(r["round"]) >= 1]
                        if v is not None else None for p, v in got.items()})
        if i == 0 and not any(players.values()):
            shotlink = False
            break
    data = {"shotlink": shotlink, "players": players}
    write_cache(SG_OP, tid, data)
    return data


def parse_event_sg(tid: str, data: dict) -> list[dict]:
    rows = []
    for pid, rounds in (data.get("players") or {}).items():
        for r in rounds or []:
            n = int(r["round"])
            vals = {SG_STATS[s["statId"]]: s["totalNum"] for s in r["strokesGained"]
                    if s["statId"] in SG_STATS}
            if n < 1 or len(vals) < len(SG_STATS) or any(v is None for v in vals.values()):
                continue
            # Every value exactly 0 is a placeholder, not a round: the feed lists
            # them for a pro-am's non-ShotLink courses and odd rounds elsewhere
            # (9,491 of them, 2015-2023). Kept, they pull category ratings to 0.
            if all(v == 0 for v in vals.values()):
                continue
            rows.append({"tournament_id": tid, "player_id": pid, "round": n, **vals})
    return rows


def sg_rounds(events: pd.DataFrame, results: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """sg_rounds for every finished stroke-play event in `events`; fetches only
    events not already cached."""
    ev = events[(events["kind"] == "stroke") & events["completed"].astype(bool)]
    field = results.groupby("tournament_id")["player_id"].apply(list)
    rows, fetched = [], 0
    for tid in ev["tournament_id"]:
        if tid not in field.index:
            continue
        fetched += read_cache(SG_OP, tid) is None
        rows += parse_event_sg(tid, fetch_event_sg(tid, field[tid]))
    if verbose and fetched:
        print(f"strokes gained: fetched {fetched} events' scorecards")
    return pd.DataFrame(rows, columns=["tournament_id", "player_id", "round"] + SG_COLS)


# ---------------------------------------------------------------- Korn Ferry

KFT_TOUR = "H"


def fetch_kft_schedule(season: int) -> list[dict]:
    data = gql("Schedule", build.SCHEDULE_Q, {"tourCode": KFT_TOUR, "year": str(season)},
                     key=f"{KFT_TOUR}_{season}", refresh=season >= build._this_season())
    out = []
    for part in ("completed", "upcoming"):
        for month in data["schedule"][part]:
            for t in month["tournaments"]:
                out.append({**t, "completed": part == "completed"})
    return out


def kft(seasons, verbose: bool = True):
    """-> (kft_events, kft_rounds, players) for the Korn Ferry seasons given.

    Scores come from the final leaderboard. A golfer who withdrew or was
    disqualified keeps no rounds: the leaderboard shows the strokes he had when he
    stopped, and one partial round would read as a very good or very bad day."""
    events, rounds, players, seen = [], [], {}, set()
    for season in seasons:
        for t in fetch_kft_schedule(season):
            if not t["completed"] or t["id"] in seen:
                continue
            seen.add(t["id"])
            lb = build.fetch_leaderboard(t["id"])
            kind, res, rd, pl = build.parse_leaderboard(t["id"], lb)
            if kind != "stroke":
                continue
            gone = {r["player_id"] for r in res if r["position"] in ("W/D", "DQ")}
            rd = [r for r in rd if r["player_id"] not in gone and build.plausible_round(r)]
            events.append({
                "tournament_id": t["id"], "season": season, "name": t["tournamentName"],
                "course": t["courseName"],
                "start_date": datetime.fromtimestamp(t["startDate"] / 1000, timezone.utc).date(),
                "end_date": build._end_date(t["date"], t["startDate"]),
                "field_size": len(res),
            })
            rounds += [{k: r[k] for k in ("tournament_id", "player_id", "round", "strokes")} for r in rd]
            for p in pl:
                players.setdefault(p["player_id"], p)
        if verbose:
            n = sum(1 for e in events if e["season"] == season)
            print(f"Korn Ferry {season}: {n} events")
    return (pd.DataFrame(events), pd.DataFrame(rounds, columns=["tournament_id", "player_id", "round", "strokes"]),
            players)


# ---------------------------------------------------------------- ratings
#
# A round's strokes gained is measured against that round's field, so +2 against
# an opposite-field event counts the same as +2 against a signature event, and a
# Korn Ferry round cannot be compared with a Tour round at all. The rating fits
# every round of the last two years at once:
#
#     value(player p, round r) = difficulty_r + skill_p + noise
#
# weighted toward recent rounds (half-life 100 days, as SG_FORM) and with skill
# shrunk toward the pool by LAMBDA pseudo-rounds. With every player in every
# round it is SG_FORM; when fields differ, the golfers who play more than one
# kind of field (Korn Ferry graduates, conditional members) tell the fit how far
# apart the fields are. Everything for an event is fitted on rounds from events
# that ended before it started.

HALFLIFE_DAYS = 100
LOOKBACK_DAYS = 730
TARGETS = ["total", "ott", "app", "arg", "putt"]
# Pseudo-rounds of shrinkage per target. shrinkage_weights() on 2015-2019 (before
# any test season) reads noise/skill variance as total 15.6, ott 7.9, app 23.9,
# arg 37.6, putt 48.0: driving is the steadiest skill, putting the noisiest. Its
# level runs high (skill drifting within a season counts as noise), and SG_FORM's
# 2, chosen in the forward test, is the tested level for total; so total keeps 2
# and each category keeps its ratio to total.
LAMBDA_EB = {"total": 15.6, "ott": 7.9, "app": 23.9, "arg": 37.6, "putt": 48.0}
LAMBDA = {t: round(2.0 * v / LAMBDA_EB["total"], 2) for t, v in LAMBDA_EB.items()}
FORM_COLS = ["SGA_TOTAL", "SGA_OTT", "SGA_APP", "SGA_ARG", "SGA_PUTT", "SGA_T2G", "SGA_ROUNDS_12M"]


def round_rows(events: pd.DataFrame, rounds: pd.DataFrame, kft_events: pd.DataFrame,
               kft_rounds: pd.DataFrame, sg: pd.DataFrame) -> dict:
    """Every round as (date, round key, player, tour, value), one frame per target.

    total: minus the score (to par when the whole round has it, so a
    multi-course round is fair across pars; strokes otherwise, as legacy does).
    PGA TOUR stroke play and Korn Ferry. The categories: ShotLink rounds only.
    Dated by the event's scheduled end."""
    ev = events[events["kind"] == "stroke"][["tournament_id", "end_date"]]
    r = rounds.merge(ev, on="tournament_id")
    full = r.groupby(["tournament_id", "round"])["to_par"].transform(lambda s: s.notna().all())
    r["y"] = -np.where(full, r["to_par"], r["strokes"]).astype(float)
    k = kft_rounds.merge(kft_events[["tournament_id", "end_date"]], on="tournament_id")
    k["y"] = -k["strokes"].astype(float)
    total = pd.concat([r.assign(tour="R"), k.assign(tour="H")], ignore_index=True)
    total = total.dropna(subset=["y"])

    s = sg.merge(ev, on="tournament_id")
    out = {"total": total[["end_date", "tournament_id", "round", "player_id", "tour", "y"]]}
    for t in TARGETS[1:]:
        out[t] = s.assign(tour="R", y=s[f"sg_{t}"])[["end_date", "tournament_id", "round",
                                                      "player_id", "tour", "y"]]
    for t, f in out.items():
        f = f.copy()
        f["end_date"] = pd.to_datetime(f["end_date"])
        f["rkey"] = f["tournament_id"] + ":" + f["round"].astype(int).astype(str)
        out[t] = f.sort_values("end_date", kind="stable").reset_index(drop=True)
    return out


def fit(y, rkey, pid, w, lam: float, ref=None, home=None, detail: bool = False):
    """Weighted ridge fit of y = difficulty[round] + mean[home tour] + skill[player].

    Minimises sum w (y - c_r - m_h - b_p)^2 + lam * sum b_p^2 exactly: player
    terms are eliminated (their block is diagonal), leaving one dense system the
    size of the number of rounds. `home` (player -> 'R' or 'H') gives each tour
    but the Tour its own unshrunk mean, so a Korn Ferry regular is shrunk toward
    Korn Ferry golf, not toward the Tour; shrinking everyone toward one pool lets
    two hundred Korn Ferry players outvote the few who play both and hides most
    of the gap between the tours. Ratings are then shifted so the w-weighted mean
    over the `ref` rows (PGA TOUR rounds) is 0, which puts every date on one
    scale: strokes per round better than the average Tour round.

    -> pd.Series of rating (m_h + b_p) by player; with detail=True also each
    round's difficulty on the same scale (y - difficulty is that round's
    strokes gained against the average Tour round)."""
    n = len(y)
    r_codes, r_uni = pd.factorize(rkey)
    p_codes, p_uni = pd.factorize(pid)
    R, P = len(r_uni), len(p_uni)
    rows, cols = [np.arange(n)], [r_codes]
    g_of_p = np.full(P, -1)
    if home is not None:
        h = pd.Series(p_uni).map(home).fillna("R").to_numpy()
        groups = [g for g in pd.unique(h) if g != "R"]
        for i, g in enumerate(groups):
            g_of_p[h == g] = i
        in_g = g_of_p[p_codes] >= 0
        rows.append(np.arange(n)[in_g])
        cols.append(R + g_of_p[p_codes][in_g])
    G = int(g_of_p.max()) + 1
    X1 = sp.csr_matrix((np.ones(sum(len(r) for r in rows)), (np.concatenate(rows), np.concatenate(cols))),
                       shape=(n, R + G))
    X2 = sp.csr_matrix((np.ones(n), (np.arange(n), p_codes)), shape=(n, P))
    X1w = (X1.T @ sp.diags(w)).tocsr()
    Dinv = 1.0 / (np.bincount(p_codes, w, P) + lam)
    C = X1w @ X2                                   # (R+G) x P
    CD = C @ sp.diags(Dinv)
    S = (X1w @ X1).toarray() - (CD @ C.T).toarray()
    v = X2.T @ (w * y)
    theta = scipy.linalg.solve(S, X1w @ y - CD @ v, assume_a="pos")
    b = (X2.T @ (w * (y - X1 @ theta))) * Dinv
    if G:
        b = b + np.where(g_of_p >= 0, theta[R + np.maximum(g_of_p, 0)], 0.0)
    shift = np.average(b[p_codes[ref]], weights=w[ref]) if ref is not None else 0.0
    b = pd.Series(b - shift, index=p_uni)
    if detail:
        return b, pd.Series(theta[:R] + shift, index=r_uni)
    return b


def home_tour(win: pd.DataFrame, w) -> pd.Series:
    """player -> the tour holding most of his (recency-weighted) rounds."""
    share_h = pd.Series(w * (win["tour"] == "H").to_numpy()).groupby(win["player_id"].to_numpy()).sum() \
        / pd.Series(w).groupby(win["player_id"].to_numpy()).sum()
    return pd.Series(np.where(share_h > 0.5, "H", "R"), index=share_h.index)


def _window(f: pd.DataFrame, as_of: pd.Timestamp):
    """The rounds a fit as of `as_of` uses (events that ended before it, within
    LOOKBACK_DAYS), their age in days and their recency weights."""
    lo, hi = np.searchsorted(f["end_date"].values,
                             [np.datetime64(as_of - pd.Timedelta(days=LOOKBACK_DAYS)),
                              np.datetime64(as_of)], side="left")
    win = f.iloc[lo:hi]
    days = (as_of - win["end_date"]).dt.days.to_numpy()
    return win, days, 0.5 ** (days / HALFLIFE_DAYS)


def adjusted_rounds(rows: dict, as_of, lam: dict = None) -> pd.DataFrame:
    """Every round in SGA_TOTAL's window, as strokes gained against the average
    Tour round: the score measured against that round's fitted difficulty rather
    than its field. Korn Ferry rounds included. The points SGA_TOTAL is a
    shrunken, recency-weighted average of, which is what a chart of a golfer's
    rounds should plot beside it. -> player_id, tournament_id, round, end_date,
    tour, SG_ADJ."""
    lam = lam or LAMBDA
    as_of = pd.Timestamp(as_of)
    win, _, w = _window(rows["total"], as_of)
    if win.empty:
        return pd.DataFrame(columns=["player_id", "tournament_id", "round", "end_date", "tour", "SG_ADJ"])
    ref = (win["tour"] == "R").to_numpy()
    _, diff = fit(win["y"].to_numpy(), win["rkey"].to_numpy(), win["player_id"].to_numpy(), w,
                  lam["total"], ref=ref, home=home_tour(win, w), detail=True)
    out = win[["player_id", "tournament_id", "round", "end_date", "tour"]].copy()
    out["SG_ADJ"] = win["y"].to_numpy() - diff.reindex(win["rkey"]).to_numpy()
    return out.reset_index(drop=True)


def ratings(rows: dict, as_of, lam: dict = None) -> pd.DataFrame:
    """Every golfer's ratings as of `as_of` (a date: the event's first day), from
    rounds of events that ended before it. One row per golfer with any round in
    the last two years; FORM_COLS."""
    lam = lam or LAMBDA
    as_of = pd.Timestamp(as_of)
    out, home = {}, None
    for t in TARGETS:   # total first: it sets each golfer's home tour for the rest
        win, days, w = _window(rows[t], as_of)
        if win.empty:
            continue
        ref = (win["tour"] == "R").to_numpy()
        if t == "total":
            home = home_tour(win, w)
        out[f"SGA_{t.upper()}"] = fit(win["y"].to_numpy(), win["rkey"].to_numpy(),
                                      win["player_id"].to_numpy(), w, lam[t], ref=ref, home=home)
        if t == "total":
            out["SGA_ROUNDS_12M"] = win.loc[days <= 365, "player_id"].value_counts()
    df = pd.DataFrame(out)
    for c in FORM_COLS:
        if c not in df:
            df[c] = np.nan
    df["SGA_T2G"] = df["SGA_OTT"] + df["SGA_APP"] + df["SGA_ARG"]
    df["SGA_ROUNDS_12M"] = df["SGA_ROUNDS_12M"].fillna(0)
    df.index.name = "player_id"
    return df[FORM_COLS]


def shrinkage_weights(rows: dict, first="2014-10-01", last="2019-09-30", min_rounds: int = 20) -> pd.DataFrame:
    """How many pseudo-rounds of average golf each target should be shrunk by:
    one round's noise variance over the variance of true skill (method of
    moments on player-seasons of PGA TOUR rounds, each round measured against
    its field). Read once to set LAMBDA; the default window ends before the
    first test season, so no test result informs it."""
    out = []
    for t in TARGETS:
        f = rows[t]
        f = f[(f["tour"] == "R") & (f["end_date"] >= first) & (f["end_date"] <= last)].copy()
        f["v"] = f["y"] - f.groupby("rkey")["y"].transform("mean")
        f["season"] = f["end_date"].dt.year + (f["end_date"].dt.month >= 10)
        g = f.groupby(["player_id", "season"])["v"].agg(["mean", "var", "count"])
        g = g[g["count"] >= min_rounds]
        noise = float(np.average(g["var"], weights=g["count"] - 1))
        skill = float(g["mean"].var() - noise * (1.0 / g["count"]).mean())
        out.append({"target": t, "player_seasons": len(g), "noise_sd": noise ** 0.5,
                    "skill_sd": max(skill, 0) ** 0.5, "lambda": noise / skill})
    return pd.DataFrame(out).set_index("target")


def form_table(frames: dict, rows: dict = None, lam: dict = None) -> pd.DataFrame:
    """sg_form: each stroke-play event's field with its ratings as they stood the
    day it started. Played events use the finishers; upcoming ones the entry list.
    Events that start the same day share one fit."""
    rows = rows or round_rows(frames["events"], frames["rounds"], frames["kft_events"],
                              frames["kft_rounds"], frames["sg_rounds"])
    ev = frames["events"]
    ev = ev[(ev["kind"] == "stroke") | ev["tournament_id"].isin(frames["field"]["tournament_id"])]
    who = pd.concat([frames["results"][["tournament_id", "player_id"]],
                     frames["field"][["tournament_id", "player_id"]]]).drop_duplicates()
    out = []
    for start, group in ev.groupby("start_date"):
        r = ratings(rows, start, lam)
        for tid in group["tournament_id"]:
            f = who[who["tournament_id"] == tid].merge(r, left_on="player_id", right_index=True, how="left")
            out.append(f.assign(as_of=str(start)))
    cols = ["tournament_id", "player_id", "as_of"] + FORM_COLS
    return pd.concat(out, ignore_index=True)[cols] if out else pd.DataFrame(columns=cols)
