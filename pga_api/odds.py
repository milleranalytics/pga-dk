"""Win odds from the Tour's own feed (FanDuel), keyed by player id.

Point in time by construction: every request carries a timestamp, and the
feed answers with the board as it stood then. Two rules keep it honest:

  history   the board an hour before the first tee time: the last price you
            could have acted on, since DraftKings locks at the first tee.
            Without tee times, noon UTC the day before round 1.
  this week refused once the first group has teed off, so a logged forecast
            never holds in-play prices

The reply also carries each golfer's finishing position. It is dropped here
and never stored: it is the answer.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd

from pga_api import build
from pga_api.client import gql

ODDS_Q = """query HistoricalTournamentsOdds($t: String!, $m: OddsMarketType!, $ts: AWSDateTime) {
  historicalTournamentsOdds(tournamentId: $t, marketId: $m, timeStamp: $ts) {
    provider
    market { subMarkets { options { ... on OddsToWinV2 {
      entity { entityId players { displayName } }
      odds { odds }
    } } } }
  }
}"""
FIRST_SEASON = 2024     # the feed is empty before 2024


def american_to_fraction(text) -> float | None:
    """American odds as the fractional number golf.db uses (12/1 -> 12.0).

    '+1300' -> 13.0; '-150' -> 0.667 (bet 150 to win 100); '+100'/'EVEN' -> 1.0.
    '0' is the feed's placeholder for a golfer with no price: None."""
    s = str(text).strip().upper()
    if s in ("EVEN", "EVS"):
        return 1.0
    try:
        v = float(s.replace("+", ""))
    except ValueError:
        return None
    if v == 0:
        return None
    return v / 100 if v > 0 else 100 / -v


TEE_Q = """query TeeTimesCompressedV2($id: ID!) { teeTimesCompressedV2(id: $id) { id payload } }"""


def first_tee(tournament_id: str, refresh: bool = False) -> datetime | None:
    """Round 1's first tee time (UTC), or None before the draw is out."""
    import base64, gzip, json
    d = gql("TeeTimesCompressedV2", TEE_Q, {"id": tournament_id}, key=tournament_id,
            refresh=refresh).get("teeTimesCompressedV2") or {}
    if not d.get("payload"):
        return None
    p = json.loads(gzip.decompress(base64.b64decode(d["payload"])))
    tees = [g["teeTime"] for r in p.get("rounds") or [] if r.get("roundInt") == 1
            for g in r.get("groups") or [] if g.get("teeTime")]
    return datetime.fromtimestamp(min(tees) / 1000, timezone.utc) if tees else None


def pre_event_time(tournament_id: str, start_date) -> datetime:
    tee = first_tee(tournament_id)
    if tee is not None:
        return tee - timedelta(hours=1)
    d = pd.Timestamp(start_date).date() - timedelta(days=1)
    return datetime(d.year, d.month, d.day, 12, 0, tzinfo=timezone.utc)


def fetch(tournament_id: str, at: datetime) -> pd.DataFrame:
    """The win board for one event as of `at`. -> player_id, name, odds_text, fraction."""
    ts = at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    live = at > datetime.now(timezone.utc) - timedelta(hours=1)
    data = gql("HistoricalTournamentsOdds", ODDS_Q, {"t": tournament_id, "m": "WINNER", "ts": ts},
               key=f"{tournament_id}_{ts.replace(':', '')}", refresh=live)
    d = data.get("historicalTournamentsOdds") or {}
    subs = (d.get("market") or {}).get("subMarkets") or []
    rows = []
    for o in (subs[0]["options"] if subs else []):
        if not o or not o.get("entity"):
            continue
        p = o["entity"]["players"][0]
        rows.append({"player_id": o["entity"]["entityId"], "name": p["displayName"],
                     "odds_text": o["odds"]["odds"],
                     "fraction": american_to_fraction(o["odds"]["odds"])})
    out = pd.DataFrame(rows, columns=["player_id", "name", "odds_text", "fraction"])
    out.attrs.update(provider=d.get("provider"), as_of=ts)
    return out


def history(seasons=None) -> pd.DataFrame:
    """Every stroke-play event since 2024 at its pre-event time. Cached forever:
    a past timestamp's board cannot change."""
    with sqlite3.connect(build.DB_PATH) as con:
        ev = pd.read_sql("SELECT tournament_id, season, start_date FROM events "
                         "WHERE kind = 'stroke' AND season >= ?", con, params=(FIRST_SEASON,))
    if seasons is not None:
        ev = ev[ev["season"].isin(list(seasons))]
    frames = []
    for e in ev.itertuples():
        b = fetch(e.tournament_id, pre_event_time(e.tournament_id, e.start_date))
        if len(b):
            frames.append(b.assign(tournament_id=e.tournament_id, as_of=b.attrs["as_of"]))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def this_week(week) -> pd.DataFrame:
    """Now's board for `week`, refused once the first group has teed off
    (before the draw is out, once round 1's date has begun, UTC)."""
    now = datetime.now(timezone.utc)
    lock = first_tee(week.tournament_id, refresh=True) or datetime.combine(
        week.start_date, datetime.min.time(), tzinfo=timezone.utc)
    if now >= lock:
        raise RuntimeError(f"{week.name} teed off {lock:%a %b %d %H:%M} UTC: odds now are in-play, "
                           "and a forecast logged from them would know part of round 1.")
    board = fetch(week.tournament_id, now)
    board.attrs["lock"] = lock
    return board
