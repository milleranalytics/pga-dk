"""The world ranking as it stood before each event: pga.db's `owgr` table.

season_stats' OWGR is a season's final ranking, so the model could only use
last season's. statDetails can instead answer "through event X", which is the
ranking after X's week: it INCLUDES X's result (Matsuyama is 54th through the
2024 Pebble Beach and 20th through the Genesis he won). So an event gets the
ranking through the last Tour event that finished before it started, which is
the Monday release a golfer teeing off that Thursday was ranked by.

The Tour kept that snapshot for only some events (2025 about half, 2026 two:
the PGA Championship and St. Jude); the rest answer with no rows. An event then
takes the latest earlier snapshot, never a later one, up to MAX_AGE_DAYS old;
past that it has no ranking. `age_days` says how stale each one is.

The entry list's owgr is no substitute for a past event: the 2024 Genesis field
shows Matsuyama 20th, his rank AFTER winning it.

    owgr   tournament_id, player_id, owgr_rank, owgr_points, through, age_days
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from pga_api import build
from pga_api.client import gql

OWGR_STAT = "186"
MAX_AGE_DAYS = 60
Q = """query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int, $eventQuery: StatDetailEventQuery) {
  statDetails(tourCode: $tourCode, statId: $statId, year: $year, eventQuery: $eventQuery) {
    rows { ... on StatDetailsPlayer { playerId rank stats { statValue } } }
  }
}"""


def fetch_through(tournament_id: str, season: int, refresh: bool = False) -> list[dict]:
    d = gql("StatDetails", Q, {"tourCode": "R", "statId": OWGR_STAT, "year": int(season),
                               "eventQuery": {"tournamentId": tournament_id, "queryType": "THROUGH_EVENT"}},
            key=f"{OWGR_STAT}_through_{tournament_id}", refresh=refresh)["statDetails"]
    return [r for r in (d or {}).get("rows") or [] if r and "playerId" in r]


def table(events: pd.DataFrame, entries: pd.DataFrame | None = None) -> pd.DataFrame:
    """owgr for every stroke-play event and every event with an entry list.

    The event it reads through is the latest finished stroke-play event that
    ended before this one started and has a snapshot, at most MAX_AGE_DAYS
    before it. A release less than ten days old is fetched again: the Tour may
    not have posted Monday's ranking when it was first read."""
    ev = events.copy()
    ev["start_date"] = pd.to_datetime(ev["start_date"])
    ev["end_date"] = pd.to_datetime(ev["end_date"])
    done = ev[(ev["kind"] == "stroke") & ev["completed"].astype(bool)].sort_values("end_date")
    targets = ev[(ev["kind"] == "stroke") |
                 ev["tournament_id"].isin([] if entries is None else entries["tournament_id"])]
    out, snaps = [], {}

    def snapshot(p):
        if p["tournament_id"] not in snaps:
            fresh = p["end_date"].date() >= date.today() - timedelta(days=10)
            snaps[p["tournament_id"]] = fetch_through(p["tournament_id"], int(p["season"]), refresh=fresh)
        return snaps[p["tournament_id"]]

    for e in targets.itertuples():
        prior = done[(done["end_date"] < e.start_date) &
                     (done["end_date"] >= e.start_date - pd.Timedelta(days=MAX_AGE_DAYS))]
        for _, p in prior.iloc[::-1].iterrows():      # newest first
            rows = snapshot(p)
            if not rows:
                continue
            age = int((e.start_date - p["end_date"]).days)
            for r in rows:
                out.append({"tournament_id": e.tournament_id, "player_id": r["playerId"],
                            "owgr_rank": build._position_rank(r.get("rank")),
                            "owgr_points": build._num(r["stats"][0]["statValue"]) if r.get("stats") else None,
                            "through": p["tournament_id"], "age_days": age})
            break
    return pd.DataFrame(out, columns=["tournament_id", "player_id", "owgr_rank", "owgr_points",
                                      "through", "age_days"])
