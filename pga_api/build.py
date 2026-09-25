"""Build data/pga.db from the PGA Tour API. Safe to delete and rebuild.

Every table is keyed by the Tour's own ids (tournament_id "R2018013",
player_id "37455"), never by a name. Names live in one place, `players`.

    from pga_api import build
    build.build(range(2015, 2027))       # first run fetches; later runs read the cache
"""

from __future__ import annotations

import base64
import gzip
import json
import re
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from pga_api.client import PgaApiError, gql

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pga.db"

# The season stats golf.db's `stats` table carries, by the Tour's stat id.
STAT_IDS = {
    "SGTTG": "02674", "SGOTT": "02567", "SGAPR": "02568", "SGATG": "02569", "SGP": "02564",
    "BIRDIES": "352", "PAR_3": "142", "PAR_4": "143", "PAR_5": "144",
    "TOTAL_DRIVING": "129", "DRIVING_DISTANCE": "101", "DRIVING_ACCURACY": "102",
    "GIR": "103", "SCRAMBLING": "130", "OWGR": "186",
}

SCHEDULE_Q = """query Schedule($tourCode: String!, $year: String) {
  schedule(tourCode: $tourCode, year: $year) {
    seasonYear
    completed { tournaments { id tournamentName courseName date startDate city state country } }
    upcoming  { tournaments { id tournamentName courseName date startDate city state country } }
  }
}"""

RESULTS_Q = """query TournamentPastResults($id: ID!) {
  tournamentPastResults(id: $id) {
    id rounds additionalDataHeaders
    players {
      position total parRelativeScore
      player { id displayName }
      rounds { score parRelativeScore }
      additionalData
    }
    teams { position }
  }
}"""

LEADERBOARD_Q = """query LeaderboardCompressedV3($id: ID!) {
  leaderboardCompressedV3(id: $id) { id payload }
}"""

LEADERBOARD_PLAIN_Q = """query LeaderboardV3($id: ID!) {
  leaderboardV3(id: $id) {
    id formatType tournamentStatus
    courses { id courseName hostCourse }
    players { __typename ... on PlayerRowV3 {
      player { id displayName }
      scoringData { position total totalStrokes rounds }
    } }
  }
}"""

FIELD_Q = """query Field($id: ID!) {
  field(id: $id, includeWithdrawn: true) {
    players    { id displayName withdrawn status owgr }
    alternates { id displayName withdrawn status owgr }
  }
}"""

STAT_Q = """query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int) {
  statDetails(tourCode: $tourCode, statId: $statId, year: $year) {
    year statTitle statHeaders
    rows { ... on StatDetailsPlayer { playerId playerName rank stats { statValue } } }
  }
}"""

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


# ---------------------------------------------------------------- fetch

def _this_season() -> int:
    return date.today().year


def fetch_schedule(season: int) -> list[dict]:
    """Every event the Tour lists for `season`, completed or not."""
    data = gql("Schedule", SCHEDULE_Q, {"tourCode": "R", "year": str(season)},
               key=str(season), refresh=season >= _this_season())
    sched = data["schedule"]
    out = []
    for part in ("completed", "upcoming"):
        for month in sched[part]:
            for t in month["tournaments"]:
                out.append({**t, "completed": part == "completed"})
    return out


def fetch_results(tournament_id: str, refresh: bool = False) -> dict:
    return gql("TournamentPastResults", RESULTS_Q, {"id": tournament_id},
               key=tournament_id, refresh=refresh)["tournamentPastResults"]


def fetch_leaderboard(tournament_id: str, refresh: bool = False) -> dict:
    """The final leaderboard, decompressed. The fallback for events whose past
    results are empty — renamed or retired tournaments (WGCs, CIMB, St. Jude
    Classic, ...) — which golf.db's scrape therefore never had."""
    try:
        data = gql("LeaderboardCompressedV3", LEADERBOARD_Q, {"id": tournament_id},
                   key=tournament_id, refresh=refresh)
    except PgaApiError as e:
        # The compressed feed answers some events with an empty 200 (2021 AmEx);
        # the plain one serves the same leaderboard.
        if "non-JSON" not in str(e):
            raise
        return gql("LeaderboardV3", LEADERBOARD_PLAIN_Q, {"id": tournament_id},
                   key=tournament_id, refresh=refresh)["leaderboardV3"]
    payload = data["leaderboardCompressedV3"]["payload"]
    return json.loads(gzip.decompress(base64.b64decode(payload)))


def fetch_field(tournament_id: str) -> list[dict]:
    """The entry list before an event is played, withdrawals and alternates
    included. Always re-fetched: it changes all week."""
    d = gql("Field", FIELD_Q, {"id": tournament_id}, key=tournament_id, refresh=True)["field"]
    out = []
    for group, rows in (("field", d.get("players") or []), ("alternate", d.get("alternates") or [])):
        for p in rows:
            last, _, first = p["displayName"].partition(", ")
            out.append({"tournament_id": tournament_id, "player_id": p["id"],
                        "name": f"{first} {last}".strip() if first else last,
                        "entry": group, "withdrawn": bool(p.get("withdrawn")),
                        "status": p.get("status"), "owgr": _int(p.get("owgr"))})
    return out


def fetch_stat(stat_id: str, season: int) -> dict:
    return gql("StatDetails", STAT_Q, {"tourCode": "R", "statId": stat_id, "year": season},
               key=f"{stat_id}_{season}", refresh=season >= _this_season())["statDetails"]


# ---------------------------------------------------------------- parse

def _end_date(date_text: str, start_ms: int) -> date:
    """'Oct 29 - Nov 1' or 'Jul 16 - 19' plus the start timestamp -> the end date.

    The scheduled end: a Monday finish after a weather delay still reads Sunday.
    """
    start = datetime.fromtimestamp(start_ms / 1000, timezone.utc).date()
    m = re.fullmatch(r"\s*([A-Z][a-z]{2}) (\d+) - (?:([A-Z][a-z]{2}) )?(\d+)\s*", date_text)
    if not m:
        raise ValueError(f"unrecognised schedule date {date_text!r}")
    end_month = MONTHS[m.group(3) or m.group(1)]
    year = start.year + (1 if end_month < start.month else 0)
    return date(year, end_month, int(m.group(4)))


def _num(text) -> float | None:
    """'$1,008,000.00' / '62.50%' / '500.000' / '1,234' -> float; blanks and dashes -> None."""
    if text is None:
        return None
    s = str(text).strip().replace("$", "").replace(",", "").replace("%", "")
    if s in ("", "-", "--", "E"):
        return 0.0 if s == "E" else None
    s = s.replace("+", "")
    try:
        v = float(s)
    except ValueError:
        return None
    return None if v != v else v   # 'NaN' parses; it is a blank


def _int(text) -> int | None:
    v = _num(text)
    return None if v is None else int(round(v))


def _position_rank(pos) -> int | None:
    """'1' / 'T3' -> 1 / 3; 'CUT', 'W/D', 'DQ', 'MDF' -> None."""
    m = re.fullmatch(r"T?(\d+)", str(pos or "").strip())
    return int(m.group(1)) if m else None


def classify(tournament_id: str, res: dict, fedex_total: float) -> str:
    """What kind of event this is, from the shape of its results.

    'stroke'     individual stroke play that awards FedExCup points, plus the TOUR
                 Championship, which pays a bonus pool instead of points
    'exhibition' individual scores but no points: Hero, Olympics, Q-School, and the
                 team shootouts, whose "players" post team scores like 57
    'match'      match play;  'team' team events;  'none' no results published
    The API's own formatType reads STROKE_PLAY for all of these, Ryder Cup included.
    """
    if res["teams"]:
        return "team"
    if not res["players"]:
        return "none"
    if "RESULT" in (res["additionalDataHeaders"] or []):
        return "match"
    if not all(re.fullmatch(r"R[1-4]", r or "") for r in res["rounds"]):
        return "exhibition"
    return "stroke" if fedex_total > 0 or tournament_id[-3:] == "060" else "exhibition"


def is_stableford(res: dict) -> bool:
    """A signed round 'score' ('+6') is Stableford points: a stroke count has no sign.

    The leaderboard's formatType cannot say so: 2019 Barracuda reads STROKE_PLAY.
    """
    return any(str(r.get("score", "")).startswith(("+", "-")) and r["score"] not in ("-", "--")
               for p in res["players"] for r in p["rounds"] or [])


def parse_results(tournament_id: str, res: dict):
    """-> (results rows, rounds rows, players rows, fedex total) for one event.

    Stableford events put points where strokes go and the true score to par in
    parRelativeScore, so their rounds carry to_par and no strokes.
    """
    stableford = is_stableford(res)
    headers = res["additionalDataHeaders"] or []
    i_pts = headers.index("FedExCup Pts") if "FedExCup Pts" in headers else None
    i_money = headers.index("Official Money") if "Official Money" in headers else None
    results, rounds, players = [], [], []
    for p in res["players"]:
        pid = p["player"]["id"]
        extra = p["additionalData"] or []
        pts = _num(extra[i_pts]) if i_pts is not None and i_pts < len(extra) else None
        money = _num(extra[i_money]) if i_money is not None and i_money < len(extra) else None
        results.append({
            "tournament_id": tournament_id, "player_id": pid,
            "position": p["position"], "finish_rank": _position_rank(p["position"]),
            "total_strokes": None if stableford else _int(p["total"]),
            "to_par": None if stableford else _int(p["parRelativeScore"]),
            "stableford_points": _int(p["total"]) if stableford else None,
            "fedex_points": pts, "official_money": money,
        })
        for n, r in enumerate(p["rounds"] or [], 1):
            strokes, to_par = _int(r["score"]), _int(r["parRelativeScore"])
            if (to_par if stableford else strokes) is None:
                continue
            rounds.append({"tournament_id": tournament_id, "player_id": pid, "round": n,
                           "strokes": None if stableford else strokes, "to_par": to_par})
        players.append({"player_id": pid, "name": p["player"]["displayName"]})
    fedex_total = sum(r["fedex_points"] or 0 for r in results)
    return results, rounds, players, fedex_total


def same_event(res: dict, lb: dict) -> bool:
    """Do past results and the leaderboard describe the same tournament?

    Past results for R2021535 (2021 U.S. Open) return a different event: Wyndham
    Clark first where the leaderboard has him missing the cut. Judged on finishing
    positions of the players both list; WD and W/D are the same.
    """
    lb_pos = {p["player"]["id"]: p["scoringData"]["position"].replace("/", "")
              for p in lb.get("players") or [] if p.get("__typename") == "PlayerRowV3"}
    if not lb_pos or lb.get("tournamentStatus") != "COMPLETED":
        return True
    common = [(p["position"].replace("/", ""), lb_pos[p["player"]["id"]])
              for p in res["players"] if p["player"]["id"] in lb_pos]
    return len(common) >= 0.5 * len(res["players"]) and \
        sum(a == b for a, b in common) >= 0.5 * len(common)


def parse_leaderboard(tournament_id: str, lb: dict):
    """-> (kind, results, rounds, players) from a final leaderboard.

    Carries no FedExCup points, money or per-round score to par; those stay NULL.
    """
    rows = [p for p in lb.get("players") or [] if p.get("__typename") == "PlayerRowV3"]
    if not rows or lb.get("tournamentStatus") != "COMPLETED":
        return "none", [], [], []
    if lb.get("formatType") != "STROKE_PLAY":
        return lb.get("formatType", "other").lower(), [], [], []
    results, rounds, players = [], [], []
    for p in rows:
        pid, sd = p["player"]["id"], p["scoringData"]
        position = "W/D" if sd["position"] == "WD" else sd["position"]   # past results' spelling
        results.append({
            "tournament_id": tournament_id, "player_id": pid,
            "position": position, "finish_rank": _position_rank(position),
            "total_strokes": _int(sd.get("totalStrokes")), "to_par": _int(sd.get("total")),
            "stableford_points": None, "fedex_points": None, "official_money": None,
        })
        for n, s in enumerate(sd.get("rounds") or [], 1):
            strokes = _int(s)
            if strokes is not None:
                rounds.append({"tournament_id": tournament_id, "player_id": pid, "round": n,
                               "strokes": strokes, "to_par": None})
        players.append({"player_id": pid, "name": p["player"]["displayName"]})
    return "stroke", results, rounds, players


def merge_rounds(tournament_id: str, results_rounds: list[dict], lb: dict):
    """Round scores in their true order: the leaderboard's, not the past results'.

    Past results swap R1 and R2 for some players (Koepka's 2017 U.S. Open reads
    70-67; he shot 67-70) while the totals still agree, so the swap is invisible
    at the event level. Its (score, to par) pairs are intact, so each leaderboard
    round takes its to-par from the pair with the same score. Players absent
    from the leaderboard keep their past-results rounds.

    -> (rounds rows, players whose past-results order differed, players whose
        round scores differed as a set)
    """
    lb_rows = [p for p in lb.get("players") or [] if p.get("__typename") == "PlayerRowV3"]
    if lb.get("tournamentStatus") != "COMPLETED" or not lb_rows:
        return results_rounds, 0, 0
    by_player: dict[str, list[dict]] = {}
    for r in results_rounds:
        by_player.setdefault(r["player_id"], []).append(r)
    rounds, swapped, differed, seen = [], 0, 0, set()
    for p in lb_rows:
        pid = p["player"]["id"]
        seen.add(pid)
        pairs = by_player.get(pid, [])
        lb_strokes = [_int(s) for s in p["scoringData"].get("rounds") or []]
        lb_strokes_played = [s for s in lb_strokes if s is not None]
        pr_strokes = [r["strokes"] for r in pairs]
        if pairs and sorted(pr_strokes) != sorted(lb_strokes_played):
            differed += 1
        elif pairs and pr_strokes != lb_strokes_played:
            swapped += 1
        in_order = pr_strokes == lb_strokes_played
        # A golfer who withdrew or was disqualified shows the strokes he had when
        # he stopped (Luke Donald's 52, 2024 WWT); only rounds the past results
        # confirm are complete.
        confirmed = Counter(pr_strokes) if p["scoringData"]["position"] in ("WD", "W/D", "DQ") else None
        played = 0
        for n, s in enumerate(lb_strokes, 1):
            if s is None:
                continue
            if confirmed is not None:
                if not confirmed[s]:
                    continue
                confirmed[s] -= 1
            if in_order and played < len(pairs):
                to_par = pairs[played]["to_par"]
            else:
                # Same score on two courses of different par (72 as +1 and as E)
                # stays unknown rather than guessed.
                cands = {r["to_par"] for r in pairs if r["strokes"] == s}
                to_par = cands.pop() if len(cands) == 1 else None
            played += 1
            rounds.append({"tournament_id": tournament_id, "player_id": pid, "round": n,
                           "strokes": s, "to_par": to_par})
    rounds += [r for r in results_rounds if r["player_id"] not in seen]
    return rounds, swapped, differed


def plausible_round(r: dict) -> bool:
    """A complete 18-hole round. The record is 58 (-12 to -14); anything under 55
    strokes or 15 under par is a round abandoned partway (C.T. Pan's nine-hole
    42, 'to par -30', 2023 RSM)."""
    s, tp = r.get("strokes"), r.get("to_par")
    return not ((s is not None and s < 55) or (tp is not None and tp < -15))


# ---------------------------------------------------------------- build

def build(seasons, stat_seasons=None, db_path: Path = DB_PATH, verbose: bool = True) -> dict:
    """Rebuild pga.db for `seasons` (results) and `stat_seasons` (season stats).

    Replaces the whole file: nothing in it is hand-edited, so nothing is lost.
    """
    seasons = list(seasons)
    stat_seasons = list(stat_seasons) if stat_seasons is not None else seasons
    events, results, rounds, players, courses = [], [], [], {}, []

    for season in seasons:
        sched = fetch_schedule(season)
        for t in sched:
            ev = {
                "tournament_id": t["id"], "season": season, "name": t["tournamentName"],
                "course": t["courseName"], "city": t["city"], "state": t["state"],
                "country": t["country"],
                "start_date": datetime.fromtimestamp(t["startDate"] / 1000, timezone.utc).date(),
                "end_date": _end_date(t["date"], t["startDate"]),
                "completed": t["completed"],
            }
            if not t["completed"]:
                ev.update(kind="upcoming", source=None, field_size=0, scoring=None,
                          past_results_rejected=False,
                          round_order_fixed=0, round_scores_disagree=0)
                events.append(ev)
                continue
            res = fetch_results(t["id"])
            lb = fetch_leaderboard(t["id"])
            r, rd, pl, fedex_total = parse_results(t["id"], res)
            kind, source = classify(t["id"], res, fedex_total), "past_results"
            swapped = differed = 0
            stableford = is_stableford(res)
            rejected = kind == "stroke" and not same_event(res, lb)
            if kind == "none" or rejected:
                kind, r, rd, pl = parse_leaderboard(t["id"], lb)
                source = "leaderboard"
            elif not stableford:
                rd, swapped, differed = merge_rounds(t["id"], rd, lb)
            ev.update(kind=kind, source=source, field_size=len(r),
                      scoring="stableford" if stableford else "strokes",
                      past_results_rejected=rejected,
                      round_order_fixed=swapped, round_scores_disagree=differed)
            events.append(ev)
            results += r
            if kind in ("stroke", "exhibition"):   # match and team "rounds" are not 18-hole scores
                rounds += [x for x in rd if plausible_round(x)]
            for p in pl:
                players.setdefault(p["player_id"], p)
            for c in lb.get("courses") or []:
                courses.append({"tournament_id": t["id"], "course_id": c["id"],
                                "course_name": c["courseName"], "host": c.get("hostCourse")})
        if verbose:
            kinds = pd.Series([e["kind"] for e in events if e["season"] == season]).value_counts()
            print(f"{season}: {len(sched)} events ({', '.join(f'{v} {k}' for k, v in kinds.items())})")

    stats = []
    for season in stat_seasons:
        for name, sid in STAT_IDS.items():
            d = fetch_stat(sid, season)
            for row in (d or {}).get("rows") or []:
                if not row or "playerId" not in row:
                    continue
                raw = row["stats"][0]["statValue"] if row.get("stats") else None
                stats.append({"season": season, "stat": name, "player_id": row["playerId"],
                              "rank": _position_rank(row.get("rank")), "value": _num(raw),
                              "raw_value": raw})
                players.setdefault(row["playerId"], {"player_id": row["playerId"],
                                                     "name": row["playerName"]})
        if verbose:
            print(f"stats {season}: {sum(1 for s in stats if s['season'] == season)} rows")

    # Entry lists of events starting in the next ten days: what this week's
    # DraftKings names and odds resolve against before any result exists.
    soon = date.today() + timedelta(days=10)
    entries = []
    for e in events:
        if e["kind"] == "upcoming" and e["start_date"] <= soon:
            entries += fetch_field(e["tournament_id"])
    for p in entries:
        players.setdefault(p["player_id"], {"player_id": p["player_id"], "name": p["name"]})
    field = pd.DataFrame(entries, columns=["tournament_id", "player_id", "name", "entry",
                                           "withdrawn", "status", "owgr"])

    frames = {
        "events": pd.DataFrame(events),
        "event_courses": pd.DataFrame(courses),
        "players": pd.DataFrame(list(players.values())),
        "results": pd.DataFrame(results),
        "rounds": pd.DataFrame(rounds),
        "season_stats": pd.DataFrame(stats),
        "field": field.drop(columns="name"),
    }
    from pga_api import archives
    frames.update(archives.port_all(frames, {t: g for t, g in field.groupby("tournament_id")}))
    _write(frames, db_path)
    if verbose:
        print(f"wrote {db_path.name}: " + ", ".join(f"{k} {len(v):,}" for k, v in frames.items()))
    return frames


def _write(frames: dict, db_path: Path) -> None:
    tmp = db_path.with_suffix(".building")
    tmp.unlink(missing_ok=True)
    with sqlite3.connect(tmp) as con:
        for name, df in frames.items():
            df.to_sql(name, con, index=False)
        con.executescript("""
            CREATE UNIQUE INDEX ix_events   ON events(tournament_id);
            CREATE UNIQUE INDEX ix_courses  ON event_courses(tournament_id, course_id);
            CREATE UNIQUE INDEX ix_players  ON players(player_id);
            CREATE UNIQUE INDEX ix_results  ON results(tournament_id, player_id);
            CREATE UNIQUE INDEX ix_rounds   ON rounds(tournament_id, player_id, round);
            CREATE UNIQUE INDEX ix_stats    ON season_stats(season, stat, player_id);
            CREATE UNIQUE INDEX ix_field    ON field(tournament_id, player_id);
            CREATE UNIQUE INDEX ix_odds     ON odds(tournament_id, player_id);
            CREATE INDEX        ix_salaries ON dk_salaries(tournament_id, player_id);
        """)
    con.close()
    tmp.replace(db_path)
