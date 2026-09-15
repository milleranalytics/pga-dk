# utils/dk_api.py
"""This week's DraftKings prices, without the manual download.

Replaces `data/DKSalaries.csv` - the file that used to be downloaded from the
DraftKings lobby by hand and overwritten every week.

**It is two steps, and which one you skip IS the switch.**

    refresh_from_dk(tournament_config)   DraftKings -> data/salaries/   HOME ONLY
    load_field(tournament_config)        data/salaries/ -> `dk`         ALWAYS

`load_field` makes **no request to DraftKings under any circumstance** - not
even a failed one - so it is the same call at home and at work, where
`draftkings.com` is blocked outright and the fetch dies in the TLS handshake.
The handoff between the two machines is a `git push` at home and a `git pull`
at work, because `data/salaries/` is committed.

WHY THE PRICES ARE ARCHIVED RATHER THAN OVERWRITTEN. DraftKings takes a
contest down once it has been played, and there is no URL that returns last
week's salaries. A single `DKSalaries.csv` meant every week landed on top of
the one before it and those prices were gone. Salary history is the one input
a lineup-level backtest cannot recompute from results: whether a lineup would
have FIT UNDER THE CAP is answerable only from the prices posted that week.

WHY THERE IS NO `DKSalaries.csv` IN THE MIDDLE ANY MORE. It would be a second
copy of the archive under a name that claims to be current - two files, both
valid salary files, free to disagree about which tournament is live.
"""

from __future__ import annotations

import glob
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

LOBBY_URL = "https://www.draftkings.com/lobby/getcontests?sport={sport}"
DRAFTABLES_URL = "https://api.draftkings.com/draftgroups/v1/draftgroups/{dg}/draftables"

SALARY_DIR = "data/salaries"
CACHE_DIR = "data/raw/dk"

#: DraftKings quotes every tee time in UTC and writes the export in Eastern.
ET = ZoneInfo("America/New_York")

#: Be a polite guest on an endpoint nobody promised us.
REQUEST_PAUSE = 0.25
RETRIES = 3

#: DraftKings' export column order, exactly - verified against the last file
#: downloaded by hand (2026 TOUR Championship).
CSV_COLUMNS = ["Position", "Name + ID", "Name", "ID", "Roster Position",
               "Salary", "Game Info", "TeamAbbrev", "AvgPointsPerGame", "Status"]

#: `draftStats` id for Fantasy Points Per Game - the export's AvgPointsPerGame.
#: GOLF's id, not NFL's (90): the stat ids are per sport, and reading the wrong
#: one here would silently return 0.0 for every player rather than raising.
FPPG_STAT_ID = 795

#: `GameTypeId` of the salary-cap ("Classic") game. The golf lobby also offers
#: Tiers (135), Snake (190) and Birdies or Better (347) on the same tournament,
#: each with its own draft group and none of them a salary cap. Recognising the
#: classic game is most of the slate choice here, since a golf draft group
#: holds one tournament rather than NFL's week of slates.
CLASSIC_GAME_TYPE = 6


# ---------------------------------------------------------------------------
# transport
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int = 30) -> dict:
    """One GET returning JSON, with a small retry for a flaky hop."""
    last = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            # A 404 IS AN ANSWER, NOT A FAILURE: that draft group does not exist.
            if e.code == 404:
                raise FileNotFoundError(f"draft group not found: {url}") from None
            last = e
            time.sleep(1.5 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {RETRIES} tries: {url} ({last})")


def cache_path(draft_group: int, directory: str = CACHE_DIR) -> str:
    return os.path.join(directory, f"dg-{int(draft_group)}.json")


def draftables(draft_group: int, *, use_cache: bool = True,
               directory: str = CACHE_DIR, pause: float = REQUEST_PAUSE) -> dict:
    """The raw draftables payload for one draft group, cached on disk.

    Written through `os.replace`, so an interrupted fetch cannot leave a
    half-written file that later parses as an empty field.
    """
    path = cache_path(draft_group, directory)
    if use_cache and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    payload = _get(DRAFTABLES_URL.format(dg=int(draft_group)))
    if pause:
        time.sleep(pause)

    os.makedirs(directory, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)
    return payload


def lobby_draft_groups(sport: str = "GOLF") -> pd.DataFrame:
    """Every draft group DraftKings is currently advertising for `sport`.

    THE LOBBY IS THE ONLY INDEX THERE IS. Draft groups do not expire, but
    nothing enumerates the past ones - which is exactly why a tournament not
    saved while it was live is gone for good.
    """
    d = _get(LOBBY_URL.format(sport=sport))
    rows = []
    for g in d.get("DraftGroups", []):
        rows.append({
            "draft_group": g.get("DraftGroupId"),
            "start": g.get("StartDateEst"),
            "suffix": (g.get("ContestStartTimeSuffix") or "").strip(),
            "tag": g.get("DraftGroupTag") or "",
            "game_type": g.get("GameTypeId"),
            "contest_type": g.get("ContestTypeId"),
        })
    df = pd.DataFrame(rows)
    if len(df):
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
        # The suffix is how DraftKings names the tour and the game: " (PGA
        # TOUR)", " (DP World Tour)", " (PGA TOUR Tiers)". Kept verbatim for
        # the printed listing; never parsed to decide anything, because
        # `game_type` already answers "is this the salary-cap game".
        df["tour"] = df["suffix"].str.strip("() ")
        df = df.sort_values(["start", "draft_group"])
    return df.reset_index(drop=True)


def golf_slates(*, classic_only: bool = True) -> pd.DataFrame:
    """What DraftKings is offering right now, one row per draft group."""
    df = lobby_draft_groups("GOLF")
    if classic_only and len(df):
        df = df[df["game_type"] == CLASSIC_GAME_TYPE].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# reading one payload
# ---------------------------------------------------------------------------

def competition(payload: dict) -> dict:
    """The tournament this draft group prices: name, venue and first tee time.

    A golf draft group holds exactly ONE competition - the tournament - where
    an NFL draft group holds a slate of games. That is what makes the choice of
    draft group a choice of TOURNAMENT here, and why it can be checked against
    the notebook's `new_tournament_name` rather than against a schedule.
    """
    comps = payload.get("competitions") or []
    if not comps:
        raise ValueError("draft group has no competition")
    c = comps[0]
    start = c.get("startTime")
    return {
        "competition_id": c.get("competitionId"),
        "name": (c.get("name") or "").strip(),
        "venue": (c.get("venue") or "").strip(),
        "start": pd.to_datetime(start, utc=True, format="ISO8601") if start else pd.NaT,
        "state": c.get("competitionState"),
    }


def _fppg(row: dict) -> float:
    """AvgPointsPerGame, matching DraftKings' own export.

    The API writes `-` for a player with no history where the CSV export writes
    `0`. That is a formatting difference and the export's spelling wins, since
    the whole point of this module is to produce the file DK produces.

    NOT POINT-IN-TIME UNLESS THE FILE WAS SAVED BEFORE THE FIRST TEE. This is
    DraftKings' own season-to-date average and it keeps updating, so a draft
    group re-fetched after its tournament finishes reports an average that
    INCLUDES that tournament. Nothing here reads the column - `load_field`
    takes Name and Salary - but the archive is meant to be read years from now,
    and a feature built on this out of a late re-pull would be a model scoring a
    tournament partly from its own result. `fetched_at` in the `-meta.json` is
    what settles it; see `data/salaries/README.md`.
    """
    for a in (row.get("draftStatAttributes") or []):
        if a.get("id") == FPPG_STAT_ID:
            try:
                return float(a.get("value"))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def to_dk_csv(payload: dict) -> pd.DataFrame:
    """The payload as `DKSalaries.csv` - same columns, same order, same values.

    ONE ROW PER PLAYER. Golf's classic game has a single roster slot ("G") and
    no flex, so unlike NFL there is no (player, slot) duplication to fold up;
    the de-duplication below is a guard, not a transformation, and keeps the
    LOWER `draftableId` the way DraftKings' own export does.

    `Game Info` is the tournament name alone - no matchup, no tee time. That is
    what DraftKings writes for golf, verified against the last hand-downloaded
    file, whose every row reads `TOUR Championship`.
    """
    rows = payload.get("draftables") or []
    if not rows:
        raise ValueError("draft group has no draftables")
    comp = competition(payload)

    per_player: dict[int, dict] = {}
    for r in rows:
        pid = r["playerId"]
        keep = per_player.get(pid)
        if keep is None or r["draftableId"] < keep["draftableId"]:
            per_player[pid] = r

    out = []
    for r in per_player.values():
        did = r["draftableId"]
        name = r["displayName"]
        out.append({
            "Position": r.get("position"),
            "Name + ID": f"{name} ({did})",
            "Name": name,
            "ID": did,
            # Golf prices one slot, so eligibility and position are the same
            # fact. Read from the payload rather than written as "G", so a
            # future game with two slots shows up rather than being flattened.
            "Roster Position": r.get("position"),
            "Salary": r.get("salary"),
            "Game Info": comp["name"],
            "TeamAbbrev": r.get("teamAbbreviation"),
            "AvgPointsPerGame": _fppg(r),
            # The export leaves a healthy player's cell EMPTY; the feed says
            # "None". Same fact, and the export's spelling wins.
            "Status": "" if r.get("status") in (None, "None") else r["status"],
        })

    df = pd.DataFrame(out, columns=CSV_COLUMNS)
    # DraftKings orders the export by salary descending; nothing downstream
    # depends on it, but a file that sorts like theirs diffs like theirs.
    return (df.sort_values(["Salary", "Name"], ascending=[False, True])
              .reset_index(drop=True))


def write_dk_csv(df: pd.DataFrame, path: str) -> str:
    """Write the export to `path`, in DraftKings' own encoding.

    utf-8-sig and CRLF, because that is what DraftKings ships and what the
    hand-downloaded files in this repo already are (a plain utf-8 write names
    the first column "\\ufeffPosition" the next time it is read). Written
    through `os.replace`, so an interrupted write cannot leave a truncated
    field that parses as a short one.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8-sig", lineterminator="\r\n")
    os.replace(tmp, path)
    return path


# ---------------------------------------------------------------------------
# the archive - where a tournament's prices live
# ---------------------------------------------------------------------------
#
# FILENAME: dk-<season>-<YYYY-MM-DD>-<slug>.csv, e.g.
#
#     dk-2026-2026-09-20-biltmore-championship-asheville.csv
#     dk-2027-2026-10-11-sanderson-farms-championship.csv
#
# Season AND the full ending date, which is the project's key everywhere else -
# `tournaments` is keyed on SEASON / ENDING_DATE / TOURNAMENT and the notebook
# asks for both in USER INPUTS.
#
# THE YEAR IS WRITTEN TWICE ON PURPOSE. A PGA season does not match a calendar
# year: the autumn events that OPEN a season end in the previous calendar year
# (the 2015 season opened at the Frys.com Open in October 2014), so season and
# ending year genuinely differ - and they differ on no fixed rule. The same
# October week belongs to the NEXT season in 2014-2022 and to the CURRENT one
# from 2023 on, when the wraparound schedule ended; September holds both kinds
# in the database today. So neither field can be derived from the other, and
# writing only one would make the folder quietly wrong about a third of the
# autumn. The second line above is the case that makes it worth the redundancy.
#
# LOOKED UP ON (season, slug), never on the date. The date is there so the
# folder reads chronologically and so a wrong `new_ending_date` is visible;
# keying on it would mean that correcting that input hid the file.

_META_SUFFIX = "-meta.json"
_NAME_RE = re.compile(
    r"^dk-(?P<season>\d{4})-(?P<ending>\d{4}-\d{2}-\d{2})-(?P<slug>.+)\.csv$")


def slugify(name: str) -> str:
    """A tournament name as a filename fragment: lowercase, hyphenated.

    Deliberately lossy and deliberately STABLE under the punctuation that moves
    around in tournament names - "THE PLAYERS Championship" and "The Players
    Championship" are the same file, as are "AT&T Pebble Beach Pro-Am" and
    "AT&T Pebble Beach Pro Am". That is what makes the lookup survive the
    notebook's own spelling drifting by a character.
    """
    s = re.sub(r"[^a-z0-9]+", "-", str(name).strip().lower())
    return s.strip("-")


def archive_name(season: int, ending_date, tournament_name: str) -> str:
    d = pd.to_datetime(ending_date)
    return f"dk-{int(season)}-{d.strftime('%Y-%m-%d')}-{slugify(tournament_name)}.csv"


def _new(config: dict) -> dict:
    """The `new` half of the notebook's `tournament_config`, with the fields
    this module needs and a readable error when one is missing."""
    try:
        n = config["new"]
        return {"season": int(n["season"]), "name": str(n["name"]),
                "ending_date": pd.to_datetime(n["ending_date"]),
                "course": str(n.get("course") or "")}
    except (KeyError, TypeError) as e:
        raise ValueError(
            "tournament_config must carry a 'new' block with season, name, "
            f"ending_date and course - missing {e}") from None


def archived_slates(directory: str = SALARY_DIR) -> pd.DataFrame:
    """Every saved salary file, oldest first. Reads the folder, nothing else."""
    rows = []
    for path in sorted(glob.glob(os.path.join(directory, "dk-*.csv"))):
        m = _NAME_RE.match(os.path.basename(path))
        if not m:
            continue
        rows.append({
            "path": path,
            "season": int(m.group("season")),
            "ending_date": pd.Timestamp(m.group("ending")),
            "slug": m.group("slug"),
        })
    df = pd.DataFrame(rows, columns=["path", "season", "ending_date", "slug"])
    if len(df):
        df = df.sort_values(["season", "ending_date"]).reset_index(drop=True)
    return df


def find_archive(config: dict, directory: str = SALARY_DIR) -> str:
    """The saved salary file for the configured tournament. NO NETWORK.

    Raises rather than falling back to the newest file. A stale archive is the
    one failure this cannot fix for itself and the one that does not look like
    a failure: last week's prices land on real players at plausible salaries
    and every screen downstream looks ordinary.
    """
    want = _new(config)
    have = archived_slates(directory)
    hit = have[(have["season"] == want["season"])
               & (have["slug"] == slugify(want["name"]))]
    if not len(hit):
        listing = "\n".join(f"      {os.path.basename(p)}"
                            for p in have.tail(4)["path"]) or "      (empty)"
        raise FileNotFoundError(
            f"no saved DraftKings prices for {want['name']!r} "
            f"(season {want['season']}).\n"
            f"    {directory}/ holds:\n{listing}\n"
            f"    Run the 'Download DraftKings salaries' cell on a machine that "
            f"can reach DraftKings,\n"
            f"    commit {directory}/, then git pull here.")
    # A slug can only repeat within a season if the same tournament was saved
    # twice under different ending dates - a corrected `new_ending_date`. The
    # later file is the corrected one.
    return str(hit.sort_values("ending_date").iloc[-1]["path"])


def _meta_path(csv_path: str) -> str:
    return csv_path[:-len(".csv")] + _META_SUFFIX


def read_meta(csv_path: str) -> dict:
    """What the fetch knew: draft group, DK's own name for the tournament, its
    venue, the first tee time, and WHEN THE FETCH RAN - which is the freshness
    answer nothing inside the CSV can give."""
    try:
        with open(_meta_path(csv_path), encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def write_meta(csv_path: str, meta: dict) -> str:
    path = _meta_path(csv_path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1, default=str)
    os.replace(tmp, path)
    return path


# ---------------------------------------------------------------------------
# which draft group - the tournament, not the lobby's idea of it
# ---------------------------------------------------------------------------

def _tokens(name: str) -> set:
    # "the", "championship" and friends are left IN. They carry little signal
    # on their own, but dropping them makes the events whose names are
    # otherwise one word apart collide.
    return set(t for t in slugify(name).split("-") if t)


def describe_slates(slates: pd.DataFrame, *, use_cache: bool = False,
                    verbose: bool = True) -> pd.DataFrame:
    """Each classic draft group with the tournament it actually prices.

    Costs one request per group, and there are two or three: the lobby names
    the TOUR (" (PGA TOUR)") but not the event, and the event name is the only
    thing that can be checked against the notebook's own config.
    """
    out = []
    for _, s in slates.iterrows():
        dg = int(s["draft_group"])
        try:
            payload = draftables(dg, use_cache=use_cache)
            players = len(payload.get("draftables") or [])
            # DRAFT GROUPS ARE POSTED BEFORE THEY ARE FILLED. An empty one
            # carries no competition either, so it would otherwise be reported
            # as unreadable - which reads like a fault rather than a slate DK
            # has not opened yet.
            if not players:
                if verbose:
                    print(f"  dg{dg}  no field posted yet - skipped")
                continue
            comp = competition(payload)
        except (FileNotFoundError, ValueError, RuntimeError) as e:
            if verbose:
                print(f"  dg{dg}  unreadable ({e})")
            continue
        out.append({"draft_group": dg, "tour": s.get("tour", ""),
                    "tournament": comp["name"], "venue": comp["venue"],
                    "start": comp["start"], "players": players})
    return pd.DataFrame(out, columns=["draft_group", "tour", "tournament",
                                      "venue", "start", "players"])


def pick_draft_group(described: pd.DataFrame, tournament_name: str) -> dict:
    """The draft group whose tournament is the one the notebook is configured
    for - matched on the EVENT NAME, never on "the first PGA TOUR one".

    The golf lobby carries several tours at once (PGA TOUR, DP World Tour,
    Korn Ferry), each a plausible-looking field of real golfers at real
    salaries. Picking the wrong one produces a field that is wrong about every
    single player while looking entirely normal, so the name has to agree.
    """
    if not len(described):
        raise LookupError("DraftKings is advertising no salary-cap golf slate.")

    want = slugify(tournament_name)
    exact = described[described["tournament"].map(slugify) == want]
    if len(exact) == 1:
        return dict(exact.iloc[0], match="exact")
    if len(exact) > 1:
        # The same event priced by two groups (an early-week and a late-week
        # posting). The wider field is the later, more complete one.
        return dict(exact.sort_values("players").iloc[-1], match="exact")

    # NO EXACT MATCH. Offer the near misses rather than guessing: a loose match
    # is accepted only when exactly one candidate shares most of its words with
    # the configured name, and it SAYS SO when it does.
    want_t = _tokens(tournament_name)
    scored = described.assign(overlap=described["tournament"].map(
        lambda n: len(want_t & _tokens(n)) / max(1, len(want_t | _tokens(n)))))
    close = scored[scored["overlap"] >= 0.5]
    listing = "\n".join(
        f"      dg{int(r.draft_group):<8} {r.tournament}  ({r.tour}, "
        f"{r.players} players)" for r in scored.itertuples())
    if len(close) == 1:
        return dict(close.iloc[0], match="loose")
    raise LookupError(
        f"no DraftKings slate matches new_tournament_name={tournament_name!r}.\n"
        f"    On offer right now:\n{listing}\n"
        f"    Either fix new_tournament_name in USER INPUTS, or pin the group:\n"
        f"      dk_api.refresh_from_dk(tournament_config, draft_group=<id>)")


# ---------------------------------------------------------------------------
# step 1 - the fetch.  HOME ONLY.
# ---------------------------------------------------------------------------

def refresh_from_dk(config: dict, *, draft_group: int | None = None,
                    use_cache: bool = False, directory: str = SALARY_DIR,
                    verbose: bool = True) -> pd.DataFrame:
    """DraftKings -> `data/salaries/dk-<season>-<MM-DD>-<slug>.csv`.

    THE ONLY FUNCTION HERE THAT TOUCHES THE NETWORK. It writes the tournament's
    archive file and a small `-meta.json` beside it, and nothing else; the
    field is built from those files by `load_field`, on both machines, so there
    is no code path that runs only at home.

    `use_cache=False` by default, and that is deliberate: the only reason to
    run this again is that something has moved - a price, a withdrawal, a late
    commitment - and a cached read would report no change with no error.

    Safe to run again later in the week. An identical file reports `unchanged`
    and touches nothing; a differing one REPLACES it and says so, because the
    reason to download again is that the later file describes the contest you
    actually enter.
    """
    want = _new(config)

    if draft_group is None:
        slates = golf_slates()
        if verbose:
            print(f"  DraftKings lobby  {len(slates)} salary-cap golf slate(s)")
        described = describe_slates(slates, use_cache=use_cache, verbose=verbose)
        pick = pick_draft_group(described, want["name"])
        if pick["match"] == "loose" and verbose:
            print(f"  ** matched LOOSELY: DraftKings calls it "
                  f"{pick['tournament']!r}, USER INPUTS says {want['name']!r}")
    else:
        payload = draftables(int(draft_group), use_cache=use_cache)
        comp = competition(payload)
        pick = {"draft_group": int(draft_group), "tour": "(pinned)",
                "tournament": comp["name"], "venue": comp["venue"],
                "start": comp["start"],
                "players": len(payload.get("draftables") or []), "match": "pinned"}

    dg = int(pick["draft_group"])
    payload = draftables(dg, use_cache=True)   # already fetched just above
    df = to_dk_csv(payload)

    # THE FILE IS NAMED FROM THE CONFIG, NOT FROM DRAFTKINGS, so that the file
    # `load_field` looks for is the one this writes even when the two spell the
    # tournament differently. DK's own spelling is kept in the meta and in the
    # CSV's `Game Info`, so the difference is recorded rather than erased.
    path = os.path.join(directory, archive_name(
        want["season"], want["ending_date"], want["name"]))
    before = None
    if os.path.exists(path):
        with open(path, "rb") as f:
            before = f.read()
    write_dk_csv(df, path)
    with open(path, "rb") as f:
        after = f.read()

    write_meta(path, {
        "draft_group": dg,
        "dk_tournament": pick["tournament"],
        "dk_venue": pick["venue"],
        "dk_start": pick["start"],
        "config_tournament": want["name"],
        "config_course": want["course"],
        "config_season": want["season"],
        "config_ending_date": want["ending_date"].strftime("%Y-%m-%d"),
        "name_match": pick["match"],
        "players": int(len(df)),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })

    if verbose:
        tee = pick["start"]
        tee = (tee.tz_convert(ET).strftime("%a %m/%d %I:%M%p ET")
               if pd.notna(tee) else "?")
        print(f"  dg{dg}  {pick['tournament']}  -  {pick['venue'] or '?'}")
        print(f"  first tee {tee}   {len(df)} players priced   "
              f"${df['Salary'].min():,.0f}-${df['Salary'].max():,.0f}")
        state = ("unchanged" if before == after
                 else "REPLACED (prices moved)" if before is not None else "written")
        print()
        print(f"  {path}   [{state}]")
        if before != after:
            print(f"  commit {directory}/ - DraftKings will not serve these again.")
    return df


# ---------------------------------------------------------------------------
# step 2 - the field.  NO NETWORK, EVER.
# ---------------------------------------------------------------------------

def load_field(config: dict, *, directory: str = SALARY_DIR,
               verbose: bool = True) -> pd.DataFrame:
    """`data/salaries/` -> the `dk` frame: one row per priced player.

    PLAYER and SALARY, names normalised to the database's spelling - exactly
    what the hand-loaded `pd.read_csv("data/DKSalaries.csv")` produced, and the
    same two columns the rest of the pipeline reads.

    MAKES NO REQUEST TO DRAFTKINGS under any circumstance, and depends on
    nothing `refresh_from_dk` leaves in memory. That is what makes "skip step 1
    at work" a workflow rather than a trick.
    """
    from utils.db_utils import DK_PLAYER_NAME_MAP, standardize_player_names

    want = _new(config)
    path = find_archive(config, directory)
    meta = read_meta(path)

    raw = pd.read_csv(path, usecols=["Name", "Salary"], encoding="utf-8-sig")
    dk = raw.rename(columns={"Name": "PLAYER", "Salary": "SALARY"})
    dk["PLAYER"] = dk["PLAYER"].replace(DK_PLAYER_NAME_MAP)
    dk = standardize_player_names(dk)

    if verbose:
        print(f"{len(dk)} players in DK field   (saved prices, no network)")
        print(f"  {path}")
        fetched = meta.get("fetched_at")
        if fetched:
            when = pd.to_datetime(fetched, utc=True, format="ISO8601")
            age = (pd.Timestamp.now(tz="UTC") - when).total_seconds() / 3600
            print(f"  fetched {when.tz_convert(ET):%a %m/%d %I:%M%p ET} "
                  f"({age:.0f}h ago) from dg{meta.get('draft_group')}")
        else:
            print("  no -meta.json beside it: age unknown "
                  "(hand-downloaded, or saved before the meta existed)")
        # DK NAMES THE VENUE INDEPENDENTLY OF THE NOTEBOOK, and `new_course` is
        # joined to course history as an EXACT STRING - so a disagreement here
        # is worth a look even though neither side is authoritative.
        venue = (meta.get("dk_venue") or "").strip()
        if venue and want["course"] and slugify(venue) != slugify(want["course"]):
            print(f"  ** DraftKings calls the venue {venue!r}; "
                  f"new_course is {want['course']!r}")
        if meta.get("name_match") == "loose":
            print(f"  ** the fetch matched the tournament name loosely: DK said "
                  f"{meta.get('dk_tournament')!r}")
    return dk
