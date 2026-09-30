"""One-time repair of data/golf.db from data/pga.db, so the old pipeline trains on
correct history while the new one is built. Retire with golf.db.

    from pga_api import golf_db_repair as fix
    plan = fix.plan()            # reads only; every change as a table
    fix.apply(plan)              # writes golf.db, in one transaction

Every change is decided by the parity check in compare.py; round scores it
writes were checked against hole-by-hole scorecards.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

from pga_api import compare
from pga_api.identity import ALIASES, Resolver, _tokens, same_golfer_name
from utils.db_utils import TOURNAMENT_NAME_MAP


def _load():
    gt, gs, ev, res, rnd, pl, st = compare._load()
    return gt, ev, res, rnd, pl


def _rounds_wide(rnd: pd.DataFrame) -> pd.DataFrame:
    w = rnd.pivot_table(index=["tournament_id", "player_id"], columns="round",
                        values=["strokes", "to_par"], aggfunc="first")
    w.columns = [f"{a}_{b}" for a, b in w.columns]
    return w


def _scores_agree(golf_row, api_row) -> bool:
    """Every round golf.db has equals the API's, and at least two exist."""
    n = 0
    for i in (1, 2, 3, 4):
        v = golf_row.get(f"ROUNDS:{i}")
        if str(v).strip() in ("", "-", "--", "None", "nan"):
            continue
        if not compare._same_round(v, api_row.get(f"strokes_{i}"), api_row.get(f"to_par_{i}")):
            return False
        n += 1
    return n >= 2


def seed_aliases(write: bool = False) -> pd.DataFrame:
    """Names in golf.db that no rule resolves, paired by identical round scores
    with an unmatched golfer in the same event. Kept only if the pairing is the
    same golfer every time and the surnames agree or it repeats in 2+ events."""
    gt, ev, res, rnd, pl = _load()
    R = Resolver(ev, res, pl)
    m = compare.match_events(gt, ev)
    m = m[m["found"] & ~m["id_wrong"] & ~m["claimed_twice"]]
    g = gt.merge(m[["SEASON", "ENDING_DATE", "TOURNAMENT", "tournament_id"]])
    wide = _rounds_wide(rnd)
    names = pl.set_index("player_id")["name"]
    votes: dict[str, Counter] = defaultdict(Counter)
    for tid, gg in g.groupby("tournament_id"):
        resolved = {R.resolve(n, tid)[0] for n in gg["PLAYER"]}
        field = R.field(tid)
        free = [p for p in field["player_id"] if p not in resolved]
        for _, row in gg.iterrows():
            if R.resolve(row["PLAYER"], tid)[0] is not None:
                continue
            hits = [p for p in free if (tid, p) in wide.index and _scores_agree(row, wide.loc[(tid, p)])]
            if len(hits) == 1:
                votes[row["PLAYER"]][hits[0]] += 1
    rows = []
    for name, c in votes.items():
        (pid, n), = c.most_common(1)
        same_surname = _tokens(name)[-1:] == _tokens(names[pid])[-1:]
        keep = len(c) == 1 and (same_surname or n >= 2)
        rows.append({"name": name, "player_id": pid, "api_name": names[pid], "events": n,
                     "other_candidates": len(c) - 1, "keep": keep})
    out = pd.DataFrame(rows).sort_values(["keep", "name"], ascending=[False, True])
    if write:
        k = out[out["keep"]]
        pd.DataFrame({"name": k["name"], "player_id": k["player_id"],
                      "evidence": k.apply(lambda r: f"golf.db round scores = {r.api_name} "
                                                    f"in {r.events} event(s)", axis=1)}
                     ).to_csv(ALIASES, index=False)
    return out


# ---------------------------------------------------------------- the repair

KEY = ["SEASON", "ENDING_DATE", "TOURNAMENT"]


def _fmt_to_par(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    v = int(v)
    return "E" if v == 0 else f"{v:+d}"


def _row_format(row: pd.Series, season: int) -> str:
    """'to_par' or 'strokes': how this golf.db row already stores its rounds."""
    for i in (1, 2, 3, 4):
        s = str(row.get(f"ROUNDS:{i}", "")).strip()
        if (s.startswith(("+", "-")) and s not in ("-", "--")) or s == "E":
            return "to_par"
        if s.isdigit() and int(s) >= 50:
            return "strokes"
    return "to_par" if season >= 2023 else "strokes"


def _round_value(api: pd.Series, i: int, fmt: str):
    if fmt == "strokes":
        v = api.get(f"strokes_{i}")
        return None if v is None or pd.isna(v) else str(int(v))
    return _fmt_to_par(api.get(f"to_par_{i}"))


def _final_pos(pos: str) -> int:
    d = "".join(ch for ch in str(pos) if ch.isdigit())
    return int(d) if d else 90


def _clean_event_name(name: str) -> str:
    for suffix in (" (2020)", " (2021)"):
        name = name.replace(suffix, "")
    return TOURNAMENT_NAME_MAP.get(name, name)


def plan() -> dict:
    """Every change the repair would make, as tables. Reads only."""
    gt, ev, res, rnd, pl = _load()
    R = Resolver(ev, res, pl)
    m = compare.match_events(gt, ev)
    g = gt.merge(m[KEY + ["tournament_id", "golf_id"]], on=KEY)
    out = [R.resolve(n, t) for n, t in zip(g["PLAYER"], g["tournament_id"])]
    g["player_id"], g["how"] = [o[0] for o in out], [o[1] for o in out]
    fields = {t: set(f["player_id"]) for t, f in R._fields.items()}
    g["in_field"] = [p in fields.get(t, set()) for p, t in zip(g["player_id"], g["tournament_id"])]
    wide = _rounds_wide(rnd)
    g["scores_agree"] = [
        f and (t, p) in wide.index and _scores_agree(row, wide.loc[(t, p)])
        for f, t, p, row in zip(g["in_field"], g["tournament_id"], g["player_id"],
                                g.to_dict("records"))]

    # Whole events. The golfers' round scores mostly not what they shot that
    # week: the rows are another event's results. Being in the field is not
    # enough: the BMW field is a subset of the St. Jude's. Two golf.db events
    # claiming one Tour event: the one closer to the field's size stays.
    evs = g.groupby(KEY + ["tournament_id", "golf_id"]).agg(
        rows=("PLAYER", "size"), in_field=("in_field", "mean"),
        scores_agree=("scores_agree", "mean")).reset_index()
    evs["field_size"] = evs["tournament_id"].map(ev.set_index("tournament_id")["field_size"])
    evs["misfiled"] = evs["scores_agree"] < 0.5
    ok = evs[~evs["misfiled"]].assign(size_gap=lambda d: (d["rows"] - d["field_size"]).abs())
    keep_idx = ok.sort_values("size_gap").drop_duplicates("tournament_id").set_index(KEY).index
    evs["duplicate"] = ~evs["misfiled"] & ~evs.set_index(KEY).index.isin(keep_idx)
    drop_events = evs[evs["misfiled"] | evs["duplicate"]]
    kept = evs[~(evs["misfiled"] | evs["duplicate"])]
    fix_ids = kept[kept["golf_id"] != kept["tournament_id"]]

    gk = g.merge(kept[KEY], on=KEY)
    junk = gk[gk["POS"].fillna("").str.strip() == ""]
    gk = gk.drop(junk.index)

    # Row fixes, only on golfers who were in the field.
    wide = _rounds_wide(rnd)
    api = res.set_index(["tournament_id", "player_id"])
    src = ev.set_index("tournament_id")["source"]
    fixes = []
    for _, row in gk[gk["in_field"]].iterrows():
        k = (row["tournament_id"], row["player_id"])
        a = api.loc[k]
        w = wide.loc[k] if k in wide.index else pd.Series(dtype=object)
        fmt = _row_format(row, int(row["SEASON"]))
        change = {}
        for i in (1, 2, 3, 4):
            new = _round_value(w, i, fmt)
            if new is not None and not compare._same_round(
                    row[f"ROUNDS:{i}"], w.get(f"strokes_{i}"), w.get(f"to_par_{i}")):
                change[f"ROUNDS:{i}"] = new
        if str(row["POS"]).strip() != str(a["position"]).strip():
            change["POS"] = a["position"]
            change["FINAL_POS"] = _final_pos(a["position"])
        if src[row["tournament_id"]] == "past_results":
            for gcol, acol, tol, f in (("FEDEX_CUP_POINTS", "fedex_points", 0.01, "{:.3f}"),
                                       ("OFFICIAL_MONEY", "official_money", 1.0, "${:,.2f}")):
                gv = compare._num(pd.Series([row[gcol]])).iloc[0]
                av = a[acol]
                if pd.notna(av) and abs((0 if pd.isna(gv) else gv) - av) >= tol:
                    change[gcol] = f.format(av)
        if change:
            fixes.append({**{c: row[c] for c in KEY}, "PLAYER": row["PLAYER"],
                          "player_id": row["player_id"], "tournament_id": row["tournament_id"],
                          "changes": change})

    # golf.db's own spelling of each golfer, so inserted rows join with his history.
    canon = (g[g["player_id"].notna()].groupby("player_id")["PLAYER"]
             .agg(lambda s: s.value_counts().index[0]))
    owner = {n: p for p, n in canon.items()}
    names = pl.set_index("player_id")["name"]

    def golf_name(pid):
        if pid in canon:
            return canon[pid]
        n = compare.std_name(names[pid])
        return n if owner.get(n, pid) == pid else f"{n} ({pid})"   # a second Zach Johnson

    ev_i = ev.set_index("tournament_id")

    def new_row(tid, pid, meta, why):
        a = api.loc[(tid, pid)]
        w = wide.loc[(tid, pid)] if (tid, pid) in wide.index else pd.Series(dtype=object)
        fmt = "to_par" if ev_i.loc[tid, "scoring"] == "stableford" or meta["SEASON"] >= 2023 else "strokes"
        r = {**meta, "TOURN_ID": tid, "PLAYER": golf_name(pid),
             "POS": a["position"], "FINAL_POS": _final_pos(a["position"]),
             "OFFICIAL_MONEY": None if pd.isna(a["official_money"]) else f"${a['official_money']:,.2f}",
             "FEDEX_CUP_POINTS": None if pd.isna(a["fedex_points"]) else f"{a['fedex_points']:.3f}",
             "why": why}
        for i in (1, 2, 3, 4):
            r[f"ROUNDS:{i}"] = _round_value(w, i, fmt)
        return r

    inserts = []
    present = set(zip(gk["tournament_id"], gk["player_id"]))
    course_of = gt.drop_duplicates(KEY).set_index(KEY)["COURSE"]
    for e in kept.itertuples():
        meta = {"SEASON": int(e.SEASON), "ENDING_DATE": e.ENDING_DATE, "TOURNAMENT": e.TOURNAMENT,
                "COURSE": course_of.loc[(e.SEASON, e.ENDING_DATE, e.TOURNAMENT)]}
        for pid in res.loc[res["tournament_id"] == e.tournament_id, "player_id"]:
            if (e.tournament_id, pid) not in present:
                inserts.append(new_row(e.tournament_id, pid, meta, "golfer missing from event"))
    course_map = (m[m["found"]].groupby("course")["COURSE"]
                  .agg(lambda s: s.value_counts().index[0]).to_dict())
    kept_ids = set(kept["tournament_id"])
    seasons = range(int(gt["SEASON"].min()), int(gt["SEASON"].max()) + 1)
    new_events = ev[(ev["kind"] == "stroke") & ev["season"].isin(seasons)
                    & ~ev["tournament_id"].isin(kept_ids)]
    for e in new_events.itertuples():
        meta = {"SEASON": int(e.season), "ENDING_DATE": str(e.end_date),
                "TOURNAMENT": _clean_event_name(e.name), "COURSE": course_map.get(e.course, e.course)}
        for pid in res.loc[res["tournament_id"] == e.tournament_id, "player_id"]:
            inserts.append(new_row(e.tournament_id, pid, meta, "event missing"))
    inserts = pd.DataFrame(inserts)

    # Odds for inserted events are renamed to the event's name and date, so the
    # old pipeline's (TOURNAMENT, ENDING_DATE) join finds them.
    with sqlite3.connect(compare.GOLF_DB) as con:
        odds = pd.read_sql("SELECT SEASON, TOURNAMENT, ENDING_DATE, PLAYER FROM odds", con)
    new_ids = set(new_events["tournament_id"])
    target = inserts[inserts["why"] == "event missing"].drop_duplicates("TOURN_ID").set_index("TOURN_ID")
    renames = []
    for (s, d, t), o in odds.groupby(KEY):
        tid, share = R.match_event(d, o["PLAYER"].tolist())
        if tid in new_ids:
            renames.append({"SEASON": s, "TOURNAMENT": t, "ENDING_DATE": d, "rows": len(o),
                            "share": round(share, 2), "tournament_id": tid,
                            "new_TOURNAMENT": target.loc[tid, "TOURNAMENT"],
                            "new_ENDING_DATE": target.loc[tid, "ENDING_DATE"]})
    renames = pd.DataFrame(renames)
    if len(renames):   # two odds events for one Tour event: the better-matched one
        renames = renames.sort_values("share", ascending=False).drop_duplicates("tournament_id")

    return {"drop_events": drop_events, "fix_ids": fix_ids, "junk_rows": junk,
            "row_fixes": pd.DataFrame(fixes), "inserts": inserts, "odds_renames": renames}


TOURN_COLS = ["SEASON", "ENDING_DATE", "TOURN_ID", "TOURNAMENT", "COURSE", "PLAYER", "POS",
              "FINAL_POS", "ROUNDS:1", "ROUNDS:2", "ROUNDS:3", "ROUNDS:4",
              "OFFICIAL_MONEY", "FEDEX_CUP_POINTS"]
_WHERE = '"SEASON" = ? AND "ENDING_DATE" = ? AND "TOURNAMENT" = ?'


def apply(p: dict, backup_to: str) -> dict:
    """Write the plan into golf.db in one transaction, after copying it to `backup_to`."""
    import shutil
    shutil.copy2(compare.GOLF_DB, backup_to)
    done = Counter()
    con = sqlite3.connect(compare.GOLF_DB)
    try:
        with con:
            for e in p["drop_events"].itertuples():
                done["event rows deleted"] += con.execute(
                    f"DELETE FROM tournaments WHERE {_WHERE}",
                    (int(e.SEASON), e.ENDING_DATE, e.TOURNAMENT)).rowcount
            for e in p["fix_ids"].itertuples():
                done["TOURN_ID corrected"] += con.execute(
                    f'UPDATE tournaments SET "TOURN_ID" = ? WHERE {_WHERE}',
                    (e.tournament_id, int(e.SEASON), e.ENDING_DATE, e.TOURNAMENT)).rowcount
            for r in p["junk_rows"].itertuples():
                done["junk rows deleted"] += con.execute(
                    f'DELETE FROM tournaments WHERE {_WHERE} AND "PLAYER" = ?',
                    (int(r.SEASON), r.ENDING_DATE, r.TOURNAMENT, r.PLAYER)).rowcount
            for r in p["row_fixes"].itertuples():
                cols = ", ".join(f'"{c}" = ?' for c in r.changes)
                done["rows corrected"] += con.execute(
                    f'UPDATE tournaments SET {cols} WHERE {_WHERE} AND "PLAYER" = ?',
                    (*r.changes.values(), int(r.SEASON), r.ENDING_DATE, r.TOURNAMENT, r.PLAYER)).rowcount
            ins = p["inserts"]
            if len(ins):
                ins = ins.assign(TOURN_ID=ins["TOURN_ID"])[TOURN_COLS]
                ins = ins.astype(object).where(ins.notna(), None)
                con.executemany(
                    f"INSERT INTO tournaments ({', '.join(chr(34) + c + chr(34) for c in TOURN_COLS)}) "
                    f"VALUES ({', '.join('?' * len(TOURN_COLS))})",
                    ins.itertuples(index=False, name=None))
                done["rows inserted"] += len(ins)
            new_season = (p["inserts"].drop_duplicates("TOURN_ID").set_index("TOURN_ID")["SEASON"]
                          if len(p["inserts"]) else {})
            for r in p["odds_renames"].itertuples():
                done["odds rows renamed"] += con.execute(
                    'UPDATE odds SET "SEASON" = ?, "TOURNAMENT" = ?, "ENDING_DATE" = ? '
                    'WHERE "SEASON" = ? AND "TOURNAMENT" = ? AND "ENDING_DATE" = ?',
                    (int(new_season[r.tournament_id]), r.new_TOURNAMENT, r.new_ENDING_DATE,
                     int(r.SEASON), r.TOURNAMENT, r.ENDING_DATE)).rowcount
            dup = con.execute('SELECT COUNT(*) FROM (SELECT 1 FROM tournaments GROUP BY '
                              '"ENDING_DATE", "TOURNAMENT", "PLAYER" HAVING COUNT(*) > 1)').fetchone()[0]
            if dup:
                raise RuntimeError(f"{dup} duplicate (date, event, player) keys; rolled back")
    finally:
        con.close()
    return dict(done)


def summarize(p: dict) -> None:
    print(f"events deleted (misfiled or duplicate): {len(p['drop_events'])}")
    print(f"events with TOURN_ID corrected:         {len(p['fix_ids'])}")
    print(f"junk rows deleted:                      {len(p['junk_rows'])}")
    rf = p["row_fixes"]
    if len(rf):
        cols = Counter(c.split(":")[0] for ch in rf["changes"] for c in ch)
        print(f"rows corrected:                         {len(rf)}  {dict(cols)}")
    ins = p["inserts"]
    if len(ins):
        print(ins.groupby("why").agg(rows=("PLAYER", "size"), events=("TOURN_ID", "nunique")).to_string())
    print(f"odds events renamed to join:            {len(p['odds_renames'])}")
