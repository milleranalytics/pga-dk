import { useCallback, useMemo, useState } from "react";
import { c, font } from "./tokens";
import { loadSlate, servedOverHttp } from "./loadSlate";
import type { Slate } from "./types";
import { enrich } from "./enrich";
import { useBuildState } from "./persist";
import type { SavedLineup, BuildState } from "./persist";
import {
  toggleLock as lockEdit,
  toggleExclude as excludeEdit,
  removeFromBuild,
  clearConstraints as wipeConstraints,
} from "./build";
import { optimize, generate, whyInfeasible } from "./optimizer";
import type { OptPlayer, GenResult, BuildContext } from "./optimizer";
import TopBar from "./panels/TopBar";
import type { Tab } from "./panels/TopBar";
import FieldGrid, { initialDir } from "./panels/FieldGrid";
import type { SortKey } from "./panels/FieldGrid";
import PlayerCard from "./panels/PlayerCard";
import LineupRail from "./panels/LineupRail";
import Tracker from "./panels/Tracker";
import ResultsBrowser from "./panels/ResultsBrowser";
import DbQuery from "./panels/DbQuery";
import CourseExplorer from "./panels/CourseExplorer";
import SgRankings from "./panels/SgRankings";

const GEN_COUNT = 5;
const MAX_EXPOSURE = 60; // percent, across the full saved set

export default function App() {
  const status = useMemo(() => loadSlate(), []);
  if (!status.ok) return <NoData detail={status.detail} />;
  return <Workspace slate={status.slate} />;
}

function Workspace({ slate }: { slate: Slate }) {
  const field = useMemo(() => enrich(slate), [slate]);
  const [build, setBuild, sync] = useBuildState(field.meta);

  const [tab, setTab] = useState<Tab>("slate");
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("P_TOP20");
  const [sortDir, setSortDir] = useState<1 | -1>(-1);
  const [selected, setSelected] = useState<string | null>(null);

  const onSort = useCallback(
    (k: SortKey) => {
      if (k === sortKey) setSortDir((d) => (d === 1 ? -1 : 1));
      else {
        setSortKey(k);
        setSortDir(initialDir(k));
      }
    },
    [sortKey],
  );

  /** Exposure across saved lineups, as a percentage. Derived every render —
   *  cheap at this scale, and it must react to every save/delete. */
  const exposure = useMemo(() => {
    const m = new Map<string, number>();
    if (build.saved.length === 0) return m;
    for (const l of build.saved) {
      for (const id of l.ids) m.set(id, (m.get(id) ?? 0) + 1);
    }
    for (const [k, v] of m) m.set(k, (v / build.saved.length) * 100);
    return m;
  }, [build.saved]);

  const optPool: OptPlayer[] = useMemo(
    () => field.players.map((p) => ({ id: p.id, salary: p.SALARY, value: p.P_TOP20 })),
    [field.players],
  );

  /**
   * The solver's view of the week. `pickedIds` is deliberately empty: both
   * Optimize and Gen build a roster from scratch around the LOCKS, so the
   * current build is an output of the optimizer, never an input to it. (The
   * field still exists in BuildContext because pre-committing players is what
   * makes the DP's search space small — Gen's own branching uses it.)
   */
  const ctx = useMemo(
    () => ({
      all: optPool,
      lockedIds: new Set(Object.keys(build.locks)),
      excludedIds: new Set(Object.keys(build.excludes)),
      pickedIds: [],
      slots: field.meta.roster,
      cap: field.meta.cap,
    }),
    [optPool, build.locks, build.excludes, field.meta],
  );

  /** Set whenever a solve could not produce a lineup, or Gen came up short. */
  const [note, setNote] = useState<string | null>(null);

  /**
   * Edit the constraints and re-solve around the result, in one press.
   *
   * This is what L and X do now. Locking a player used to set a flag and stop,
   * so on a full roster the press had no visible effect at all and you had to
   * press Optimize to see it land — "lock him, then optimize" was two steps for
   * one intention, every time. The solve is an exact DP over ~150 players and
   * costs about a millisecond, so there is no reason to make you ask for it.
   *
   * On an INFEASIBLE constraint set the edit is still applied but the build is
   * left alone, and the rail says which constraint ran out. Keeping the edit
   * matters: locking a seventh player is how you discover you have seven locks,
   * and silently refusing the lock would leave nothing on screen to undo.
   *
   * Solved outside the updater so the note can be set from the same result —
   * updaters must stay pure, and this reads `build` the same way onGenerate
   * does.
   */
  const applyConstraints = useCallback(
    (edit: (s: BuildState) => BuildState) => {
      const next = edit(build);
      const solveCtx: BuildContext = {
        all: optPool,
        lockedIds: new Set(Object.keys(next.locks)),
        excludedIds: new Set(Object.keys(next.excludes)),
        pickedIds: [],
        slots: field.meta.roster,
        cap: field.meta.cap,
      };
      const r = optimize(solveCtx);
      setNote(r ? null : `No lineup — ${whyInfeasible(solveCtx)}`);
      setBuild(() => (r ? { ...next, picks: r.map((p) => p.id) } : next));
    },
    [build, optPool, field.meta, setBuild],
  );

  const toggleLock = useCallback(
    (id: string) => applyConstraints((s) => lockEdit(s, id)),
    [applyConstraints],
  );

  const toggleExclude = useCallback(
    (id: string) => applyConstraints((s) => excludeEdit(s, id)),
    [applyConstraints],
  );

  // CLR is the one constraint edit that does NOT re-solve — see build.ts. The
  // lineup on the rail stays exactly as it is; it is simply unconstrained now.
  const clearConstraints = useCallback(() => {
    setNote(null);
    setBuild(wipeConstraints);
  }, [setBuild]);

  /**
   * Re-solve from scratch, keeping only what is LOCKED.
   *
   * It used to fill around the current build, which meant Optimize was a no-op
   * on a full roster and re-optimizing took a Clear first, every time. Every L
   * and X press now runs this same solve, so the button is no longer the only
   * way to reach it — what is left for it is the unconstrained case (no locks,
   * no exclusions, just "build me the best lineup") and re-solving after a slot
   * has been emptied by hand.
   *
   * No longer silent when it fails. `optimize` returns null for three different
   * reasons — too many locks, locks over the cap, nothing left that fits — and
   * all three used to be reported by the button simply not doing anything.
   */
  const onOptimize = useCallback(() => {
    const r = optimize(ctx); // ctx.pickedIds is empty by construction
    setNote(r ? null : `No lineup — ${whyInfeasible(ctx)}`);
    if (!r) return;
    setBuild((s) => ({ ...s, picks: r.map((p) => p.id) }));
  }, [ctx, setBuild]);

  // Solved outside the setBuild updater on purpose: updaters must stay pure
  // (StrictMode runs them twice), and this one both reports a note and is a
  // few tens of milliseconds of search.
  const onGenerate = useCallback(() => {
    const r = generate(ctx, GEN_COUNT, build.saved.map((l) => l.ids), MAX_EXPOSURE);
    setNote(r.lineups.length < GEN_COUNT ? genShortfall(r, GEN_COUNT, MAX_EXPOSURE) : null);
    if (r.lineups.length === 0) return;
    const added: SavedLineup[] = r.lineups.map((ids) => ({ ids }));
    setBuild((s) => ({ ...s, saved: [...s.saved, ...added] }));
  }, [ctx, build.saved, setBuild]);

  /** Click-through from the Course / SG tables: select the player AND jump to
   *  the workspace, so the card is actually visible. Mirrors the Streamlit
   *  app's "click a row to open that player in Player Detail". */
  const openPlayer = useCallback((id: string) => {
    setSelected(id);
    setTab("slate");
  }, []);

  const onSave = useCallback(() => {
    setBuild((s) => {
      if (s.picks.length !== field.meta.roster) return s;
      const key = [...s.picks].sort().join("|");
      if (s.saved.some((l) => [...l.ids].sort().join("|") === key)) return s;
      return { ...s, saved: [...s.saved, { ids: [...s.picks] }] };
    });
  }, [setBuild, field.meta.roster]);

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <TopBar
        meta={field.meta}
        tab={tab}
        onTab={setTab}
        query={query}
        onQuery={setQuery}
        resultsEnabled={servedOverHttp}
      />

      {tab === "slate" ? (
        <div style={{ flex: 1, display: "flex", overflowX: "auto", minHeight: 0 }}>
          <FieldGrid
            field={field}
            query={query}
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={onSort}
            selected={selected}
            onSelect={setSelected}
            picks={build.picks}
            locks={build.locks}
            excludes={build.excludes}
            exposure={exposure}
            savedCount={build.saved.length}
            onToggleLock={toggleLock}
            onToggleExclude={toggleExclude}
            onClearConstraints={clearConstraints}
          />
          <PlayerCard
            field={field}
            player={selected ? (field.byId.get(selected) ?? null) : null}
            inLineup={selected ? build.picks.includes(selected) : false}
            locked={selected ? !!build.locks[selected] : false}
            excluded={selected ? !!build.excludes[selected] : false}
          />
          <LineupRail
            field={field}
            picks={build.picks}
            locks={build.locks}
            saved={build.saved}
            genCount={GEN_COUNT}
            maxExposure={MAX_EXPOSURE}
            syncStatus={sync.status}
            note={note}
            // Clicking a filled slot means "get this player out of my lineup",
            // so it drops the lock too — otherwise the constraints would claim
            // a player the build does not contain. It deliberately does NOT
            // re-solve: this is the one action whose whole purpose is to leave
            // the slot empty, and a solve would hand it straight back to the
            // same player whenever he was still the best available.
            onRemove={(id) => setBuild((s) => removeFromBuild(s, id))}
            onOptimize={onOptimize}
            onGenerate={onGenerate}
            onSave={onSave}
            onClear={() => setBuild((s) => ({ ...s, picks: [] }))}
            onLoadSaved={(l) => setBuild((s) => ({ ...s, picks: [...l.ids] }))}
            onClearSaved={() => {
              setNote(null);
              setBuild((s) => ({ ...s, saved: [] }));
            }}
            onDeleteSaved={(index) => {
              // Deleting frees up exposure, so any "ran out of room" note from
              // the last Gen press is no longer true of this saved set.
              setNote(null);
              // By position: the rail's "L3" IS index 2, so nothing has to be
              // matched up and the remaining cards renumber themselves.
              setBuild((s) => ({
                ...s,
                saved: s.saved.filter((_, i) => i !== index),
              }));
            }}
          />
        </div>
      ) : tab === "course" ? (
        <CourseExplorer course={slate.course} field={field} onSelect={openPlayer} />
      ) : tab === "sg" ? (
        <SgRankings rows={slate.sg_rankings ?? []} field={field} onSelect={openPlayer} />
      ) : tab === "tracker" ? (
        <Tracker rows={slate.tracker ?? []} weeks={slate.weeks ?? []} />
      ) : tab === "results" ? (
        <ResultsBrowser />
      ) : (
        <DbQuery />
      )}
    </div>
  );
}

/**
 * Why Gen added fewer lineups than it was asked for.
 *
 * Shown in the rail rather than swallowed: a button labelled "Gen 5" that adds
 * three is a bug report waiting to happen, and every one of these reasons is
 * something the user can act on (unlock someone, un-exclude someone, delete a
 * saved lineup).
 */
function genShortfall(r: GenResult, asked: number, maxExposure: number): string {
  const got = r.lineups.length;
  const head = got === 0 ? "no lineups added" : `added ${got} of ${asked}`;
  const why: Record<GenResult["stop"], string> = {
    complete: "",
    infeasible: "no roster fits the cap under the current locks and exclusions",
    exposure: `the rest would push a player past ${maxExposure}% exposure — no one may appear in more than ${r.ceiling} saved lineups. Unlock or delete a saved lineup to free someone up.`,
    exhausted: "no different roster is left to build",
    capped: "search limit reached — press again to continue",
  };
  return `${head} — ${why[r.stop]}`;
}

function NoData({ detail }: { detail: string }) {
  return (
    <div style={{ padding: 40, fontFamily: font.mono, fontSize: 13 }}>
      <div style={{ color: c.red, letterSpacing: "0.14em", marginBottom: 8 }}>NO SLATE DATA</div>
      <div style={{ color: c.muted, lineHeight: 1.6 }}>{detail}</div>
    </div>
  );
}
