import { useCallback, useMemo, useState } from "react";
import { c, font } from "./tokens";
import { loadSlate, servedOverHttp } from "./loadSlate";
import type { Slate } from "./types";
import { enrich } from "./enrich";
import { useBuildState } from "./persist";
import type { SavedLineup } from "./persist";
import { optimize, generate } from "./optimizer";
import type { OptPlayer, GenResult } from "./optimizer";
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

  /**
   * The build is only ever ADDED to by a solve (Optimize / Gen / loading a
   * saved lineup), so the one thing left to do to a single pick is drop it.
   * Adding by hand is gone from both the grid and the card: a hand-added pick
   * was not an input to the solver, so the next Optimize dropped it without
   * saying so. Lock is how you say "keep this player".
   */
  const removePick = useCallback(
    (id: string) => setBuild((s) => ({ ...s, picks: s.picks.filter((x) => x !== id) })),
    [setBuild],
  );

  // Lock and exclude are mutually exclusive: "force into every solve" and
  // "remove from every solve" cannot both be true. Setting either clears the
  // other rather than producing a state the optimizer would have to arbitrate.
  // (Being in the current build AND excluded is still allowed and meaningful —
  // the build wins for the current build, the exclusion applies to future
  // solves — so picks are deliberately untouched here.)
  const toggleLock = useCallback(
    (id: string) =>
      setBuild((s) => {
        const locks = { ...s.locks };
        const excludes = { ...s.excludes };
        if (locks[id]) {
          delete locks[id];
        } else {
          locks[id] = true;
          delete excludes[id];
        }
        return { ...s, locks, excludes };
      }),
    [setBuild],
  );

  const toggleExclude = useCallback(
    (id: string) =>
      setBuild((s) => {
        const excludes = { ...s.excludes };
        const locks = { ...s.locks };
        if (excludes[id]) {
          delete excludes[id];
        } else {
          excludes[id] = true;
          delete locks[id];
        }
        return { ...s, excludes, locks };
      }),
    [setBuild],
  );

  /**
   * Re-solve from scratch, keeping only what is LOCKED.
   *
   * It used to fill around the current build, which meant Optimize was a no-op
   * on a full roster and re-optimizing took a Clear first, every time. Lock is
   * now the one way to say "keep this player" — which is what the button next
   * to it always meant, so nothing became unsayable, and the grid's ＋ button
   * (a pick the optimizer would preserve) lost its only distinct job and is
   * gone with it. Hand-picking still exists on the player card, where it reads
   * as a build action rather than a constraint.
   */
  const onOptimize = useCallback(() => {
    const r = optimize(ctx); // ctx.pickedIds is empty by construction
    if (!r) return;
    setBuild((s) => ({ ...s, picks: r.map((p) => p.id) }));
  }, [ctx, setBuild]);

  /** Set only when a Gen press produced fewer lineups than it was asked for. */
  const [genNote, setGenNote] = useState<string | null>(null);

  // Solved outside the setBuild updater on purpose: updaters must stay pure
  // (StrictMode runs them twice), and this one both reports a note and is a
  // few tens of milliseconds of search.
  const onGenerate = useCallback(() => {
    const r = generate(ctx, GEN_COUNT, build.saved.map((l) => l.ids), MAX_EXPOSURE);
    setGenNote(r.lineups.length < GEN_COUNT ? genShortfall(r, GEN_COUNT, MAX_EXPOSURE) : null);
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
            genNote={genNote}
            // Clicking a filled slot means "get this player out of my lineup",
            // so it drops the lock too. Leaving the lock would make the next
            // Optimize put him straight back with nothing on screen explaining
            // why — the removal has to actually take.
            onRemove={(id) => {
              removePick(id);
              if (build.locks[id]) toggleLock(id);
            }}
            onOptimize={onOptimize}
            onGenerate={onGenerate}
            onSave={onSave}
            onClear={() => setBuild((s) => ({ ...s, picks: [] }))}
            onLoadSaved={(l) => setBuild((s) => ({ ...s, picks: [...l.ids] }))}
            onClearSaved={() => {
              setGenNote(null);
              setBuild((s) => ({ ...s, saved: [] }));
            }}
            onDeleteSaved={(index) => {
              // Deleting frees up exposure, so any "ran out of room" note from
              // the last Gen press is no longer true of this saved set.
              setGenNote(null);
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
