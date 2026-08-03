import { useCallback, useMemo, useState } from "react";
import { c, font } from "./tokens";
import { loadSlate, servedOverHttp } from "./loadSlate";
import type { Slate } from "./types";
import { enrich } from "./enrich";
import { useBuildState } from "./persist";
import type { SavedLineup } from "./persist";
import { optimize, generate } from "./optimizer";
import type { OptPlayer } from "./optimizer";
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

  const ctx = useMemo(
    () => ({
      all: optPool,
      lockedIds: new Set(Object.keys(build.locks)),
      excludedIds: new Set(Object.keys(build.excludes)),
      pickedIds: build.picks,
      slots: field.meta.roster,
      cap: field.meta.cap,
    }),
    [optPool, build.locks, build.excludes, build.picks, field.meta],
  );

  const togglePick = useCallback(
    (id: string) =>
      setBuild((s) => {
        if (s.picks.includes(id)) return { ...s, picks: s.picks.filter((x) => x !== id) };
        // Adding is a no-op when the roster is full — no error state, the
        // button simply does not act.
        if (s.picks.length >= field.meta.roster) return s;
        return { ...s, picks: [...s.picks, id] };
      }),
    [setBuild, field.meta.roster],
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

  const onOptimize = useCallback(() => {
    const r = optimize(ctx);
    if (!r) return;
    setBuild((s) => ({ ...s, picks: r.map((p) => p.id) }));
  }, [ctx, setBuild]);

  const onGenerate = useCallback(() => {
    setBuild((s) => {
      const lineups = generate(
        { ...ctx, pickedIds: [] }, // Gen solves fresh builds; locks still apply
        GEN_COUNT,
        s.saved.map((l) => l.ids),
        MAX_EXPOSURE,
      );
      let next = s.saved.reduce((m, l) => Math.max(m, l.id), 0);
      const added: SavedLineup[] = lineups.map((ids) => ({ id: ++next, ids }));
      return { ...s, saved: [...s.saved, ...added] };
    });
  }, [ctx, setBuild]);

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
      const next = s.saved.reduce((m, l) => Math.max(m, l.id), 0) + 1;
      return { ...s, saved: [...s.saved, { id: next, ids: [...s.picks] }] };
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
            onTogglePick={togglePick}
            onToggleLock={toggleLock}
            onToggleExclude={toggleExclude}
          />
          <PlayerCard
            field={field}
            player={selected ? (field.byId.get(selected) ?? null) : null}
            inLineup={selected ? build.picks.includes(selected) : false}
            locked={selected ? !!build.locks[selected] : false}
            excluded={selected ? !!build.excludes[selected] : false}
            onTogglePick={togglePick}
            onToggleLock={toggleLock}
            onToggleExclude={toggleExclude}
          />
          <LineupRail
            field={field}
            picks={build.picks}
            locks={build.locks}
            saved={build.saved}
            genCount={GEN_COUNT}
            maxExposure={MAX_EXPOSURE}
            syncStatus={sync.status}
            lastSaved={sync.lastSaved}
            onRemove={togglePick}
            onOptimize={onOptimize}
            onGenerate={onGenerate}
            onSave={onSave}
            onClear={() => setBuild((s) => ({ ...s, picks: [] }))}
            onLoadSaved={(l) => setBuild((s) => ({ ...s, picks: [...l.ids] }))}
            onDeleteSaved={(id) =>
              setBuild((s) => ({ ...s, saved: s.saved.filter((l) => l.id !== id) }))
            }
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

function NoData({ detail }: { detail: string }) {
  return (
    <div style={{ padding: 40, fontFamily: font.mono, fontSize: 13 }}>
      <div style={{ color: c.red, letterSpacing: "0.14em", marginBottom: 8 }}>NO SLATE DATA</div>
      <div style={{ color: c.muted, lineHeight: 1.6 }}>{detail}</div>
    </div>
  );
}
