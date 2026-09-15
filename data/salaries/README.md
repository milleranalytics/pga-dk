# `data/salaries/` — one DraftKings salary export per tournament

Written automatically by the notebook's **Download DraftKings salaries** cell
(`utils/dk_api.refresh_from_dk`). You never download anything by hand and you never
edit this folder.

Each file is DraftKings' own export, written **verbatim**, under the tournament it
describes:

```
dk-2026-2026-08-30-tour-championship.csv
dk-2026-2026-09-20-biltmore-championship-asheville.csv
dk-2027-2026-10-11-sanderson-farms-championship.csv     <- season 2027, ends Oct 2026
```

`dk-<season>-<ending date>-<tournament>.csv`, plus a small `-meta.json` beside it
holding the draft group, DraftKings' own name for the event, the venue, the first tee
time and **when the fetch ran** — the one freshness fact nothing inside the CSV can give.

**The year is written twice on purpose.** A PGA season is not a calendar year, and the
two differ on no fixed rule: the autumn events that open a season used to end in the
previous calendar year (the 2015 season opened at the Frys.com Open in October 2014),
but since 2023 the same October week belongs to the current season. September holds both
kinds in the database today. Neither field is derivable from the other, so both are
written. Files are looked up on **(season, tournament)** — never on the date, so fixing a
wrong `new_ending_date` cannot hide a file you already saved.

## Why it exists

There used to be a single `data/DKSalaries.csv` you downloaded from the lobby every week
and overwrote. DraftKings takes a contest down once it has been played, and **there is no
URL that returns last week's salaries** — so every week landed on top of the one before it
and those prices were gone.

That matters because salary history is exactly what lineup-level backtesting needs.
Finish positions and strokes gained are already in `golf.db` for any season, but *"would
this lineup have fit under the cap"* is answerable only from the prices posted that week.
The file also carries DraftKings' own `AvgPointsPerGame` and the list of who was priced at
all — the field as DK saw it, kept nowhere else.

`data/golf.db` is a derived index and is safe to rebuild; this folder and the odds are the
system of record for anything DraftKings-side.

Git *history* is not a substitute. It holds each committed version, but nothing in this
project reads git history, so a rebuild cannot reach it — and a tournament overwritten
before it was committed is gone with nothing to notice.

## The two-machine split

| | what it does | at home | at work |
|---|---|---|---|
| **Download DraftKings salaries** | fetch this week's prices and save them here | run it | **skip it** |
| **DraftKings Field** | build `dk` from the saved prices | run it | run it |

The work network blocks `draftkings.com` outright. The download cell is the only thing in
the notebook that talks to DraftKings; the field cell makes **no request under any
circumstance**, not even a failed one, so nothing on that machine ever calls out. The
handoff is a `git push` at home and a `git pull` at work.

## Rules

- **Verbatim.** Byte for byte, including DraftKings' byte-order mark and CRLF line
  endings. A parser change years from now can then be re-run over every tournament ever
  downloaded, rather than over this parser's idea of what mattered at the time.
- **Committed.** A few KB a week. It carries no account, entry or contest identifiers.
- **Re-running is free.** An identical file reports `unchanged` and touches nothing.
- **A differing file for the same tournament REPLACES it, and says so.** The reason to
  download again mid-week is that something moved — a withdrawal, a late commitment, a
  price — and the later file describes the contest you actually enter.
- **Download BEFORE the first tee, and treat a late re-fetch as suspect.** `Salary` is
  fixed once the contest is posted, but `AvgPointsPerGame` is DraftKings' own season-to-date
  average and it keeps updating — so re-fetching a draft group *after* its tournament
  finishes writes an average that **includes that tournament's own result**. A file saved on
  Tuesday carries what was knowable on Tuesday; the same file re-pulled on Monday does not.
  `Status` is worse in the same way: re-reading an old draft group returns *today's* status,
  not that week's.

  Nothing in this project reads either column today — `load_field` takes `Name` and `Salary`
  and nothing else — so no backtest can currently reach one. But this folder exists to be
  read years from now, and a feature built on `AvgPointsPerGame` out of a late re-fetch
  would be a model scoring a tournament partly from its own outcome, with nothing in the
  data to complain. If you ever use that column, check `fetched_at` in the `-meta.json`
  against the tournament's start first.
