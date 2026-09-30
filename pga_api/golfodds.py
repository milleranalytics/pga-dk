"""The golfodds.com weekly board: weekly.odds(week, source="golfodds").

The Tour's FanDuel feed (pga_api.odds) is the default source; this is the
scrape the old notebook used, kept as a second source for a week the feed does
not price. Every season of golf.db's odds came from it, which is why
archives.py still reads its spellings (data/history/golfdb_odds.csv).
"""

from __future__ import annotations

import io

import pandas as pd
import requests

from pga_api.names import PLAYER_NAME_MAP, TOURNAMENT_NAME_MAP, standardize_player_names


def weekly_board(season: int, tournament_name: str, url: str = "http://golfodds.com/weekly-odds.html") -> pd.DataFrame:
    """
    Scrapes and cleans current week odds from GolfOdds.com.
    Returns a DataFrame with SEASON, TOURNAMENT, PLAYER, ODDS (string), and VEGAS_ODDS (decimal).
    """
    headers = {'User-Agent': 'Mozilla/5.0'}
    html = requests.get(url, headers=headers).text

    # Parse the page's own tournament header so callers can verify the board
    # matches this week's event.
    import re as _re
    from datetime import datetime as _dt
    scraped_name, scraped_end = None, None
    m = _re.search(r'class="Headline-orange">([^<]+)</span>', html)
    if m:
        scraped_name = m.group(1).strip()
    m2 = _re.search(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+\d{1,2}\s*-\s*'
        r'(?:(January|February|March|April|May|June|July|August|September|October|November|December)\s+)?'
        r'(\d{1,2}),\s*(\d{4})', html)
    if m2:
        end_month = m2.group(2) or m2.group(1)
        try:
            scraped_end = _dt.strptime(f"{end_month} {m2.group(3)}, {m2.group(4)}", "%B %d, %Y").date()
        except ValueError:
            pass

    odds_df = pd.read_html(io.StringIO(html))[3]

    # Drop all-NaN rows and reset index
    odds_df = odds_df.dropna(how='all').reset_index(drop=True)

    # Rename first two columns
    odds_df = odds_df.rename(columns={0: "PLAYER", 1: "ODDS"})

    # Insert season and tournament info
    odds_df.insert(loc=0, column="SEASON", value=season)
    odds_df.insert(loc=1, column="TOURNAMENT", value=tournament_name)

    # Cut at a second event's board. Table [3] sometimes stacks two concurrent
    # tournaments (e.g. John Deere + BMW International Open); the second board is
    # introduced by its own "ODDS to Win" header. Truncate at the first such
    # header that has real rows above it, BEFORE dropping null rows — the header
    # block is all-null-ODDS and would otherwise be silently deleted, fusing the
    # two fields together with no boundary left to detect.
    hdr_pos = [odds_df.index.get_loc(i) for i in odds_df.index[
        odds_df["PLAYER"].astype(str).str.contains("ODDS to", na=False)]]
    hdr_pos = [p for p in hdr_pos if p > 0]
    if hdr_pos:
        odds_df = odds_df.iloc[:hdr_pos[0]]

    # Drop rows with missing player names, or missing odds (e.g. the page's
    # "- current as of M/D/YYYY -" caption row, which lands in the PLAYER
    # column with a blank ODDS and is not a real entry).
    odds_df = odds_df.dropna(subset=["PLAYER", "ODDS"])

    # A PLAYER WITH NO LETTERS IN IT IS NOT A PLAYER. Before the book opens on
    # an event, golfodds.com puts the board up with a dotted-leader separator
    # where the field will go, and that row is neither NaN nor blank -- it is a
    # string of periods with an empty ODDS, so every drop above passes it
    # through. It reached the odds table once (2026 Biltmore Championship) and
    # had to be deleted by hand: golf.db's odds save only ever inserted, so
    # re-running after the real odds post added the field alongside the phantom
    # rather than replacing it.
    #
    # `[^\W\d_]` is "a letter" in any alphabet, so an accented or non-Latin
    # name is kept; only rules, dots and pure punctuation are dropped.
    has_letter = odds_df["PLAYER"].astype(str).str.contains(r"[^\W\d_]", regex=True, na=False)
    if not has_letter.all():
        dropped = odds_df.loc[~has_letter, "PLAYER"].astype(str).tolist()
        print(f"ℹ️ Dropped {len(dropped)} placeholder row(s) with no player name: "
              + ", ".join(repr(d[:20]) for d in dropped[:3]))
    odds_df = odds_df[has_letter]

    # Trim rows after "Tournament Matchups" section
    try:
        matchups_row = odds_df.index[odds_df.iloc[:, 2].astype(str).str.contains("Tournament")].tolist()[0]
        odds_df = odds_df.iloc[:matchups_row]
    except IndexError:
        pass  # If not found, continue without trimming

    # Remove entries that are not valid odds
    odds_df = odds_df[~odds_df["ODDS"].isin(["WD", "XX", "ODDS to Win:", "ODDS to\xa0Win:"])]

    # Clean formatting. Strip commas and any stray non-digit/non-slash chars
    # (the scrape occasionally prefixes a value with junk, e.g. "\2000/1").
    odds_df["ODDS"] = odds_df["ODDS"].str.replace(",", "", regex=True)
    odds_df["ODDS"] = odds_df["ODDS"].str.replace(r"[^\d/]", "", regex=True)
    odds_df["PLAYER"] = odds_df["PLAYER"].str.replace(r"\s", " ", regex=True)

    # Convert fractional odds to decimal, row-by-row so a single unparseable
    # entry (e.g. "EVEN", "SP", a blank, or a value with no "/") only nulls
    # that one row instead of blanking the whole column.
    # `expand=True` on an EMPTY Series yields a frame with no columns at all,
    # not one column of nothing -- so `parts[0]` raises KeyError rather than
    # returning empty. That is the shape this takes when the board is up but
    # unpriced, which is a normal state early in the week, so both operands
    # fall back to an empty float column instead.
    parts = odds_df["ODDS"].str.split("/", n=1, expand=True)
    num = (pd.to_numeric(parts[0], errors="coerce") if parts.shape[1] > 0
           else pd.Series(index=odds_df.index, dtype=float))
    den = (pd.to_numeric(parts[1], errors="coerce") if parts.shape[1] > 1
           else pd.Series(index=odds_df.index, dtype=float))
    odds_df["VEGAS_ODDS"] = num / den

    # Apply name normalization maps
    odds_df["PLAYER"] = odds_df["PLAYER"].replace(PLAYER_NAME_MAP)
    odds_df["TOURNAMENT"] = odds_df["TOURNAMENT"].replace(TOURNAMENT_NAME_MAP)

    # Final column selection
    odds_df = odds_df[["SEASON", "TOURNAMENT", "PLAYER", "ODDS", "VEGAS_ODDS"]]

    # Normalize player names
    odds_df = standardize_player_names(odds_df)

    odds_df.attrs["scraped_tournament"] = scraped_name
    odds_df.attrs["scraped_end_date"] = scraped_end
    if scraped_name:
        print(f"ℹ️ Odds page is serving: {scraped_name}"
              + (f" (ends {scraped_end})" if scraped_end else ""))
    # AN EMPTY BOARD IS A NORMAL ANSWER EARLY IN THE WEEK, and it is worth
    # saying out loud: the page is up, the header names the right event, and
    # there is simply no field priced yet. Said here rather than left to the
    # caller because an empty frame is otherwise indistinguishable from a
    # parser that has quietly stopped matching the page's layout.
    if odds_df.empty:
        print("⚠️ No odds posted yet — the board is up but the field is not "
              "priced. Nothing to save; re-run this cell once it opens.")
    return odds_df
