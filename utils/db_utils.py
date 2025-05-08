# db_utils.py

# region --- Update Tournament
# -----------------------------------------------------
import requests
import pandas as pd
import sqlite3 as sql
from datetime import datetime
import numpy as np
from numpy import nan

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_tournament_results(config: dict, db_path: str, season: int, year: int):
    """Scrapes and updates the tournament results in the SQLite DB."""

    # Extract tournament details
    tourn_id = config["old"]["id"]
    tourn_name = config["old"]["name"]
    course = config["old"]["course"]
    date_str = config["old"]["date"]

    payload = {
        "operationName": "TournamentPastResults",
        "variables": {
            "tournamentPastResultsId": tourn_id,
            "year": year
        },
        "query": "query TournamentPastResults($tournamentPastResultsId: ID!, $year: Int) {\n  tournamentPastResults(id: $tournamentPastResultsId, year: $year) {\n    id\n    players {\n      id\n      position\n      player {\n        id\n        firstName\n        lastName\n        shortName\n        displayName\n        abbreviations\n        abbreviationsAccessibilityText\n        amateur\n        country\n        countryFlag\n        lineColor\n      }\n      rounds {\n        score\n        parRelativeScore\n      }\n      additionalData\n      total\n      parRelativeScore\n    }\n    rounds\n    additionalDataHeaders\n    availableSeasons {\n      year\n      displaySeason\n    }\n    winner {\n      id\n      firstName\n      lastName\n      totalStrokes\n      totalScore\n      countryFlag\n      countryName\n      purse\n      points\n    }\n  }\n}"
    }

    # Send POST request
    response = requests.post(
        "https://orchestrator.pgatour.com/graphql",
        json=payload,
        headers={"x-api-key": X_API_KEY}
        # verify=False  # 🚨 TEMPORARY bypass
    )
    response.raise_for_status()

    # Parse players data
    players = response.json()["data"]["tournamentPastResults"]["players"]

    if not players:
        raise ValueError("No player data found. Check tournament ID and year.")

    # Convert to dataframe
    df = pd.DataFrame(map(lambda p: {
        "POS": p["position"],
        "PLAYER": p["player"]["displayName"],
        "ROUNDS:1": p["rounds"][0]["parRelativeScore"] if len(p["rounds"]) > 0 else nan,
        "ROUNDS:2": p["rounds"][1]["parRelativeScore"] if len(p["rounds"]) > 1 else nan,
        "ROUNDS:3": p["rounds"][2]["parRelativeScore"] if len(p["rounds"]) > 2 else nan,
        "ROUNDS:4": p["rounds"][3]["parRelativeScore"] if len(p["rounds"]) > 3 else nan,
        "OFFICIAL_MONEY": p["additionalData"][1],
        "FEDEX_CUP_POINTS": p["additionalData"][0],
    }, players))

    df = df.dropna(subset=["POS"])
    df["FINAL_POS"] = df["POS"].str.extract(r"(\d+)", expand=False).fillna(90).astype(int)
    df.insert(0, "SEASON", season)
    df.insert(1, "ENDING_DATE", datetime.strptime(date_str, "%m/%d/%Y").date())
    df.insert(2, "TOURN_ID", tourn_id)
    df.insert(3, "TOURNAMENT", tourn_name)
    df.insert(4, "COURSE", course)

    # Connect to database
    db = sql.connect(db_path)
    existing = pd.read_sql("SELECT * FROM tournaments", db)

    # Merge and drop duplicates
    combined = pd.concat([existing, df]).drop_duplicates(["PLAYER", "TOURNAMENT", "ENDING_DATE"], keep="last")
    combined["ENDING_DATE"] = pd.to_datetime(combined["ENDING_DATE"]).dt.date

    # Save back to DB
    combined.to_sql("tournaments", db, index=False, if_exists="replace")
    db.close()

    print(f"✅ Tournament results for '{tourn_name}' added to {db_path}")
    return combined

# endregion