from sqlalchemy import Column, String, Integer, Float, Date, Table, MetaData

metadata = MetaData()

tournaments_table = Table(
    "tournaments",
    metadata,
    Column("SEASON", Integer),
    Column("ENDING_DATE", Date, primary_key=True),
    Column("TOURN_ID", String),
    Column("TOURNAMENT", String, primary_key=True),
    Column("COURSE", String),
    Column("PLAYER", String, primary_key=True),
    Column("POS", String),
    Column("FINAL_POS", Integer),
    Column("ROUNDS:1", String),
    Column("ROUNDS:2", String),
    Column("ROUNDS:3", String),
    Column("ROUNDS:4", String),
    Column("OFFICIAL_MONEY", String),
    Column("FEDEX_CUP_POINTS", String),
)

stats_table = Table(
    "stats",
    metadata,
    Column("SEASON", Integer, primary_key=True),
    Column("PLAYER", String, primary_key=True),
    Column("SGTTG_RANK", Integer),
    Column("SGTTG", Float),
    Column("SGOTT_RANK", Integer),
    Column("SGOTT", Float),
    Column("SGAPR_RANK", Integer),
    Column("SGAPR", Float),
    Column("SGATG_RANK", Integer),
    Column("SGATG", Float),
    Column("SGP_RANK", Integer),
    Column("SGP", Float),
    Column("BIRDIES_RANK", Integer),
    Column("BIRDIES", Float),
    Column("PAR_3_RANK", Integer),
    Column("PAR_3", Float),
    Column("PAR_4_RANK", Integer),
    Column("PAR_4", Float),
    Column("PAR_5_RANK", Integer),
    Column("PAR_5", Float),
    Column("TOTAL_DRIVING_RANK", Integer),
    Column("TOTAL_DRIVING", Float),
    Column("DRIVING_DISTANCE_RANK", Integer),
    Column("DRIVING_DISTANCE", Float),
    Column("DRIVING_ACCURACY_RANK", Integer),
    Column("DRIVING_ACCURACY", Float),
    Column("GIR_RANK", Integer),
    Column("GIR", Float),
    Column("SCRAMBLING_RANK", Integer),
    Column("SCRAMBLING", Float),
    Column("OWGR_RANK", Integer),
    Column("OWGR", Float)
)

odds_table = Table(
    "odds",
    metadata,
    Column("SEASON", Integer, primary_key=True),
    Column("TOURNAMENT", String, primary_key=True),
    Column("PLAYER", String, primary_key=True),
    Column("ODDS", String, primary_key=True),
    Column("VEGAS_ODDS", Float),
)