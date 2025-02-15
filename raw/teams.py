import pandas as pd
from nba_api.stats.endpoints import leaguedashteamstats
from datetime import datetime
from pydantic_settings import BaseSettings
from pathlib import Path
from raw.utils import transform, load
from raw.settings.config import settings
import time
import logging

logger = logging.getLogger(__name__)

class BigQuerySchema(BaseSettings):
    TEAM_ID: str
    TEAM_NAME: str
    GP: int
    W: int
    L: int
    W_PCT: int
    MIN: int
    FGM: int
    FGA: int
    FG_PCT: float
    FG3M: int
    FG3A: int
    FG3_PCT: float
    FTM: int
    FTA: int
    FT_PCT: float
    OREB: int
    DREB: int
    REB: int
    AST: int
    TOV: int
    STL: int
    BLK: int
    BLKA: int
    PF: int
    PFD: int
    PTS: int
    OPP_FGM: int
    OPP_FGA: int
    OPP_FG_PCT: float
    OPP_FG3M: int
    OPP_FG3A: int
    OPP_FG3_PCT: float
    OPP_FTM: int
    OPP_FTA: int
    OPP_FT_PCT: float
    OPP_OREB: int
    OPP_DREB: int
    OPP_REB: int
    OPP_AST: int
    OPP_TOV: int
    OPP_STL: int
    OPP_BLK: int
    OPP_BLKA: int
    OPP_PF: int
    OPP_PFD: int
    OPP_PTS: int
    season: str
    exported_at: datetime

def bronze_teams():
    try:
        BQ_PROJECT = settings.BQ_PROJECT
        BQ_DATASET = 'bronze'
        BQ_TABLE = Path(__file__).stem

        seasons = ['2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

        all_stats = []
        for season in seasons:
            season_stats = leaguedashteamstats.LeagueDashTeamStats(season=season)
            season_stats_opp = leaguedashteamstats.LeagueDashTeamStats(season=season, measure_type_detailed_defense='Opponent')
            df_season = pd.DataFrame(season_stats.get_data_frames()[0])
            df_season_opp = pd.DataFrame(season_stats_opp.get_data_frames()[0])
            df_season_opp = df_season_opp.drop(columns=["TEAM_NAME", "GP", "W", "L", "W_PCT", "MIN"])
            df_merged = pd.merge(df_season, df_season_opp, on="TEAM_ID", how="inner")
            df_merged['season'] = season
            all_stats.append(df_merged)
            time.sleep(1)

        df_data = pd.concat(all_stats)
        df_data = transform.add_exported_datetime(df_data)
        df_data = df_data[BigQuerySchema.model_fields.keys()]
        df_data = load.apply_pydantic_types(df_data, BigQuerySchema)
        schema = load.define_schema_from_class(BigQuerySchema)

        load.send_data_to_bigquery(
            bigquery_project=BQ_PROJECT,
            bigquery_dataset=BQ_DATASET,
            bigquery_table=BQ_TABLE,
            df=df_data,
            schema=schema
        )
    except Exception as e:
        logger.error(f'error on DAG bronze_players: {str(e)}')
        raise e

if __name__ == '__main__':
    bronze_teams()
