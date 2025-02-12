import pandas as pd
from nba_api.stats.endpoints import leaguedashteamstats
from datetime import datetime
from pydantic_settings import BaseSettings
from pathlib import Path
from utils import transform, load
from settings.config import settings
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
    PLUS_MINUS: float
    GP_RANK: int
    W_RANK: int
    L_RANK: int
    W_PCT_RANK: int
    MIN_RANK: int
    FGM_RANK: int
    FGA_RANK: int
    FG_PCT_RANK: float
    FG3M_RANK: int
    FG3A_RANK: int
    FG3_PCT_RANK: float
    FTM_RANK: int
    FTA_RANK: int
    FT_PCT_RANK: float
    OREB_RANK: int
    DREB_RANK: int
    REB_RANK: int
    AST_RANK: int
    TOV_RANK: int
    STL_RANK: int
    BLK_RANK: int
    BLKA_RANK: int
    PF_RANK: int
    PFD_RANK: int
    PTS_RANK: int
    PLUS_MINUS_RANK: int
    season: str
    exported_at: datetime

def bronze_players():
    try:
        BQ_PROJECT = settings.BQ_PROJECT
        BQ_DATASET = 'bronze'
        BQ_TABLE = Path(__file__).stem

        seasons = ['2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

        all_stats = []
        for season in seasons:
            season_stats = leaguedashteamstats.LeagueDashTeamStats(season=season)
            df_season = pd.DataFrame(season_stats.get_data_frames()[0])
            df_season['season'] = season
            all_stats.append(df_season)

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

    bronze_players()
