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
    PERSON_ID: str
    DISPLAY_LAST_COMMA_FIRST: str
    DISPLAY_FIRST_LAST: str
    ROSTERSTATUS: str
    FROM_YEAR: str
    TO_YEAR: str
    PLAYERCODE: str
    TEAM_ID: str
    TEAM_CITY: str
    TEAM_NAME: str
    TEAM_ABBREVIATION: str
    TEAM_CODE: str
    GAMES_PLAYED_FLAG: str
    OTHERLEAGUE_EXPERIENCE_CH: str
    exported_at: datetime

def bronze_players():
    try:
        BQ_PROJECT = settings.BQ_PROJECT
        BQ_DATASET = 'bronze'
        BQ_TABLE = Path(__file__).stem

        team_stats = leaguedashteamstats.LeagueDashTeamStats()

        df_data = pd.DataFrame(team_stats.get_data_frames()[0])
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
