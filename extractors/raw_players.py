import pandas as pd
from nba_api.stats.endpoints import commonallplayers
from utils import transform, load
from datetime import datetime
from extractors.config import settings
from pydantic_settings import BaseSettings
from pathlib import Path

class BigQuerySchema(BaseSettings):
    IsActive: str
    Username: str
    UserRoleId: str
    ProfileId: str
    Name: str
    IdSAP__c: str
    Department: str
    Division: str
    exported_at: datetime

def bronze_players():

    BQ_PROJECT = settings.BQ_PROJECT
    BQ_DATASET = 'bronze'
    BQ_TABLE = Path(__file__).stem

    all_players = commonallplayers.CommonAllPlayers()

    df_data = pd.DataFrame(all_players.get_data_frames()[0])
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

if __name__ == '__main__':

    bronze_players()
