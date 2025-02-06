import pandas as pd
import google.auth
from datetime import datetime, date
from pydantic_settings import BaseSettings
from pandas_gbq import to_gbq


def apply_pydantic_types(df:pd.DataFrame, pydantic_class: BaseSettings):
    field_types = pydantic_class.__annotations__
    
    for f, t in field_types.items():
        if t == datetime:
            df[f] = df[f].astype("datetime64[ns]")
        elif t == date:
            df[f] = df[f].astype("datetime64[ns]").dt.date
        elif t == str:
            df[f] = df[f].apply(lambda x: str(x) if pd.notnull(x) else None)
        elif t == int:
            df[f] = df[f].apply(lambda x: int(x) if pd.notnull(x) else None)
        elif t == float:
            df[f] = df[f].apply(lambda x: float(x) if pd.notnull(x) else None)
        elif t == bool:
            df[f] = df[f].apply(lambda x: bool(x) if pd.notnull(x) else None)
    return df


def define_schema_from_class(pydantic_class)->list:
    class_dict = pydantic_class.__annotations__
    schema = []
    for field, typing in class_dict.items():
        if typing==int:
            bq_typing = "INTEGER"
        elif typing==float:
            bq_typing = "FLOAT"
        elif typing==bool:
            bq_typing = "BOOLEAN"
        elif typing==datetime:
            bq_typing = "DATETIME"
        elif typing==date:
            bq_typing = "DATE"
        elif typing==str:
            bq_typing = "STRING"
        schema.append({"name": field, "type": bq_typing})
    return schema


def send_data_to_bigquery(
        bigquery_project: str,
        bigquery_dataset: str,
        bigquery_table: str,
        df: pd.DataFrame,
        schema: str,
    )->None:

    table_path = f"{bigquery_project}.{bigquery_dataset}.{bigquery_table}"

    credentials, _ = google.auth.default(
        quota_project_id=bigquery_project
    )
    to_gbq(
        dataframe=df,
        project_id=bigquery_project,
        destination_table=table_path,
        table_schema=schema,
        if_exists="replace",
        credentials=credentials
    )
    return None