import pandas as pd
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel


def add_exported_datetime(
        df: pd.DataFrame, 
        export_datetime=datetime.now(timezone.utc) - timedelta(hours=3)
    )-> pd.DataFrame:
    export_datetime = export_datetime.replace(tzinfo=None)
    df['exported_at'] = pd.to_datetime(export_datetime)
    return df


def identify_gateways(df:pd.DataFrame)->pd.DataFrame:
    df['is_gateway'] = df['email'].apply(lambda x: True if "@gateways.dynamox" in str(x) else False)
    return df


def identify_internal_users(df:pd.DataFrame)->pd.DataFrame:
    df['is_internal'] = df['email'].apply(lambda x: True if "@dynamox" in str(x) else False)
    return df


def transform_columns_to_date(df:pd.DataFrame, columns:list)->pd.DataFrame:
    for col in columns:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.strftime('%Y-%m-%d')
    return df


def transform_miliseconds_to_datetime(df:pd.DataFrame, columns:list)->pd.DataFrame:
    for col in columns:
        df[col] = pd.to_datetime(df[col], unit="ms")
    return df


def remove_columns_time_zone(df:pd.DataFrame, columns:list)->pd.DataFrame:
    for col in columns:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.tz_convert(None)
    return df


def filter_columns_by_model(df: pd.DataFrame, model: BaseModel)->pd.DataFrame:
    model_columns = [field.alias or field_name for field_name, field in model.model_fields.items()]
    return df[model_columns]


def rename_columns_by_model(df: pd.DataFrame, model: BaseModel)->pd.DataFrame:
    rename_dict = {field.alias: field_name for field_name, field in model.model_fields.items() if field.alias}
    return df.rename(columns=rename_dict)
