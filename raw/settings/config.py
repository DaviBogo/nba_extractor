from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    AIRFLOW_UID: str
    AIRFLOW_HOME_PATH: str

    BQ_PROJECT: str
    BQ_DATASET: str

    class Config:
        env_file = '.env'


settings = Settings()