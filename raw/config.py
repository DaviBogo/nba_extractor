from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    BQ_PROJECT: str
    BQ_DATASET: str

    class Config:
        env_file = '.env'


settings = Settings()