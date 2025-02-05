import os
import sys
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_dir)

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from extractors.raw_players import raw_players


with DAG(
    "daily_dag",
    start_date=datetime(2024, 9, 23),
    schedule="0 9 * * *",
    catchup=False,
    ) as dag:

    raw_players = PythonOperator(
        task_id = "raw_players",
        python_callable=raw_players
    )
