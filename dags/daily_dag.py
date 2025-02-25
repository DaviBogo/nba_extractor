import os
import sys
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_dir)

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from raw.players import bronze_players
from raw.stats import bronze_stats
from raw.teams import bronze_teams
from raw.dataform_nba_stats import run_dataform_nba_stats


with DAG(
    "daily_dag",
    start_date=datetime(2024, 9, 23),
    schedule="*/15 * * * *",
    catchup=False,
    ) as dag:

    bronze_players = PythonOperator(
        task_id = "bronze_players",
        python_callable=bronze_players
    )

    bronze_stats = PythonOperator(
        task_id = "bronze_stats",
        python_callable=bronze_stats
    )

    bronze_teams = PythonOperator(
        task_id = "bronze_teams",
        python_callable=bronze_teams
    )

    run_dataform_nba_stats = PythonOperator(
        task_id = "run_dataform_health_score",
        python_callable=run_dataform_nba_stats
    )

    [
        bronze_players,
        bronze_stats,
        bronze_teams
    ] >> run_dataform_nba_stats