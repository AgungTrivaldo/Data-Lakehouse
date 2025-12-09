from datetime import datetime,timedelta
from airflow.decorators import dag,task

default_args = {"owner": "agung", "retries": 5, "retry_delay": timedelta(minutes=5)}

# kalo pake taskflow declare nya pake @ bukan with
@dag(
    dag_id="catchup_backfill",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
)

def