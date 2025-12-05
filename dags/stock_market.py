from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id = 'tutorial',
    schedule_interval = '@once',
    start_date = datetime(2023, 1, 1),
    tags=['example'],
) as dag:
    
    @task
    def test():
        pass