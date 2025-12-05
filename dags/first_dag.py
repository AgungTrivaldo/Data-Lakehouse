from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'Agung',
    'retries': '5',
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'First Dag', # DAG Name
    default_args = default_args,
    description = 'This is my First Dag', # DAG description
    start_date = datetime(2025,1,1),# DAG Start Date
    schedule_interval = '@daily', # DAG frequency 
) as dag:
    task1 = BashOperator(
        first_task = 'first task',
        bash_command = 'echo This is my first DAG'
    )

    task1