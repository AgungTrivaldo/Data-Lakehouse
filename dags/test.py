from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='hello_airflow_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def say_hello():
        print("✅ Hello from Airflow 2!")

    say_hello()
