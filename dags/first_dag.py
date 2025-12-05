from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'Agung',
    'retries': '5',
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'First_Dag_v2', # DAG Name
    default_args = default_args,
    description = 'This is my First Dag', # DAG description
    start_date = datetime(2025,1,1),# DAG Start Date
    schedule_interval = '@daily', # DAG frequency 
) as dag:
    task1 = BashOperator(
        task_id = 'first_task', # task id or task name
        bash_command = 'echo This is my first DAG' # command
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = 'echo This is the second task'
    )
    task1.set_upstream(task2) # bakal nyambung ke task yang pertama jadi Task1 -> Task2
