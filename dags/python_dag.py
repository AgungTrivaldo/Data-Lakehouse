from airflow import DAG
from aiflow.operators.python import PythonOperator 
from datetime import datetime,timedelta

default_args = {
    'owner':"Agung",
    'retries':"5",
    'retry_delay':timedelta(minutes=2)
}

def greet(name,age):
    print(f"jenengku {name},umur {age}")

with DAG(
    dag_id='python_dag',
    default_args=default_args,
    description='A simple python DAG',
    schedule_interval='@once',
    start_date=datetime(2025,1,1)
) as dag:
    task1 = PythonOperator(
        task_id = 'First_Task'
        python_callable = greet
        op_kwargs = {'name':'Agung','age':22}
    )