from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {"owner": "Agung", "retries": "5", "retry_delay": timedelta(minutes=2)}


# function
def greet(ti):
    name = ti.xcom_pull(
        task_ids="Second_Task",key='name'
    )  # Xcom_pull bisa dipake buat ngambil data dari task atau func lain
    age = ti.xcom_pull(
        task_ids="Second_Task",key='age'
    )
    print(f"jenengku {name},umur {age}")


def get_name(ti):
    ti.xcom_push(key='name',value='Don')
    ti.xcom_push(key='age',value=32)


with DAG(
    dag_id="python_dagv01",
    default_args=default_args,
    description="A simple python DAG",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
) as dag:
    task1 = PythonOperator(
        task_id="First_Task",
        python_callable=greet,  # ngecall function yang bakal dipake
        # op_kwargs={"age": 22},  # input parameter buat functionya
    )

    task2 = PythonOperator(
        task_id="Second_Task",
        python_callable=get_name,
    )

    task2 >> task1
