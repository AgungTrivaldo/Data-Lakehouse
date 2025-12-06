from datetime import timedelta, datetime
from airflow.decorators import dag, task


default_args = {"owner": "agung", "retries": "5", "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="first_taskflow",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
)
def hello():
    @task()
    def get_name():
        return "yanto"

    @task()
    def get_age():
        return 5

    @task()
    def greet(name, age):
        print(f"hello name:{name},age:{age}")

    name = get_name()
    age = get_age()
    greet(name, age)


first_taskflow = hello()
