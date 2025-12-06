from datetime import timedelta, datetime
from airflow.decorators import dag, task


default_args = {"owner": "agung", "retries": 5, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="first_taskflowv1",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
)
def hello():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'firstname':'Yanto',
            'lastname':'Suyanto'
        }

    @task()
    def get_age():
        return 5

    @task()
    def greet(firstname,lastname, age):
        print(f"hello name:{firstname}{lastname},age:{age}")

    name = get_name()
    age = get_age()
    greet(firstname=name['firstname'],lastname=name['lastname'], age=age)


first_taskflow = hello()
