from datetime import timedelta, datetime
from airflow.decorators import dag, task 


default_args = {"owner": "agung", "retries": 5, "retry_delay": timedelta(minutes=5)}

# kalo pake taskflow declare nya pake @ bukan with
@dag(
    dag_id="first_taskflowv1",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
)
def hello():
    # kalo kita pake taskflow kita cuma perlu declare tasknya aja,nanti depedency nya bakal ngikut sesuai kebutuhan
    @task(multiple_outputs=True) # kalo mau ngelakuin multiple output masukin ini multiple_ouput = true di parameter tasknya
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
