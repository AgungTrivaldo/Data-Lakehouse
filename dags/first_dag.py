from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "Agung", "retries": "5", "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="First_Dag_v3",  # DAG Name
    default_args=default_args,
    description="This is my First Dag",  # DAG description
    start_date=datetime(2025, 1, 1),  # DAG Start Date
    schedule_interval="@daily",  # DAG frequency
) as dag:
    task1 = BashOperator(
        task_id="first_task",  # task id or task name
        bash_command="echo This is my first DAG",  # command
    )

    task2 = BashOperator(
        task_id="second_task", bash_command="echo This is the second task"
    )
    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo This is the third task that run parallel with the second task",
    )
    task4 = BashOperator(
        task_id="fourth_task",
        bash_command="echo This is the Fourth task that run after the second task",
    )

    task5 = BashOperator(
        task_id="fifth_task",
        bash_command="echo This is the Fifth task that run before the first task",
    )
    # task1.set_downstream(
    #     task2
    # )  # bakal nyambung ke task yang pertama jadi Task1 -> Task2

    # task1.set_upstream(
    #     task2
    # )  # bakal nyambu ke task yang pertama tapi jadinya Task2 -> Task1

    # task1.set_downstream(
    #     task3
    # )  # kalo semisam ada 2 downstream maka task yang nyambung ke parentnya jadi 2 task dan berjalan bersamaan

    # ATAU BISA PAKE INI BIAR LEBIH SIMPLE
    # task5 << task1 >> task2 >> task4 (ENDNYA di task 4 lewat dari task 2)
    # task1 >> task3 (Endnya di task 3 langsung tanpa harus melewati task 2 dan task 4)

    # PAKE YANG ATAS KALO END UPNYA BEDA

    task5 << task1 >> [task2, task3] >> task4
    # pake ini kalo semisal udah diparalel dan endpointnya sama sama di task 4

    # >> buat downstream
    # << buat Upstream
