from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import request
from airflow.sensors.base import PokeReturnValue


@dag(
    dag_id="first_taskflowv1",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = Basehook.get_connection("api_connection")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = request.get(url, headers=api.extra_djson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    is_api_available


stock_market()
