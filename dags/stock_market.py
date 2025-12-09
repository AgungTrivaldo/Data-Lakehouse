from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
from airflow.sensors.base import PokeReturnValue

symbol = "NVDA"


@dag(
    dag_id="stock_market_api",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    @task
    def stock_prices(url, symbol):
        url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
        api = BaseHook.get_connection("stock_api")
        response = requests.get(url, headers=api.extra_dejson["headers"])
        data = response.json()["chart"]["result"][0]
        return json.dumps(data)

    url = is_api_available()
    stock_prices(url, symbol)


stock_market()
