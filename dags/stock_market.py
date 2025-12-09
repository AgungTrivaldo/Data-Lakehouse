from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
from airflow.sensors.base import PokeReturnValue


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
    def stock_prices(url):
        api = BaseHook.get_connection("stock_api")
        response = requests.get(url, headers=api.extra_dejson["headers"])
        symbols = response.json()['symbols']  # assuming the API gives this
        results = {}
        for symbol in symbols:
            stock_url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
            res = requests.get(stock_url, headers=api.extra_dejson["headers"]).json()
            results[symbol] = res['chart']['result'][0]
        return json.dumps(results)

    url = is_api_available()
    stock_prices(url)


stock_market()
