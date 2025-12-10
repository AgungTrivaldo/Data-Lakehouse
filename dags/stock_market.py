from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from minio import Minio
from datetime import datetime, timedelta
from io import BytesIO
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
    def fetch_symbol():
        api = BaseHook.get_connection("stock_api")
        response = requests.get(url, headers=api.extra_dejson.get("headers", {}))
        symbol= response.json()["chart"]["result"]["meta"]["symbol"][0]
        return json.dumps(symbol)

    @task
    def stock_prices(url, symbol):
        url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
        api = BaseHook.get_connection("stock_api")
        response = requests.get(url, headers=api.extra_dejson["headers"])
        data = response.json()["chart"]["result"][0]
        return json.dumps(data)

    @task
    def store_stock_price(stock_prices):
        minio = BaseHook.get_connection("minio")
        client = Minio(
            endpoint=minio.extra_dejson["endpoint"].split("//")[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False,
        )
        bucket_name = "storemarket"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        stock = json.loads(stock_prices)
        symbol = stock["meta"]["symbol"]
        data = json.dumps(stock, ensure_ascii=False).encode("utf8")
        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=f"{symbol}/prices.json",
            data=BytesIO(data),
            length=len(data),
        )
        return f"{objw.bucket_name}/{symbol}"

    url = is_api_available()
    stock_prices = stock_prices(url, symbol)
    fetch_symbol()
    store_stock_price(stock_prices)


stock_market()
