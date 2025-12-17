from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from io import BytesIO
import requests
import pandas as pd

@dag(
    dag_id="stock_market",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def stock_market():
    def minio_client():
        minio = BaseHook.get_connection("minio")
        client = Minio(
            endpoint=minio.extra_dejson["endpoint"].split("//")[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False,
        )
        return client
    
    @task
    def get_symbol():
        client = minio_client()
        csv = client.get_object(
            bucket_name="stockmarket",
            object_name="symbol/stock_symbol_clean.csv"
        )

        csv = pd.read_csv(csv)
        symbols = csv['symbol'].tolist()
        print(symbols)
        return symbols
    @task
    def get_link(symbols):
        api = BaseHook.get_connection("stock_api")
        full_url = f"{api.host}{api.extra_dejson['endpoint']}"
        urls = []
        for symbol in symbols:
            url = f"{full_url}{symbol}?metrics=high?&interval=1d&range=1y"
            urls.append(url)
        return urls
    @task
    def stock_prices(urls):
        api = BaseHook.get_connection("stock_api")
        full_url = f"{api.host}{api.extra_dejson['endpoint']}"
        url = f"{full_url}{symbol}?metrics=high&interval=1d&range=1y"

        headers = api.extra_dejson.get("headers", {})
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()["chart"]["result"][0]
        return data
    
    symbols = get_symbol()
    fetch_results = fetch_stock_price.expand(symbol=symbols)

stock_market()