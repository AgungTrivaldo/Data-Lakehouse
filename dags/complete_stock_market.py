from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    def fetch_stock_prices(url):
        api = BaseHook.get_connection("stock_api")
        response = requests.get(url, headers=api.extra_dejson["headers"])
        data = response.json()["chart"]["result"][0]
        return data
    
    symbols = get_symbol()
    urls = get_link(symbols)
    stock_prices = fetch_stock_prices.expand(url = urls)

stock_market()