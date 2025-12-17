from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from io import BytesIO
import json
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
        stock_prices = []
        for url in urls:
            response = requests.get(url, headers=api.extra_dejson["headers"])
            data = response.json()["chart"]["result"][0]
            stock_prices.append(data)
        return stock_prices
    
    @task
    def store_stock_price(stock_price):
        client = minio_client()

        if not client.bucket_exists("storemarket"):
            client.make_bucket("storemarket")

        # If API response is wrapped in a list, unwrap it
        if isinstance(stock_price, list):
            stock_price = stock_price[0]

        symbol = stock_price["meta"]["symbol"]

        timestamps = stock_price["timestamp"]
        quote = stock_price["indicators"]["quote"][0]

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(timestamps, unit="s"),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "close": quote.get("close"),
            "volume": quote.get("volume"),
        })

        df["symbol"] = symbol

        csv_bytes = df.to_csv(index=False).encode("utf-8")

        client.put_object(
            bucket_name="storemarket",
            object_name=f"{symbol}/prices.csv",
            data=BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )

    symbols = get_symbol()
    urls = get_link(symbols)
    stock_price = stock_prices(urls)
    store_stock_price.expand(stock_price)
stock_market()