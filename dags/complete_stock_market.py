from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from io import BytesIO

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
        csv = f"stockmarket/symbol/stock_symbol_clean.csv"
        client = minio_client()
        with open(csv,"rb") as f:
            data = f.read

        client.put_object(
            bucket_name="stockmarket",
            object_name=f"symbol/stock_symbol_clean.csv",
            data=BytesIO(data),
            length=len(data),
            content_type="text/csv"
    )
        return data

    def test_api():
        api = BaseHook.get_connection("stock_api")
        full_url = f"{api.host}{api.extra_dejson['endpoint']}"
        for symbol in symbols:
            url = f"{full_url}{symbol}?metrics=high?&interval=1d&range=1y"
            print(url)
    

    symbols = get_symbol()

stock_market()