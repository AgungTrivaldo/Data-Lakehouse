from airflow.decorators import dag
from datetime import datetime

@dag(
    start = datetime(2025,1,1),
    schedule = '@daily',
    catchup = False,
    tags = ['stock']
    )
def stock_marker():
    pass

stock_marker()