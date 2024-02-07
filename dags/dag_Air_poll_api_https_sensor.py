from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor


default_args = {
    'owner': 'gauss',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2024, 2, 7),
    schedule_interval=@daily
    catchup=False
) as dag:
    task1 = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=5031cde3d1a8b9469fd47e998d7aef79'
        )