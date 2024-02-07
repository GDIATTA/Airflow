from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator


default_args = {
    'owner': 'gauss',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='dag_with_air_poll',
    default_args=default_args,
    start_date=datetime(2024, 2, 6),
    schedule_interval='@daily',
    #catchup=False
) as dag:
    task1 = HttpSensor(
        task_id ='conect_api_air_poll',
        http_conn_id='weathermap_api',
        endpoint='/feed/toulouse/?token=47745c182fb9aee4c14e5234e4ad010b13083722'
        )
    task2_extract = SimpleHttpOperator(
        task_id = 'extract_air_poll_data',
        http_conn_id = 'weathermap_api',
        endpoint='/feed/toulouse/?token=47745c182fb9aee4c14e5234e4ad010b13083722',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
    task1 >> task2_extract