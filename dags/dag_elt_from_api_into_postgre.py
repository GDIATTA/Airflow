from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="task2_extract")
    temp = data["main"]["temp"]
    temp_min = data["main"]["temp_min"]
    description = data["weather"][0]["description"]

    transformed_data = {"temp": temp,
                        "temp_min": temp_min,
                        "description": description
    }

    transformed_data_list = [transformed_data]
    df = pd.DataFrame(transformed_data_list)
    df.to_csv("./data_extra_from_api_weathermap.csv", index=False)

default_args = {
    'owner': 'gauss',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='dag_elt_from_api_into_postgres',
    default_args=default_args,
    start_date=datetime(2024, 2, 8),
    schedule_interval='@daily'
) as dag:
    
    task1 = HttpSensor(
        task_id ='conect_weather_map',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=paris&appid=8630592e008d5fa001fc61cd192fa020'
        )
    
    task2_extract = SimpleHttpOperator(
        task_id = 'task2_extract',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=paris&appid=8630592e008d5fa001fc61cd192fa020',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
    
    task3_transf = PythonOperator(
      task_id='task3_transf',
      python_callable= transform_load_data
    )

    task4 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists weathermap (
                temp date,
                temp_min character varying,
                description character varying,
                primary key (temp, temp_min)
            )
        """
    )

    
    task1 >> task2_extract >> task3_transf