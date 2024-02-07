from airflow import DAG
#from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def custom_transformation(bucketname, sourcekey, destinationkey):

 s3_hook = S3Hook(aws_conn_id='aws_conn')

#Read S3 file
 content = s3_hook.read_key(bucket_name=bucketname, key=sourcekey)

# Applly Custom Transformations
 transformed_content = content.upper()

# Load S3 file
 s3_hook.load_string(transformed_content, bucket_name=bucketname, key=destinationkey)

dag_owner = ''

default_args = {
        'owner': 'gauss',
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dag_with_hooks_s3',
        default_args=default_args,
        description='transfer some file from local into S3 bucket AWS',
        start_date=datetime(2024,2,3),
        schedule_interval='@daily',
) as dag :
   transform_task = PythonOperator(
                 task_id='transform_task',
                 python_callable=custom_transformation,
                 op_args=['firstbucketgauss', 'customers.csv', 'customer_transformed.csv']
)
transform_task