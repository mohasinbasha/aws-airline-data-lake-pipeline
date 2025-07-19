# Purpose: This DAG simulates ingesting a file from local or another source into S3 bucket daily.
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohasin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 7, 1)
}

with DAG(dag_id='airline_data_ingestion_dag',
         default_args=default_args,
         description='Daily push mock airline data to S3',
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest_data = BashOperator(
        task_id='upload_csv_to_s3',
        bash_command='aws s3 cp /home/user/data/flights_sample.csv s3://airline-datalake-ingest/raw/'
    )