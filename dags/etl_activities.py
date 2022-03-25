import json
import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from Fetch import Fetch

default_args = {
    'start_date': datetime(year=2021, month=5, day=12)
}

fetch = Fetch()


def extract_users(url: str, ti) -> None:
    res = requests.get(url)
    json_data = json.loads(res.content)
    ti.xcom_push(key='extracted_users', value=json_data)


with DAG(
        dag_id='etl_activities',
        default_args=default_args,
        schedule_interval='@daily',
        description='ETL pipeline for pulling ftp csv to sqlite'
) as dag:
    # Task 1 - Fetch user data from the API
    task_extract_users = PythonOperator(
        task_id='download_csv',
        python_callable=extract_users,
        op_kwargs={'url': 'https://jsonplaceholder.typicode.com/users'}
    )

    # Task 2 - Transform fetched users
    task_transform_users = PythonOperator(
        task_id='upload_by_chunks',
        python_callable=fetch.read_by_chunk()
    )

    task_extract_users >> task_transform_users
