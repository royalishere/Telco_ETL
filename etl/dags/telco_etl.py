from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

with DAG(
    'telco_etl',
    start_date=datetime(2025, 6, 10),
    schedule='@daily',
    catchup=False,
    tags=['telco'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    task_extract >> task_transform >> task_load
