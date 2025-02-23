from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'antaq_crawler_dag',
    default_args=default_args,
    description='Run ANTAQ crawler to download and process ZIP files into MinIO',
    schedule_interval='0 12 1 * *',  # Runs at noon (12:00) on the 1st of every month
    start_date=datetime(2023, 1, 1),  # Adjust start date as needed
    catchup=False,  # Prevents backfilling for past dates
    tags=['antaq', 'monthly', 'crawler'],
) as dag:

    def run_antaq_crawler():
        # Use the absolute path where jobs is mounted in the container
        script_path = '/opt/airflow/jobs/crawlers/crawler_antaq.py'
        with open(script_path, 'r') as file:
            exec(file.read())

    # Define the task
    antaq_crawler_task = PythonOperator(
        task_id='run_antaq_crawler',
        python_callable=run_antaq_crawler,
    )