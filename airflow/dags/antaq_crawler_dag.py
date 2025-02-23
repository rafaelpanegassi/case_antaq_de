import runpy
from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'antaq_crawler_dag',
    default_args=default_args,
    description='Run ANTAQ crawler to download and process ZIP files into MinIO',
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['antaq', 'monthly', 'crawler'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    def run_antaq_crawler():
        script_path = '/opt/airflow/jobs/crawlers/crawler_antaq.py'
        runpy.run_path(script_path, run_name="__main__")

    antaq_crawler_task = PythonOperator(
        task_id='run_antaq_crawler',
        python_callable=run_antaq_crawler,
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> antaq_crawler_task >> end