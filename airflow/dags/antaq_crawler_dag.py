from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_crawler():
    from jobs.crawlers.crawler_antaq import main
    main()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'antaq_crawler_dag',
    default_args=default_args,
    description='DAG para executar o ANTAQ crawler',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    crawler_task = PythonOperator(
        task_id='run_crawler_task',
        python_callable=run_crawler
    )

    crawler_task
