from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import runpy

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define a DAG
with DAG(
    'antaq_crawler_dag',
    default_args=default_args,
    description='Run ANTAQ crawler to download and process ZIP files into MinIO',
    schedule_interval='0 12 1 * *',  # Roda ao meio-dia do primeiro dia de cada mês
    start_date=datetime(2023, 1, 1),  # Ajuste a data de início conforme necessário
    catchup=False,  # Impede a execução retroativa das tarefas
    tags=['antaq', 'monthly', 'crawler'],
) as dag:

    def run_antaq_crawler():
        # Caminho absoluto onde o script está montado no container
        script_path = '/opt/airflow/jobs/crawlers/crawler_antaq.py'
        # Executa o script como se estivesse sendo chamado como o módulo principal
        runpy.run_path(script_path, run_name="__main__")

    # Define a task
    antaq_crawler_task = PythonOperator(
        task_id='run_antaq_crawler',
        python_callable=run_antaq_crawler,
    )
