from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='antaq__gold_to_db_dag',
    default_args=default_args,
    description='DAG to ingest Gold Delta tables into MySQL',
    schedule_interval='0 14 1 * *',  # Executa no 1º dia de cada mês às 14:00
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['antaq', 'gold', 'mysql', 'delta'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    bash_command = (
        "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,"
        "io.delta:delta-spark_2.12:3.3.0,"
        "mysql:mysql-connector-java:8.0.33,"
        "io.delta:delta-core_2.12:2.4.0 /opt/airflow/jobs/gold_to_db/ingest_gold_to_db.py"
    )

    ingest_gold_to_db = BashOperator(
        task_id='ingest_gold_to_db',
        bash_command=bash_command
    )

    start >> ingest_gold_to_db >> end
