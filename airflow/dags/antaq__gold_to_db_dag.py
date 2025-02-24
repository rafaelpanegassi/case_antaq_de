from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='antaq__gold_to_db_dag',
    default_args=default_args,
    schedule_interval='0 14 1 * *',  # Runs on the 1st of each month at 14:00
    description='DAG to ingest Gold Delta tables into MySQL',
    tags=['antaq', 'gold', 'mysql', 'delta'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    ingest_gold_to_db = BashOperator(
        task_id='ingest_gold_to_db',
        bash_command=(
            "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,"
            "io.delta:delta-spark_2.12:3.3.0,"
            "mysql:mysql-connector-java:8.0.33,"
            "io.delta:delta-core_2.12:2.4.0 jobs/gold_to_db/ingest_gold_to_db.py"
        )
    )

    start >> ingest_gold_to_db >> end
