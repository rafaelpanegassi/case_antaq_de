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
    dag_id="silver_antaq__carga_regiao_dag",
    default_args=default_args,
    description="Silver Carga files to Silver Delta table",
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["silver", "carga", "regiao", "monthly", "antaq"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Lista dos scripts a serem executados
    carga_scripts = [
        "silver_antaq__carga_regiao.py",
    ]

    # Cria uma tarefa para cada script usando BashOperator e spark-submit
    carga_tasks = []
    for script in carga_scripts:
        task_id_name = script.replace(".py", "")
        bash_command = (
            f"spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,"
            f"io.delta:delta-spark_2.12:3.3.0 /opt/airflow/jobs/silver/{script}"
        )
        task = BashOperator(
            task_id=task_id_name,
            bash_command=bash_command,
        )
        carga_tasks.append(task)

    end = EmptyOperator(task_id="end")

    start >> carga_tasks >> end