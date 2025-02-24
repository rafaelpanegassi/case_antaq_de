import runpy
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="antaq__instalacao_monthly_dag",
    default_args=default_args,
    description="Unified pipeline to run instalacao scripts for Bronze, Silver, and Gold layers sequentially",
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["antaq", "instalacao", "monthly", "bronze", "silver", "gold"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Bronze TaskGroup (using PythonOperator to execute Python scripts)
    with TaskGroup("bronze", tooltip="Bronze instalacao tasks") as bronze_group:
        def run_script(script_name: str):
            """
            Executes the given Python script located in /opt/airflow/jobs/bronze.
            """
            script_path = f"/opt/airflow/jobs/bronze/{script_name}"
            runpy.run_path(script_path, run_name="__main__")
            
        bronze_scripts = [
            "bronze_antaq__instalacao_destino.py",
            "bronze_antaq__instalacao_origem.py",
        ]
        
        for script in bronze_scripts:
            PythonOperator(
                task_id=script.replace(".py", ""),
                python_callable=run_script,
                op_kwargs={'script_name': script},
            )

    # Silver TaskGroup (using BashOperator to execute spark-submit commands)
    with TaskGroup("silver", tooltip="Silver instalacao tasks") as silver_group:
        silver_scripts = [
            "silver_antaq__instalacao_destino.py",
            "silver_antaq__instalacao_origem.py",
        ]
        
        for script in silver_scripts:
            bash_command = (
                f"spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,"
                f"io.delta:delta-spark_2.12:3.3.0 /opt/airflow/jobs/silver/{script}"
            )
            BashOperator(
                task_id=script.replace(".py", ""),
                bash_command=bash_command,
            )

    # Gold TaskGroup (using BashOperator to execute spark-submit commands)
    with TaskGroup("gold", tooltip="Gold instalacao tasks") as gold_group:
        gold_scripts = [
            "gold_antaq__instalacao_origem.py",
            "gold_antaq__instalacao_destino.py",
        ]
        
        for script in gold_scripts:
            bash_command = (
                f"spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,"
                f"io.delta:delta-spark_2.12:3.3.0 /opt/airflow/jobs/gold/{script}"
            )
            BashOperator(
                task_id=script.replace(".py", ""),
                bash_command=bash_command,
            )

    # Execution order: Bronze >> Silver >> Gold
    start >> bronze_group >> silver_group >> gold_group >> end
