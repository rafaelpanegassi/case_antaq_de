import runpy
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="antaq__carga_monthly_dag",
    default_args=default_args,
    description="Unified pipeline to run all carga scripts for Bronze, Silver, and Gold layers, dependent on crawler DAG",
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["antaq", "carga", "monthly", "bronze", "silver", "gold"],
) as dag:

    # Sensor that waits for the final task ("end") in the crawler DAG
    wait_for_crawler = ExternalTaskSensor(
        task_id="wait_for_crawler",
        external_dag_id="antaq__crawler_dag",  # The crawler DAG ID
        external_task_id="end",               # The final task ID in the crawler DAG
        poke_interval=60,                     # Check every 60 seconds
        timeout=60 * 60,                      # Timeout after 1 hour
        mode="poke"                           # or 'reschedule'
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Bronze TaskGroup (using PythonOperator to execute Python scripts)
    with TaskGroup("bronze", tooltip="Bronze tasks") as bronze_group:

        def run_script(script_name: str):
            """
            Executes the given Python script located in /opt/airflow/jobs/bronze.
            """
            script_path = f"/opt/airflow/jobs/bronze/{script_name}"
            runpy.run_path(script_path, run_name="__main__")
            
        bronze_scripts = [
            "bronze_antaq__carga_conteinerizada.py",
            "bronze_antaq__carga_hidrovia.py",
            "bronze_antaq__carga_rio.py",
            "bronze_antaq__carga_regiao.py",
            "bronze_antaq__carga.py",
        ]
        
        for script in bronze_scripts:
            PythonOperator(
                task_id=script.replace(".py", ""),
                python_callable=run_script,
                op_kwargs={'script_name': script},
            )

    # Silver TaskGroup (using BashOperator to execute spark-submit commands)
    with TaskGroup("silver", tooltip="Silver tasks") as silver_group:
        silver_scripts = [
            "silver_antaq__carga_conteinerizada.py",
            "silver_antaq__carga.py",
            "silver_antaq__carga_hidrovia.py",
            "silver_antaq__carga_regiao.py",
            "silver_antaq__carga_rio.py",
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
    with TaskGroup("gold", tooltip="Gold tasks") as gold_group:
        gold_scripts = [
            "gold_antaq__carga_conteinerizada.py",
            "gold_antaq__carga.py",
            "gold_antaq__carga_hidrovia.py",
            "gold_antaq__carga_regiao.py",
            "gold_antaq__carga_rio.py",
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

    # Execution order: first wait_for_crawler, then Bronze >> Silver >> Gold
    wait_for_crawler >> start >> bronze_group >> silver_group >> gold_group >> end
