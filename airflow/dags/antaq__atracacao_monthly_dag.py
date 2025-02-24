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
    dag_id="antaq__atracacao_monthly_dag",
    default_args=default_args,
    description="Unified pipeline to run Bronze, Silver, and Gold layers for atracacao, dependent on crawler DAG",
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["antaq", "atracacao", "monthly", "bronze", "silver", "gold"],
) as dag:

    # Sensor that waits for the final task ("end") in the crawler DAG
    wait_for_crawler = ExternalTaskSensor(
        task_id="wait_for_crawler",
        external_dag_id="antaq__crawler_dag",  # Replace with the actual crawler DAG ID
        external_task_id="end",               # Replace with the final task ID in your crawler DAG
        poke_interval=60,                     # Check every 60 seconds
        timeout=60 * 60,                      # Timeout after 1 hour (adjust as needed)
        mode="poke"                           # 'poke' or 'reschedule'
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # TaskGroup for the Bronze layer
    with TaskGroup("bronze", tooltip="Bronze tasks") as bronze_group:

        def run_script(script_name: str):
            """
            Executes the given Python script located in /opt/airflow/jobs/bronze.
            """
            script_path = f"/opt/airflow/jobs/bronze/{script_name}"
            runpy.run_path(script_path, run_name="__main__")

        bronze_scripts = [
            "bronze_antaq__atracacao.py",
        ]

        for script in bronze_scripts:
            PythonOperator(
                task_id=script.replace(".py", ""),
                python_callable=run_script,
                op_kwargs={'script_name': script},
            )

    # TaskGroup for the Silver layer
    with TaskGroup("silver", tooltip="Silver tasks") as silver_group:
        silver_scripts = [
            "silver_antaq__atracacao.py",
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

    # TaskGroup for the Gold layer
    with TaskGroup("gold", tooltip="Gold tasks") as gold_group:
        gold_scripts = [
            "gold_antaq__atracacao.py",
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

    # Define the execution order: first wait for crawler, then run Bronze >> Silver >> Gold
    wait_for_crawler >> start >> bronze_group >> silver_group >> gold_group >> end
