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

# Define a DAG
with DAG(
    'bronze_antaq__atracacao',
    default_args=default_args,
    description='Run all Bronze atracacao scripts in parallel',
    schedule_interval='0 12 1 * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bronze', 'antaq', 'atracacao', 'monthly'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    def run_script(script_name: str):
        """
        Executes the given Python script (located in /opt/airflow/jobs/bronze) 
        as if it were run directly (including the `if __name__ == '__main__':` block).
        """
        script_path = f"/opt/airflow/jobs/bronze/{script_name}"
        runpy.run_path(script_path, run_name="__main__")

    atracacao_scripts = [
        "bronze_antaq__atracacao.py",
    ]

    atracacao_tasks = []
    for script_file in atracacao_scripts:
        task_id_name = script_file.replace(".py", "")

        task = PythonOperator(
            task_id=task_id_name,
            python_callable=run_script,
            op_kwargs={'script_name': script_file},
        )
        atracacao_tasks.append(task)

    end = EmptyOperator(
        task_id='end'
    )

    start >> atracacao_tasks >> end