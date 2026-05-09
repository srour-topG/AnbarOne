from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'anbar',
}

with DAG(
    dag_id='anbar_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval=None
) as dag:

    bronze_task = BashOperator(
        task_id='bronze_layer',
        bash_command='python /opt/airflow/Ingestion/bronze.py'
    )

    silver_task = BashOperator(
        task_id='silver_layer',
        bash_command='python /opt/airflow/Ingestion/silver.py'
    )

    gold_task = BashOperator(
        task_id='gold_layer',
        bash_command='python /opt/airflow/Ingestion/gold.py'
    )

    bronze_task >> silver_task >> gold_task