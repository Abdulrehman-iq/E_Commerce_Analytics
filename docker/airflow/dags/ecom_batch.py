from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_spark_job():
    import subprocess
    subprocess.run(["spark-submit", "/app/batch_processing/spark_etl.py"], check=True)

with DAG(
    'ecom_batch_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    process_data_with_spark = PythonOperator(
        task_id='process_data_with_spark',
        python_callable=run_spark_job
    )

    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='echo "Running data quality checks..."'
    )

    process_data_with_spark >> data_quality_check
