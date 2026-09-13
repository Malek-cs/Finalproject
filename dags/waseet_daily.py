from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DATA_DIR = "/opt/airflow/data"

def alert_failure(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    print(f"[CRITICAL ALERT] Task {task_id} failed in DAG {dag_id} on {execution_date}!")

def choose_branch(ds, **kwargs):
    file_path = f"{DATA_DIR}/scans_{ds}.csv"
    if not os.path.exists(file_path):
        return "skip_day"
    
    # Check if empty or only headers (e.g., Eid May 15)
    df = pd.read_csv(file_path, dtype=str)
    if df.empty or len(df) == 0:
        return "skip_day"
    return "run_pipeline"

default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': alert_failure,
}

with DAG(
    'waseet_daily',
    default_args=default_args,
    description='Daily ingestion, transformation, and quality pipeline for Waseet',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 1),
    end_date=datetime(2026, 5, 21),
    catchup=False,
) as dag:

    # 30s timeout so May 10 triggers failure callback quickly
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=f"{DATA_DIR}/scans_{{{{ ds }}}}.csv",
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=30,
        mode='reschedule',
    )

    check_branch = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=choose_branch,
    )

    run_pipeline = BashOperator(
        task_id='run_pipeline',
        bash_command='python /opt/airflow/pipeline/pipeline.py {{ ds }}',
    )

    run_quality = BashOperator(
        task_id='run_quality',
        bash_command='python /opt/airflow/pipeline/quality.py {{ ds }}',
    )

    skip_day = EmptyOperator(
        task_id='skip_day',
    )

    finish = EmptyOperator(
        task_id='finish',
        trigger_rule='none_failed_min_one_success',  # Allows successful DAG runs when skipping
    )

    wait_for_file >> check_branch
    check_branch >> run_pipeline >> run_quality >> finish
    check_branch >> skip_day >> finish