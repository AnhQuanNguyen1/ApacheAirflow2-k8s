from datetime import timedelta
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import time

default_args = {
    'owner': 'quanna8',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dbt_airflow',
    catchup=False,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/dbt/'
)

dbt_task = KubernetesPodOperator(
    task_id='dbt_airflow_pod',
    name='dbt_airflow_pod',
    namespace='dbt',
    image='naq113469/dbt:latest', 
    image_pull_policy='Always',
    service_account_name="dbt", 
    cmds=["dbt"],
    arguments=[
        "run",
        "--profiles-dir", "/dbt/profiles",
        "--fail-fast",
        "--select", "staging.stg_weather_data",
        "--select", "mart.daily_weather",
        "--select", "mart.weather_report",
    ],
    in_cluster=True,
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False
)

dbt_task