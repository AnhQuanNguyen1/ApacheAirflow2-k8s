from datetime import timedelta
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import time

default_args = {
    'owner': 'quanna8',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dbt_airflow',
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=timedelta(days=1),
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
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False
)

dbt_task