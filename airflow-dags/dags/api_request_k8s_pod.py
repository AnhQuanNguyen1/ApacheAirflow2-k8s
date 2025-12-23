from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


default_args = {
    "description": "Orchestrate weather API ingestion by running a K8s pod",
    "start_date": datetime(2025, 12, 22),
    "catchup": False,
}

with DAG(
    dag_id="weather-api-orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["k8s", "api-request"],
) as dag:

    ingest_data_task = KubernetesPodOperator(
        task_id="ingest_data_task",
        name="api-request-run",
        namespace="api-request", 
        image="naq113469/api-request:latest",
        image_pull_policy="Always",
        service_account_name="api-request", 

        # Nếu image PRIVATE thì bỏ comment và dùng đúng secret name (vd: regcred)
        # image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],

        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=False,
        on_finish_action='keep_pod',
        do_xcom_push=False,

        cmds=["/bin/sh", "-c"],
        arguments=["python -c 'from insert_records_clickhouse import main; main()'"],

        # env_vars={
        #     "ENV": "prod",
        #     # "CLICKHOUSE_HOST": "...",
        #     # "CLICKHOUSE_USER": "...",
        #     # "CLICKHOUSE_PASSWORD": "...",
        # },

        labels={"app": "api-request", "dag": "weather-api-orchestrator"},
    )
