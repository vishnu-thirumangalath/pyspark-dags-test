from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
    dag_id="hello_world_kpo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "kpo"],
) as dag:

    hello = KubernetesPodOperator(
        task_id="say_hello",
        name="hello-world",
        namespace="airflow",
        image="python:3.11-alpine",
        cmds=["python", "-c"],
        arguments=['print("hello world from inside a pod!")'],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        service_account_name="airflow-webserver",

        resources={
            "request_cpu": "50m",
            "request_memory": "64Mi",
            "limit_cpu": "200m",
            "limit_memory": "256Mi",
        },

        startup_timeout_seconds=120,
    )
