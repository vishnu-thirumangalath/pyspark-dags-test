from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models.baseoperator import chain

with DAG(
    dag_id="hello_world_kpo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "kpo"],
) as dag:

    # Short-lived pod that prints "hello world" and exits 0
    hello = KubernetesPodOperator(
        task_id="say_hello",
        name="hello-world",
        namespace="airflow",
        image="python:3.11-alpine",
        cmds=["python", "-c"],
        arguments=['print("hello world from inside a pod!")'],
        get_logs=True,                    # Stream logs into Airflow UI
        is_delete_operator_pod=True,      # Clean up after success
        in_cluster=True,                  # Use in-cluster config
        service_account_name="airflow-kpo",
        container_resources={
            "requests": {"cpu": "50m", "memory": "64Mi"},
            "limits":   {"cpu": "200m", "memory": "256Mi"},
        },
        # Optional: tolerate pod scheduling hiccups
        startup_timeout_seconds=120,
    )

    chain(hello)
