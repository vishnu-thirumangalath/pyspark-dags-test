import os
import json
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# Define the connection ID
# K8S_CONN_ID = "k8s_conn_id"

# Create the AIRFLOW_CONN_* env var
# os.environ[f"AIRFLOW_CONN_{K8S_CONN_ID.upper()}"] = json.dumps({
#     "conn_type": "kubernetes",
#     "extra": {
#         "in_cluster": True,
#         "namespace": "test"  # Default namespace for this connection
#     }
# })

# DAG definition
with DAG(
    dag_id="run_pyspark_test",
    start_date=datetime(2025, 8, 15),
    schedule_interval=None,  # Run manually for now
    catchup=False,
    tags=["pyspark", "k8s"],
) as dag:

    run_pyspark_job = KubernetesPodOperator(
        task_id="run_pyspark_job",
        name="pyspark-test-pod",
        labels={"app": "pyspark-job", "team": "data"},
        namespace="airflow",  # Runs in namespace test
        service_account_name="airflow-webserver",
        image="ghcr.io/vishnu-thirumangalath/docker-images/pyspark-dags-test:latest",
        cmds=["python", "/app/pyspark_test.py"],  # matches dockerfile (refer Dockerfile)
        get_logs=True,
        is_delete_operator_pod=False,  # pod termination
        # kubernetes_conn_id="k8s_conn_id"
    )
