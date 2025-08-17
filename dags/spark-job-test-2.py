import os
import json
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

K8S_CONN_ID = "k8s_conn_id"

# Create in-cluster Kubernetes connection
os.environ[f"AIRFLOW_CONN_{K8S_CONN_ID.upper()}"] = json.dumps({
    "conn_type": "kubernetes",
    "extra": {
        "in_cluster": True,
        "namespace": "test"
    }
})

with DAG(
    dag_id="run_pyspark_test_2",
    start_date=datetime(2025, 8, 15),
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "k8s", "prometheus"],
) as dag:

    run_pyspark_job = KubernetesPodOperator(
        task_id="run_pyspark_job_2",
        name="pyspark-test-pod-2",
        labels={"app": "pyspark-job", "team": "data"},
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/pyspark-dags-test:latest",
        cmds=["python", "pyspark_test.py"],
        arguments=[
            "--master", "local[*]",  # Local mode for test
            "--conf", "spark.metrics.conf=/opt/spark/conf/metrics.properties",
            "--conf", "spark.ui.prometheus.enabled=true",
            "--conf", "spark.driver.bindAddress=0.0.0.0",
            "/app/pyspark_test.py",
        ],
        get_logs=True,
        is_delete_operator_pod=False,  # keep pod for debugging
    )
