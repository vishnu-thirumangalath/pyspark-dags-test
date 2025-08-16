import os
import json
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# Kubernetes connection ID
K8S_CONN_ID = "k8s_conn_id"

# Create Airflow K8S connection dynamically (for in-cluster)
os.environ[f"AIRFLOW_CONN_{K8S_CONN_ID.upper()}"] = json.dumps({
    "conn_type": "kubernetes",
    "extra": {
        "in_cluster": True,
        "namespace": "test"
    }
})

with DAG(
    dag_id="run_pyspark_test",
    start_date=datetime(2025, 8, 15),
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "k8s"],
) as dag:

    run_pyspark = KubernetesPodOperator(
        task_id="spark_submit",
        name="spark-submit",
        namespace="test",  # spark-submit pod namespace
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/pyspark-dags-test:latest",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",
            "--deploy-mode", "cluster",
            "--name", "pyspark-etl",
            "--conf", "spark.kubernetes.namespace=test",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=dagsvc",
            "--conf", "spark.kubernetes.container.image=ghcr.io/vishnu-thirumangalath/docker-images/pyspark-dags-test:latest",
            "--conf", "spark.metrics.conf=/opt/spark/conf/metrics.properties",
            "/app/pyspark_test.py",
        ],
        get_logs=True,
        do_xcom_push=True,
        env_vars={"AIRFLOW__XCOM_RETURN_PATH": "/airflow/xcom/return.json"},
        is_delete_operator_pod=True,
        kubernetes_conn_id=K8S_CONN_ID,
    )
