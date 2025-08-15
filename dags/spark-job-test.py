from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

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
        namespace="test",  # Runs in namespace test
        image="ghcr.io/vishnu-thirumangalath/docker-images/pyspark-dags-test:latest",
        cmds=["python", "/app/pyspark_test.py"],  # matches dockerfile (refer Dockerfile)
        get_logs=True,
        is_delete_operator_pod=True,  # pod termination
        kubernetes_conn_id="k8s_conn_id"
    )
