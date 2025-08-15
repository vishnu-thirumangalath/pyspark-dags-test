from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
    dag_id="simple_k8s_pod_dag",
    start_date=datetime(2025, 8, 14),
    schedule_interval=None,  # run manually for test
    catchup=False,
    tags=["test", "k8s"]
) as dag:

    hello_pod = KubernetesPodOperator(
        task_id="hello_pod",
        name="hello-pod",
        namespace="airflow",  # same namespace as your Airflow deployment
        image="busybox",       # super small image
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from inside the pod!' && sleep 5"],
        get_logs=True
    )

    hello_pod
