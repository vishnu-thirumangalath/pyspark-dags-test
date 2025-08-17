# dags/kpo_xcom_hello.py
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="kpo_xcom_hello",
    start_date=datetime(2025, 8, 15),
    schedule=None,
    catchup=False,
    tags=["demo", "kpo", "xcom"],
) as dag:

    # This pod writes a valid JSON string ("hello world") to /airflow/xcom/return.json
    write_xcom = KubernetesPodOperator(
        task_id="write_xcom",
        name="write-xcom",
        namespace="airflow",
        image="alpine:3.20",
        cmds=["sh", "-c"],
        arguments=[
            "mkdir -p /airflow/xcom/ && "
            "echo '\"hello world\"' > /airflow/xcom/return.json;"
            "echo 'log: wrote xcom'; "
            "sleep 30"
        ],
        do_xcom_push=True,          # enable sidecar that collects /airflow/xcom/return.json
        in_cluster=True,
        service_account_name="airflow-webserver",
        get_logs=True,
        is_delete_operator_pod=True,          # clean up after success
        startup_timeout_seconds=120,
    )

    # This prints the XCom pushed by the pod
    read_xcom = BashOperator(
        task_id="read_xcom",
        bash_command='echo "{{ ti.xcom_pull(task_ids=\'write_xcom\') }}"',
    )

    write_xcom >> read_xcom
