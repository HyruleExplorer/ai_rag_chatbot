# Importing all needed libraries
from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from airflow.models import Variable

KUBERNETES_NAMESPACE = Variable.get("KUBERNETES_NAMESPACE")
S3_ACCESS_KEY_ID = Variable.get("s3_access_key_id")
S3_SECRET_ACCESS_KEY = Variable.get("s3_sec_access_key_id")
DOCKER_IMAGE = "https://hub.docker.com/r/hyruleexplorer/ai_rag_chatbot:latest"
CONN_ID = "ai_rag_chatbot"
table_schema = "public"
table_name = "tb_ai_rag_chatbot_doc_dev"
s3_bucket_name = "ai_rag_chatbot"

pod_env_vars = [
    k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value=S3_ACCESS_KEY_ID),
    k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value=S3_SECRET_ACCESS_KEY),
    k8s.V1EnvVar(name="S3_BUCKET", value=s3_bucket_name),
    k8s.V1EnvVar(name="DB_USER", value="{{ conn['POSTGRES'].login }}"),
    k8s.V1EnvVar(name="DB_PW", value="{{ conn['POSTGRES'].password }}"),
    k8s.V1EnvVar(name="DB_HOST", value="{{ conn['POSTGRES'].host }}"),
    k8s.V1EnvVar(name="DB_NAME", value="{{ conn['POSTGRES'].schema }}"),
    k8s.V1EnvVar(name="DB_PORT", value="{{ conn['POSTGRES'].port }}"),
]

# This sets the default arguments for the DAG.
default_args = {
    "owner": "vpervendetta1992",
    # Change the email to the one where you want to get notifications about tasks failures.
    "email": [
        "vpervendetta1992@gmail.com",
    ],
    # If a task fails, Airflow will send an email to the address specified above.
    "email_on_failure": True,
    # if a task fails, Airflow will try 2 other times before giving up.
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def vdb_refresh(**context):  # Call refreshData endpoint in ai_rag_chatbot
    run_hook = HttpHook(method="POST", http_conn_id=CONN_ID)
    api_endpoint = "refreshData"
    print(api_endpoint)

    run_hook.run(
        endpoint=api_endpoint,
        headers={"Content-Type": "text/html; charset=UTF-8"},
        extra_options={"verify": False, "check_response": False, "timeout": 360},
    )


with DAG(
    "dag_ai_rag_chatbot_create_vdb",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2022, 10, 13),
    # At 05:00 on day-of-month 2, every month.
    schedule_interval="0 0 2 * *",
    max_active_runs=1,
    template_searchpath="/efs/git-sync/git/repo",
) as dag:

    run_create_vdb = KubernetesPodOperator(
        task_id="run_create_vdb",
        name="run_create_vdb",
        namespace=KUBERNETES_NAMESPACE,
        image=DOCKER_IMAGE,
        cmds=[
            "bash",
            "-c",
            f"python create_vdb.py '{table_schema}' '{table_name}' '{s3_bucket_name}'",
        ],
        env_vars=pod_env_vars,
        is_delete_operator_pod=True,
        startup_timeout_seconds=240,
    )

    fgpt_vdb_refresh = PythonOperator(
        task_id="fgpt_vdb_refresh",
        python_callable=vdb_refresh,
    )


# Finally here you define the flow of the DAG tasks.
# The immediate parent of each task is the task that preceeds it.
run_create_vdb >> fgpt_vdb_refresh
# get_value_from_gp_task >> send_email_hello_world_task >> cleanup_xcom_hello_world_task
