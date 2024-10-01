# Importing all needed libraries
from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.models import Variable

KUBERNETES_NAMESPACE = Variable.get("KUBERNETES_NAMESPACE")
DOCKER_IMAGE = "https://hub.docker.com/r/hyruleexplorer/ai_rag_chatbot:latest"
CONFLUENCE_API_KEY = Variable.get("CONFLUENCE_API_KEY")
table_schema = "public"
table_name = "tb_ai_rag_chatbot_doc"

pod_env_vars = [
    k8s.V1EnvVar(name="CONFLUENCE_API_KEY", value=CONFLUENCE_API_KEY),
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

with DAG(
    "dag_ai_rag_chatbot_confluence_scraping",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2022, 10, 13),
    # At 05:00 on day-of-month 1, every month.
    schedule_interval="0 0 1 * *",
    max_active_runs=1,
    template_searchpath="/efs/git-sync/git/repo",
) as dag:

    run_scraping = KubernetesPodOperator(
        task_id="run_scraping",
        name="run_scraping",
        namespace=KUBERNETES_NAMESPACE,
        image=DOCKER_IMAGE,
        cmds=[
            "bash",
            "-c",
            f"python confluence_scraper.py '{table_schema}' '{table_name}'",
        ],
        env_vars=pod_env_vars,
        is_delete_operator_pod=True,
        startup_timeout_seconds=240,
    )

# Finally here you define the flow of the DAG tasks.
# The immediate parent of each task is the task that preceeds it.
run_scraping
