from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_airflow_basic",
    description="Basic DAG to verify Airflow is running",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@once",   # chạy đúng 1 lần
    catchup=False,
    tags=["test", "sanity"],
) as dag:

    start = BashOperator(
        task_id="print_start",
        bash_command="echo 'Airflow DAG started'",
    )

    check_env = BashOperator(
        task_id="check_env",
        bash_command="""
        echo "Hostname: $(hostname)"
        echo "Date: $(date)"
        echo "User: $(whoami)"
        """,
    )

    end = BashOperator(
        task_id="print_end",
        bash_command="echo 'Airflow DAG finished successfully'",
    )

    start >> check_env >> end
