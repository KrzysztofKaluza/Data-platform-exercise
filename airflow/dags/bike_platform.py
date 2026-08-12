from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="bike_platform",
    start_date=(datetime(2026, 8, 1, tzinfo=ZoneInfo("Europe/Warsaw"))),
    schedule=None,
    catchup=False,
    tags=["data_platform"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo START")

    docker_test = BashOperator(task_id="docker_test", bash_command="docker ps")

    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/bronze.py"
        ),
    )

    silver = BashOperator(
        task_id="silver_layer",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/silver.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec dbt bash -c 'cd /app/bike_rental && dbt run'",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt bash -c 'cd /app/bike_rental && dbt test'",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="docker exec dbt bash -c 'cd /app/bike_rental && dbt docs generate'",
    )

    end = BashOperator(task_id="end", bash_command="echo END")

    (
        start
        >> docker_test
        >> bronze
        >> silver
        >> dbt_run
        >> dbt_test
        >> dbt_docs_generate
        >> end
    )
