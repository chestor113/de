from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
}



with DAG(
    dag_id="user_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    parse_to_silver = BashOperator(
        task_id="parse_to_silver",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/parse_to_silver.py
        """
    )

    silver_to_dv = BashOperator(
        task_id="silver_to_dv",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/silver_to_dv.py
        """
    )

    silver_to_mart = BashOperator(
        task_id="silver_to_mart",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/silver_to_mart.py
        """
    )

    parse_to_silver >> silver_to_dv >> silver_to_mart