from airflow.decorators import dag
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime



@dag(
    dag_id="silver_to_dv",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
    run_slvr_dv = BashOperator(
        task_id="slvr_to_dv",
        bash_command=
        """
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/silver_to_dv.py \
        """
    )
    

my_dag()