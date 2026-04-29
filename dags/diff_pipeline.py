from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

from parse_part import discover_partitions


def run_diff():
    raw = discover_partitions("raw/topic1/")
    silver = discover_partitions("silver/users/")

    diff = raw - silver

    print("RAW:", raw)
    print("SILVER:", silver)
    print("DIFF:", diff)

    for y, m, d, h in diff:
        cmd = f"""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/parse_to_silver.py \
        --year {y} --month {m:02d} --day {d:02d} --hour {h:02d}
        """

        print("RUN:", cmd)

        result = subprocess.run(
            cmd,
            shell=True,
            text=True,
            capture_output=True
        )

        print("STDOUT:")
        print(result.stdout)

        print("STDERR:")
        print(result.stderr)

        result.check_returncode()


with DAG(
    dag_id="diff_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_diff_task = PythonOperator(
        task_id="run_diff",
        python_callable=run_diff,
    )