from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from parse_part import discover_partitions


@dag(
    dag_id="diff_pipeline_dynamic",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
)
def my_dag():

    @task
    def get_partitions():
        raw = discover_partitions("raw/topic1/")
        silver = discover_partitions("silver/users/")

        diff = raw - silver

        print("RAW:", raw)
        print("SILVER:", silver)
        print("DIFF:", diff)

        return [
            {
                "year": y,
                "month": f"{m:02d}",
                "day": f"{d:02d}",
                "hour": f"{h:02d}",
            }
            for y, m, d, h in sorted(diff)
        ]

    @task
    def build_commands(partitions):
        return [
            f"""
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/airflow/jobs/parse_to_silver.py \
            --year {p['year']} \
            --month {p['month']} \
            --day {p['day']} \
            --hour {p['hour']}
            """
            for p in partitions
        ]

    partitions = get_partitions()
    commands = build_commands(partitions)

    parse_tasks = BashOperator.partial(
        task_id="parse_to_silver"
    ).expand(
        bash_command=commands
    )


my_dag()










