from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
import re


def discover_partitions(data: str):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://192.168.0.215:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="adminadmin",
    )

    bucket = "datalake"

    paths = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=data):
        for obj in page.get("Contents", []):
            paths.append(obj["Key"])

    pattern = re.compile(
        r"year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)/"
    )   

    partitions = set()

    for path in paths:
        match = pattern.search(path)
        if match:
            year, month, day, hour = match.groups()
            partition = (
                int(year),
                int(month),
                int(day),
                int(hour)
            )

            partitions.add(partition)
    return partitions

@task
def get_diff():
    raw = discover_partitions("raw/topic1/")
    silver = discover_partitions("silver/users/")

    diff = raw - silver

    result = []
    for y, m, d, h in diff:
        result.append({
            "year": y,
            "month": m,
            "day": d,
            "hour": h
        })

    return result


# --- DAG ---
@dag(
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False
)
def raw_to_silver_diff():

    partitions = get_diff()

    BashOperator.partial(
        task_id="run_spark"
    ).expand(
        bash_command=[
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
    )


dag = raw_to_silver_diff()