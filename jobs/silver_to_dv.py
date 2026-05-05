from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, current_timestamp, lit, concat_ws
from pyspark.sql.types import StringType, TimestampType


def main():
    spark = SparkSession.builder \
        .appName("silver_to_dv") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.215:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    silver_path = "s3a://datalake/silver/users/"
    hub_path = "s3a://datalake/dv/hub_user/"
    sat_path = "s3a://datalake/dv/sat_user_profile/"

    init_df = spark.read.parquet(silver_path)

    df_hub = init_df.select(
        sha2(col("uuid"), 256).alias("user_hk"),
        col("uuid").alias("user_bk")
    ).dropDuplicates(["user_hk"])

    df_hub = df_hub.withColumn(
        "load_dts", current_timestamp()
    ).withColumn(
        "record_source", lit("randomuser.api")
    )

    df_hub = df_hub.select(
        col("user_hk").cast(StringType()).alias("user_hk"),
        col("user_bk").cast(StringType()).alias("user_bk"),
        col("load_dts").cast(TimestampType()).alias("load_dts"),
        col("record_source").cast(StringType()).alias("record_source")
    )

    try:
        existing_hub = spark.read.parquet(hub_path)

        new_hub = df_hub.join(
            existing_hub.select("user_hk"),
            on="user_hk",
            how="left_anti"
        )

        print("NEW HUB ROWS:", new_hub.count())

        new_hub.write.mode("append").parquet(hub_path)

    except Exception:
        print("HUB NOT FOUND, INITIAL LOAD")
        df_hub.write.mode("overwrite").parquet(hub_path)

    df_sat = init_df.select(
        sha2(col("uuid"), 256).alias("user_hk"),
        col("gender"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("country"),
        col("city"),
        col("registered_date"),
        col("dob_date")
    )

    df_sat = df_sat.withColumn(
        "hashdiff",
        sha2(
            concat_ws(
                "|",
                col("gender"),
                col("first_name"),
                col("last_name"),
                col("email"),
                col("country"),
                col("city"),
                col("registered_date"),
                col("dob_date")
            ),
            256
        )
    ).withColumn(
        "load_dts",
        current_timestamp()
    )
    df_sat = df_sat.select(
        col("user_hk").cast(StringType()).alias("user_hk"),
        col("load_dts").cast(TimestampType()).alias("load_dts"),
        col("hashdiff").cast(StringType()).alias("hashdiff"),
        col("gender").cast(StringType()).alias("gender"),
        col("first_name").cast(StringType()).alias("first_name"),
        col("last_name").cast(StringType()).alias("last_name"),
        col("email").cast(StringType()).alias("email"),
        col("country").cast(StringType()).alias("country"),
        col("city").cast(StringType()).alias("city"),
        col("registered_date").cast(TimestampType()).alias("registered_date"),
        col("dob_date").cast(TimestampType()).alias("dob_date")
    )

    df_sat = df_sat.dropDuplicates(["user_hk", "hashdiff"])

    try:
        existing_sat = spark.read.parquet(sat_path)

        new_sat = df_sat.join(
            existing_sat.select("user_hk", "hashdiff"),
            on=["user_hk", "hashdiff"],
            how="left_anti"
        )

        print("NEW SAT ROWS:", new_sat.count())

        new_sat.write.mode("append").parquet(sat_path)

    except Exception:
        print("SAT NOT FOUND, INITIAL LOAD")
        df_sat.write.mode("overwrite").parquet(sat_path)

    hub_check = spark.read.parquet(hub_path)
    sat_check = spark.read.parquet(sat_path)

    hub_check.show(3, truncate=False)
    sat_check.show(3, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()