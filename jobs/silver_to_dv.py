from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, current_timestamp, lit, concat_ws


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
        "user_hk",
        "load_dts",
        "hashdiff",
        "gender",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "registered_date",
        "dob_date"
    )

    # SAT пока оставляем full refresh
    df_sat.write.mode("overwrite").parquet(sat_path)

    hub_check = spark.read.parquet(hub_path)
    sat_check = spark.read.parquet(sat_path)

    hub_check.show(3, truncate=False)
    sat_check.show(3, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()