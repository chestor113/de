from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import sha2, current_timestamp, lit, concat_ws
from pyspark.sql.functions import to_timestamp

def main():
    spark = SparkSession.builder \
    .appName("test_job") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.215:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    init_df = spark.read.parquet("s3a://datalake/silver/users/")

    df_hub = init_df.select(
        sha2(col("uuid"), 256).alias("user_hk"),
        col("uuid").alias("user_bk")
    )

    df_hub = df_hub.withColumn(
        "load_dts", current_timestamp()
    ).withColumn(
        "recourd_source", lit("randomuser.api")
    )

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
    )

    df_sat = df_sat.withColumn(
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


    df_hub.write.mode("overwrite").parquet(
        "s3a://datalake/dv/hub_user/"
    )

    df_sat.write.mode("overwrite").parquet(
        "s3a://datalake/dv/sat_user_profile/"
    )

    hub_check = spark.read.parquet("s3a://datalake/dv/hub_user/")
    sat_check = spark.read.parquet("s3a://datalake/dv/sat_user_profile/")

    hub_check.show(3, truncate=False)
    sat_check.show(3, truncate=False)


if __name__ == "__main__":
    main()