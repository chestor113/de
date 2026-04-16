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

    init_df = spark.read.parquet("s3a://datalake/raw/topic1/year=2026/")

    schema = StructType([
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("name", StructType([
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True),
        StructField(
            "login", StructType([
                StructField("uuid", StringType(), True)
            ]), True
        ),
        StructField(
            "location", StructType([
                StructField("country", StringType(), True),
                StructField("city", StringType(), True)
            ]), True
        ),
        StructField(
            "registered", StructType([
                StructField("date", StringType(), True)
            ]), True
        ),
        StructField(
            "dob", StructType([
                StructField("date", StringType(), True)
            ]), True
        )
    ])

    parsed_df = init_df.withColumn(
        "payload_json",
        from_json(col("payload"), schema)
    )
    # parsed_df.select("payload_json").show(3, truncate=False)
    
    silver_df = parsed_df.select(
        col("payload_json.login.uuid").alias("uuid"),
        col("payload_json.name.first").alias("first_name"),
        col("payload_json.name.last").alias("last_name"),
        col("payload_json.gender").alias("gender"),
        col("payload_json.email").alias("email"),
        col("payload_json.location.country").alias("country"),
        col("payload_json.location.city").alias("city"),
        to_timestamp(col("payload_json.registered.date")).alias("registered_date"),
        to_timestamp(col("payload_json.dob.date")).alias("dob_date")
    )

    silver_df = silver_df.withColumn(
        "ingest_dts",
        current_timestamp()
    ).withColumn(
        "record_source",
        lit("randomuser.api")
    )

    silver_df.write.mode("overwrite").parquet(
        "s3a://datalake/silver/users/"
    )

    df_silver = spark.read.parquet("s3a://datalake/silver/users/")




if __name__ == "__main__":
    main()