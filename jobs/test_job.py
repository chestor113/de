from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import sha2, current_timestamp, lit, concat_ws

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

    df = spark.read.parquet("s3a://datalake/raw/topic1/year=2026/")

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
        )
    ])
 
    df2 = df.withColumn(
    "payload_json",
    from_json(col("payload"), schema)
    )

    df2.select("payload_json").show(5, truncate=False)
    df2.printSchema()

    df3 = df2.select(
        col("payload_json.login.uuid").alias("uuid"),
        col("payload_json.name.first").alias("first_name"),
        col("payload_json.name.last").alias("last_name"),
        col("payload_json.gender").alias("gender"),
        col("payload_json.email").alias("email")
    )
    df_hub = df3.withColumn(
        "user_hk",
        sha2(col("uuid"), 256)
    )
    
    df3.show(5, truncate=False)
    df3.printSchema()

    df_hub = df3.select(
        col("uuid").alias("user_bk"),
        sha2(col("uuid"), 256).alias("user_hk")
    )

    df_hub = df_hub.withColumn(
        "load_dts", current_timestamp()
    ).withColumn(
        "record_source", lit("randomuser_api")
    )

    df_hub.select("*").show(5, truncate=False)


    df_sat = df3.withColumn(
        "user_hk",
        sha2(col("uuid"), 256)
    )   

    df_sat = df_sat.withColumn(
        "hashdiff",
        sha2(
            concat_ws(
                "|",
                col("gender"),
                col("first_name"),
                col("last_name"),
                col("email")
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
        "email"
    )

    df_sat.show(5, False)
    df_sat.printSchema()

if __name__ == "__main__":
    main()