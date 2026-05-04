from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import sha2, current_timestamp, lit, concat_ws
from pyspark.sql.functions import to_timestamp
import argparse

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--hour", required=True)

    args = parser.parse_args()

    month = int(args.month)
    day = int(args.day)
    hour = int(args.hour)
    year = int(args.year)

    input_path = (
        f"s3a://datalake/raw/topic1/"
        f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    )
            
      
    print(f"Processing: {input_path}")

    spark = SparkSession.builder \
    .appName("parse_to_silver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.215:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    init_df = spark.read.parquet(input_path)

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
        to_timestamp(col("payload_json.dob.date")).alias("dob_date"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        lit(int(args.year)).alias("year"),
        lit(int(args.month)).alias("month"),
        lit(int(args.day)).alias("day"),
        lit(int(args.hour)).alias("hour")
    )

    silver_df = silver_df.withColumn(
        "ingest_dts",
        current_timestamp()  
    ).withColumn(
        "record_source",
        lit("randomuser.api")
    )

    silver_df = silver_df.dropDuplicates([
        "kafka_partition",
        "kafka_offset"
    ])  

    silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day", "hour") \
    .parquet("s3a://datalake/silver/users/")




if __name__ == "__main__":
    main()