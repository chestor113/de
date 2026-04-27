from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, count
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

    silver_df = spark.read.parquet("s3a://datalake/silver/users/")

    mart_df = silver_df.groupBy(
        "country",
        "gender"
    ).agg(
        count("*").alias("users_count")
    )

    mart_df.show(20,truncate=False)

    mart_df.write.mode("overwrite").parquet(
        "s3a://datalake/marts/users_by_country_gender/"
    )

    spark.stop()


if __name__ == "__main__":
    main()