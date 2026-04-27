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

    init_df = spark.read.parquet("s3a://datalake/silver/users/")
    
    result = init_df \
    .filter(col("country") == "Ukraine") \
    .groupBy("first_name") \
    .agg(count("*").alias("name_count")) \
    .orderBy(col("name_count").desc())


    # result.show(100, truncate=False)

    init_df.show(1, truncate=False)

if __name__ == "__main__":
    main()