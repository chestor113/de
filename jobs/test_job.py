from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType



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

    df = spark.read.parquet("s3a://datalake/raw/topic1/")

    schema = StructType([
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True)
    ])
 
    df2 = df.withColumn(
    "payload_json",
    from_json(col("payload"), schema)
    )

    df2.select("payload_json").show(5, truncate=False)
    df2.printSchema()

if __name__ == "__main__":
    main()