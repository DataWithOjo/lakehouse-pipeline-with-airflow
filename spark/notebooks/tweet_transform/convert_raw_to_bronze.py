import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

RAW_PATH = "s3a://lakehouse/raw/full_dataset.tsv"
BRONZE_PATH = "s3a://lakehouse/bronze/full_dataset"

spark = (
    SparkSession.builder.appName("ConvertRawToBronzeIceberg")
    .config(
        "spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")
    )
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Use Hadoop catalog without warehouse, direct path writes
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse")
    .getOrCreate()
)

df = (
    spark.read.option("header", True)
    .option("sep", "\t")
    .csv(RAW_PATH)
    .withColumn("ingest_date", current_timestamp())
)

(
    df.write.format("parquet")
    .mode("overwrite")
    .option("path", BRONZE_PATH)
    .save()
)

print(f"Wrote Bronze Iceberg table at path: {BRONZE_PATH}")
spark.stop()
