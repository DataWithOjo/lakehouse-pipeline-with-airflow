import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp, date_format, count

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hardcoded paths
BRONZE_PATH = "s3a://lakehouse/bronze/full_dataset"
GOLD_PATH = "s3a://lakehouse/gold/tweet_metrics"

logger.info("Starting Gold transformation job...")

# Spark session
spark = (
    SparkSession.builder.appName("CalculateTweetMetrics")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

logger.info(f"Reading Bronze dataset from {BRONZE_PATH}...")
df = spark.read.parquet(BRONZE_PATH)

if "ingest_date" in df.columns:
    df = df.drop("ingest_date")
    logger.info("Dropped column: ingest_date")

logger.info("Creating timestamp column from date and time...")
df = df.withColumn("timestamp", to_timestamp(concat_ws(" ", col("date"), col("time"))))

logger.info("Calculating tweets per hour...")
tweets_per_hour = df.groupBy(
    date_format(col("timestamp"), "yyyy-MM-dd HH:00:00").alias("period_hour")
).agg(count("*").alias("tweets_per_hour"))

logger.info("Calculating tweets per day...")
tweets_per_day = df.groupBy(
    date_format(col("timestamp"), "yyyy-MM-dd").alias("period_day")
).agg(count("*").alias("tweets_per_day"))

logger.info("Joining metrics...")
metrics_df = tweets_per_hour.join(
    tweets_per_day,
    tweets_per_hour.period_hour.substr(1, 10) == tweets_per_day.period_day,
    "left",
).drop("period_day")

logger.info(f"Writing Gold metrics to {GOLD_PATH}...")
metrics_df.write.mode("overwrite").parquet(GOLD_PATH)

logger.info("Gold transformation job completed successfully.")
spark.stop()
