import subprocess
import logging
import os
from airflow.models import Variable
import pandas as pd
import s3fs
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from io import StringIO
from minio import Minio



def _run_with_live_output(command: list, label: str):
    """Run a subprocess and log its output line by line."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    logging.info(f"Running command: {' '.join(command)}")

    for line in process.stdout:
        logging.info(f"[{label}] {line.strip()}")

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)


def download_and_upload_to_minio():
    url = "https://zenodo.org/records/3723940/files/full_dataset.tsv?download=1"
    local_path = "/tmp/full_dataset.tsv"
    minio_bucket = "lakehouse"
    minio_object = "raw/full_dataset.tsv"

    access_key = Variable.get("MINIO_ACCESS_KEY")
    secret_key = Variable.get("MINIO_SECRET_KEY")

    # Download the file using aria2c for faster parallel download
    _run_with_live_output(
    [
        "aria2c",
        "-x", "16",  # max connections per server
        "-s", "16",  # split downloads
        "--dir=/tmp",  # directory
        "-o", "full_dataset.tsv",  # filename
        url
    ],
    label="aria2c",
    )

    # Verify the file exists
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Downloaded file not found: {local_path}")

    # Configure mc client with access key and secret
    _run_with_live_output(
        ["mc", "alias", "set", "localminio", "http://minio:9000", access_key, secret_key],
        label="mc-alias"
    )

    # Upload the file to MinIO
    _run_with_live_output(
    ["mc", "cp", "/tmp/full_dataset.tsv", f"localminio/{minio_bucket}/{minio_object}"],
    label="mc-cp",
    )

    logging.info(f"Download and upload completed successfully: local file -> localminio/{minio_bucket}/{minio_object}")
    
def load_gold_to_postgres():
    logging.info("Starting load_gold_to_postgres task...")

    # Get MinIO connection from Airflow
    minio_conn = BaseHook.get_connection('minio')
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    if not minio_endpoint:
        raise ValueError("MinIO 'endpoint_url' not found in connection extras")
    # Extract host without protocol, e.g. from http://minio:9000 get minio:9000
    minio_host = minio_endpoint.split('//')[1]

    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password

    logging.info(f"Using MinIO endpoint: {minio_host}")

    # Setup s3fs filesystem to read parquet from MinIO
    fs = s3fs.S3FileSystem(
        key=minio_access_key,
        secret=minio_secret_key,
        client_kwargs={"endpoint_url": minio_endpoint, "use_ssl": False}
    )

    # Directory path in MinIO where gold parquet files are stored
    parquet_dir = "lakehouse/gold/tweet_metrics"

    logging.info(f"Listing parquet files in s3://{parquet_dir}/")
    files = [f"s3://{file_path}" for file_path in fs.ls(parquet_dir) if file_path.endswith(".parquet")]

    if not files:
        raise FileNotFoundError(f"No parquet files found in s3://{parquet_dir}/")

    logging.info(f"Found parquet files: {files}")

    # Read all parquet files into a single DataFrame
    df = pd.read_parquet(files, filesystem=fs)

    logging.info(f"Read {len(df)} rows from parquet files")

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create 'gold' schema if it does not exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    conn.commit()
    logging.info("Ensured 'gold' schema exists")
    
    # Create table if not exists - adjust columns and types as per your parquet data
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS gold.tweet_metrics (
        period_hour TIMESTAMP,
        tweets_per_hour BIGINT,
        tweets_per_day BIGINT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    logging.info("Ensured table 'gold.tweet_metrics' exists")

    # truncate table before load
    cursor.execute("TRUNCATE TABLE gold.tweet_metrics;")
    conn.commit()

    # Prepare CSV data in memory for bulk load
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    logging.info("Loading data into Postgres table gold.tweet_metrics...")
    cursor.copy_expert("COPY gold.tweet_metrics FROM STDIN WITH CSV", csv_buffer)
    conn.commit()

    cursor.close()
    conn.close()

    logging.info("Data load to Postgres completed successfully.")