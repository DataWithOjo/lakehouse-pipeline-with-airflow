from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from datetime import datetime
from include.tasks.tweet_pipeline_tasks import download_and_upload_to_minio, load_gold_to_postgres

default_args = {
    "owner": "Oluwakayode",
    "retries": 1,
}

with DAG(
    dag_id="tweet_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "minio"],
    description="ETL pipeline using Iceberg: RAW → BRONZE → METRICS",
) as dag:

    download_task = PythonOperator(
        task_id="download_tsv_to_minio",
        python_callable=download_and_upload_to_minio,
    )

    convert_to_bronze = DockerOperator(
        task_id="convert_raw_to_bronze",
        image="spark-jobs:latest",
        container_name="convert_to_bronze",
        api_version="auto",
        auto_remove="success",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        mount_tmp_dir=False,
        command=[
            "/app/convert_raw_to_bronze.py",
            "lakehouse/raw/full_dataset.tsv",
            "s3a://lakehouse/bronze/full_dataset",
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("MINIO_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_SECRET_KEY"),
            "S3_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
        },
    )

    calculate_metrics = DockerOperator(
        task_id="calculate_tweet_metrics",
        image="spark-jobs:latest",
        container_name="calculate_metrics",
        api_version="auto",
        auto_remove="success",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        mount_tmp_dir=False,
        command=[
            "/app/calculate_tweet_metrics.py"
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("MINIO_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_SECRET_KEY"),
            "S3_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
        },
    )
    
    load_gold_to_dw = PythonOperator(
        task_id="load_gold_to_dw",
        python_callable=load_gold_to_postgres,
    )

    download_task >> convert_to_bronze >> calculate_metrics >> load_gold_to_dw
