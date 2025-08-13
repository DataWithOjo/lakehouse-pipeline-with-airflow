# Lakehouse Pipeline with Airflow

## 📌 Overview

This project implements a **Lakehouse data pipeline** using:

- **Apache Airflow** orchestrated with the **Astro CLI**
- **Docker** for containerization
- **Apache Spark** for large-scale data processing

The pipeline processes **raw tweet data** from a TSV file into a **bronze layer** in **MinIO** (S3-compatible storage), prepares **metrics**, and stores the results for analytics.

The project follows **industry best practices**, with a focus on:
- **Modularity**
- **Testability**
- **Production readiness**

---

## 🏗 Architecture

### Key Components

- **Astro CLI** – Sets up and manages the Airflow environment locally.
- **Docker & Docker Compose** – Provides a containerized environment for Airflow, Spark jobs, and MinIO.
- **Apache Airflow** – Handles orchestration and scheduling of ETL workflows.
- **Apache Spark** – Data processing engine for transforming large datasets.
- **MinIO** – S3-compatible object storage for the Lakehouse layers.
- **Aria2c** – Enables fast, reliable, multi-connection downloading for large datasets.
- **PostgreSQL** – Metadata store for Airflow and optionally for processed outputs.

---

## 📂 Project Structure

lakehouse-pipeline-with-airflow/
-│
-├── dags/                   # Airflow DAG definitions
-│   └── tweet_pipeline_dag.py
-│
-├── include/                # Task scripts imported into DAGs
-│   └── tasks/
-│       └── tweet_pipeline_tasks.py
-│   └── helpers/
-│       └── minio.py
-│
-├── spark/                  # Spark job code & Dockerfile
-│   └── notebooks/tweet_transform
-│       └── convert_raw_to_bronze.py
-│       └── calculate_tweet_metrics.py
-│   └── Dockerfile
-│
-├── requirements.txt        # Python dependencies for Airflow
-├── docker-compose.override.yml
-├── Dockerfile
-├── README.md
-└── ...

---

## ⚙️ Workflow Design

The pipeline is split into multiple **Airflow tasks**, each implemented as a plain Python callable in the `include/` folder.  
These callables are imported into DAGs in `dags/` to keep DAG definitions clean and modular.

### Key Tasks

#### 1. Download Raw Dataset
- Uses **Aria2c** for fast, resumable downloads of large files.
- Streams or saves directly to **MinIO**.

#### 2. Run Spark Job in Docker
- Each Spark job is built separately into a **Docker image**.
- **Airflow’s `DockerOperator`** runs the job container, reading/writing to MinIO via `s3a://` paths.

#### 3. Load Metrics
- Reads transformed data from the **bronze layer**.
- Produces **tweets per hour/day** metrics.
- Stores results as **Iceberg/Parquet** files or loads into **PostgreSQL** for analytics.

---

## 🧪 Testing Strategy

- **Task-by-task testing**: Each task in `include/` was executed independently before integrating into the main DAG.
- **Local end-to-end testing**: The full DAG was run locally to ensure all services could communicate and connections worked.
- **Mocked downloads**: Small sample files were used to validate Aria2c streaming logic before downloading the full dataset.

---

## 🔌 Inter-service Communication

- **Airflow ↔ MinIO**: Connected via `minio.py` utility using Airflow connection ID `minio`.
- **Spark ↔ MinIO**: Uses Hadoop **S3A connector** for read/write operations.
- **DockerOperator ↔ Spark**: Runs containerized Spark job images that already have necessary connectors installed.
- **Aria2c ↔ MinIO**: Streams large files directly into object storage.

---

## 📊 Metrics & Analytics

The final outputs are:

- **Number of tweets per hour**
- **Number of tweets per day**

These metrics can be visualized in **Metabase** or any BI tool pointing to the **gold layer** or **PostgreSQL** table.

---

## 🛠 Technologies Used

- **Apache Airflow (Astro CLI)**
- **Docker & Docker Compose**
- **Apache Spark**
- **MinIO**
- **Aria2c**
- **PostgreSQL**
- **Python**
- **Iceberg / Parquet** formats
- **Metabase**
