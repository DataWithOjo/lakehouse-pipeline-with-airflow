FROM openjdk:8-jdk-slim

# ===== Versions =====
ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3.3.4 \
    ICEBERG_VERSION=1.5.2 \
    AWS_SDK_VERSION=1.12.262 \
    PATH="/opt/spark/bin:$PATH"

# ===== Install system deps =====
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl wget bash unzip procps gnupg && \
    ln -sf python3 /usr/bin/python && \
    ln -sf pip3 /usr/bin/pip && \
    pip install --upgrade pip && \
    pip install pyspark==${SPARK_VERSION} && \
    rm -rf /var/lib/apt/lists/*

# ===== Install Apache Spark =====
RUN mkdir -p /opt && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# ===== Add Iceberg runtime for Spark 3.5 =====
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime-${ICEBERG_VERSION}.jar

# ===== Add Hadoop AWS + AWS SDK for S3 access =====
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# ===== Copy Spark job scripts =====
COPY convert_raw_to_bronze.py /app/convert_raw_to_bronze.py
COPY calculate_tweet_metrics.py /app/calculate_tweet_metrics.py
WORKDIR /app

# ===== Default entrypoint (can be overridden by Airflow DockerOperator) =====
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
