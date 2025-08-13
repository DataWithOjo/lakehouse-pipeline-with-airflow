FROM quay.io/astronomer/astro-runtime:12.10.0

USER root

RUN apt-get update && apt-get install -y wget curl aria2 \
 && curl -# https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/bin/mc \
 && chmod +x /usr/bin/mc \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Install required Python packages for parquet read & postgres load
RUN pip install --no-cache-dir \
    duckdb --upgrade \
    pandas \
    pyarrow \
    s3fs \
    psycopg2-binary

USER astro

