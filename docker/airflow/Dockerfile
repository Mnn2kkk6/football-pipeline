FROM apache/airflow:2.9.3-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 requests psycopg2-binary
# Tải sẵn JAR cho Spark (S3A + PostgreSQL) để không phải tải từ Maven lúc chạy
RUN J=/home/airflow/.local/lib/python3.11/site-packages/pyspark/jars && \
    for u in \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar ; do \
      f=$J/$(basename $u); \
      curl -fL --retry 10 --retry-all-errors --retry-delay 3 -o $f https://maven-central.storage-download.googleapis.com/maven2/$u \
      || curl -fL --retry 10 --retry-all-errors --retry-delay 3 -o $f https://repo1.maven.org/maven2/$u ; \
    done && test "$(ls $J | grep -cE 'hadoop-aws|aws-java-sdk-bundle|postgresql')" -ge 3
# dbt chạy trong venv riêng để tránh xung đột dependency với Airflow
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.8.8" "dbt-postgres==1.8.2"
