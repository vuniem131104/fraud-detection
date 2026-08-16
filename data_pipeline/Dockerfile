ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Spark chạy NGAY TRONG container này (--master local[2]) chứ không có cụm
# Spark riêng: cả data lake ~33 MB / 100k giao dịch nên 2 core là thừa.
#
# Vì không có cụm managed nào lo hộ, image phải tự mang đủ ba thứ:
#   * JVM           — pyspark chỉ cần JRE, không cần JDK
#   * gcs-connector — job đọc/ghi thẳng gs://, Hadoop không hiểu scheme này
#   * JDBC Postgres — dp3_* ghi feat_* vào Cloud SQL
USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Jar tải lúc build, KHÔNG để spark-submit tự kéo từ Maven lúc chạy: VM đi ra
# internet qua Cloud NAT và một task fail vì mạng lúc 00:15 thì không ai thấy.
# gcs-connector phải là bản -shaded (bản thường thiếu dependency Guava/Guice).
ARG GCS_CONNECTOR=hadoop3-2.2.33
ARG POSTGRES_JDBC=42.7.4
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/gcs-connector-shaded.jar \
      "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR}/gcs-connector-${GCS_CONNECTOR}-shaded.jar" \
 && curl -fsSL -o /opt/spark-jars/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC}/postgresql-${POSTGRES_JDBC}.jar" \
 && chmod 644 /opt/spark-jars/*.jar

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
