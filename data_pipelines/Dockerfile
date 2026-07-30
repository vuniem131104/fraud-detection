ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt