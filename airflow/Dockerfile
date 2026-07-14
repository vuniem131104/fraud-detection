# syntax=docker/dockerfile:1

# Custom Airflow image for the fraud-detection feature pipeline.
# Extends the base slim image (keep AIRFLOW_VERSION/PYTHON_VERSION in sync with
# docker-compose.yml) with Feast (Redis online store + Postgres offline store),
# psycopg 3 for the Feast SQL registry, and psycopg 2 for Airflow's metadata DB.
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Runs as the default unprivileged `airflow` user so packages land in the
# airflow user-site (PIP_USER=true in the base image) and ownership stays correct.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
