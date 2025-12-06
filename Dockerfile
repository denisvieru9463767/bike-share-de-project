FROM apache/airflow:3.1.2

USER root
RUN apt-get update && \
    apt-get install -y git build-essential && \
    apt-get clean

USER airflow


RUN PIP_USER=false python -m venv /opt/airflow/dbt_venv && \
    PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
    setuptools \
    dbt-core==1.7.10 \
    dbt-clickhouse==1.7.7


RUN pip install --no-cache-dir \
    clickhouse-connect