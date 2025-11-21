FROM apache/airflow:3.1.2

USER airflow
RUN pip install --no-cache-dir dbt-core==1.7.4 dbt-snowflake==1.7.4
