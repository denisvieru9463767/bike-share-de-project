FROM apache/superset:latest

USER root

RUN python -m ensurepip --upgrade

# Install ClickHouse driver (compatible with Superset's SQLAlchemy 1.x)
RUN python -m pip install --no-cache-dir \
    "clickhouse-connect>=0.6.0,<0.8.0" \
    psycopg2-binary

USER superset