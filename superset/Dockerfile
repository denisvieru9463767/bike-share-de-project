FROM apache/superset:latest

USER root


RUN python -m ensurepip --upgrade


RUN python -m pip install --no-cache-dir \
    snowflake-sqlalchemy \
    snowflake-connector-python \
    psycopg2-binary

USER superset