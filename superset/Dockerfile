FROM apache/superset:latest

USER root

# 1. Re-install pip (it is missing in the 'lean' image)
RUN python -m ensurepip --upgrade

# 2. Now you can install the drivers using the restored pip
RUN python -m pip install --no-cache-dir \
    snowflake-sqlalchemy \
    snowflake-connector-python \
    psycopg2-binary

USER superset