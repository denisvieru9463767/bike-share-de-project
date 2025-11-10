# Start from the same Airflow image your compose file uses
FROM apache/airflow:3.1.2

# Install your packages
# This runs *once* when we build the image
RUN pip install --user \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    requests \
    numpy \
    apache-airflow-providers-snowflake