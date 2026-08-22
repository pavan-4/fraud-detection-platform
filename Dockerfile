# airflow/Dockerfile
# Pre-installs Databricks provider + dbt into the Airflow image.
# Using a Dockerfile instead of _PIP_ADDITIONAL_REQUIREMENTS avoids
# dependency conflicts that occur when pip installs packages at runtime.

FROM apache/airflow:2.9.0

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-databricks==6.3.0" \
    "dbt-core==1.7.14" \
    "dbt-databricks==1.7.14"
