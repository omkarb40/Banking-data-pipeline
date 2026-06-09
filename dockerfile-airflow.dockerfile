FROM apache/airflow:2.8.0

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.3.0 \
    apache-airflow-providers-amazon==8.13.0 \
    boto3==1.34.0 \
    snowflake-connector-python==3.6.0
