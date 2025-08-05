FROM apache/airflow:2.9.1-python3.10

# Install additional Python packages
RUN pip install --no-cache-dir \
    pandas \
    requests \
    snowflake-connector-python
