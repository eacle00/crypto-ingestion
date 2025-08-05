from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import snowflake.connector
from airflow.hooks.base import BaseHook

def fetch_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'bitcoin',
        'vs_currencies': 'usd'
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data['bitcoin']['usd']

def load_to_snowflake(**kwargs):
    btc_price = kwargs['ti'].xcom_pull(task_ids='fetch_btc_price')
    ts = datetime.utcnow()

    conn = BaseHook.get_connection("snowflake_conn")

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.schema.split('.')[0],
        schema=conn.schema.split('.')[1],
        role=conn.extra_dejson.get("role")
    )

    cs = ctx.cursor()
    try:
        insert_stmt = f"INSERT INTO btc_prices (timestamp, price_usd) VALUES (%s, %s)"
        cs.execute(insert_stmt, (ts, btc_price))
    finally:
        cs.close()
        ctx.close()

# Define DAG
with DAG(
    dag_id='btc_price_to_snowflake_operator',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['btc', 'coingecko', 'snowflake'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_btc_price',
        python_callable=fetch_btc_price
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    fetch_task >> load_task
