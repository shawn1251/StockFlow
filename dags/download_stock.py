#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import yfinance as yf
from airflow.models import Variable
import psycopg2
from datetime import datetime
from airflow.hooks.base import BaseHook
import csv
import os
import zipfile

# [END import_module]
DOWNLOAD_PATH = "./download"
POSTGRES_CONN_ID = "stockDB"

def download_data(**context):
    print("+++++ start ++++++")
    # stock index
    ticker_list = Variable.get("stock_list", default_var= ["^GSPC", "^IXIC", "^DJI"], deserialize_json=True)
    
    print(f"ticker_list: {ticker_list}")
    
    trigger_params = context.get("dag_run")
    if trigger_params:
        period = trigger_params.conf.get("period", "3d")
        interval = trigger_params.conf.get("interval", "1d")

    ticker_id_with_data = []
    for ticker_id in ticker_list:
        print(f"+++++ {ticker_id} begin +++++")
        ticker = yf.Ticker(ticker_id)
        hist = ticker.history(period=period, interval=interval)
        
        if hist.shape[0] > 0:
            ticker_id_with_data.append(ticker_id)
        else:
            print(f"{ticker_id} is empty")
            continue

        print(type(hist))
        print(hist.shape)
        print(hist)

        with open(f"{DOWNLOAD_PATH}/{ticker_id}.csv", "w") as f:
            hist.to_csv(f)
        print(f"++++ {ticker_id} end ++++++")

    print("++++ finish ++++++")
    return ticker_id_with_data

def _load_data(ticker_id):
    data_to_insert = []
    with open(f"{DOWNLOAD_PATH}/{ticker_id}.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data_to_insert.append((
                ticker_id,
                row['Date'],
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                float(row['Volume']),
                float(row['Dividends']),
                float(row['Stock Splits'])
            ))
    return data_to_insert
        

def import_data(**context):
    ticker_id_list = context["ti"].xcom_pull(task_ids="download_data")
    print(f"non-empty ticker id: {ticker_id_list}")

    # conn = psycopg2.connect(database="stockdb", user="airflow", 
    #     password="airflow", host="localhost", 
    #     port="5432")
    
    conn_info = BaseHook.get_connection(POSTGRES_CONN_ID)
    conn = psycopg2.connect(database=conn_info.schema, user=conn_info.login, 
        password=conn_info.password, host=conn_info.host, 
        port=conn_info.port)
    
    cursor = conn.cursor()
    
    for ticker_id in ticker_id_list:
        data = _load_data(ticker_id)
        sql = """
        INSERT INTO stock_price_stage (ticker, dt, open, high, low, close, volume, dividends, stock_splits) 
        VALUES (%s, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, dt) DO NOTHING
        """
        cursor.executemany(sql, data)
        conn.commit()
    conn.close()


def archive_file():
    time = datetime.utcnow()
    dir_path = os.path.join(DOWNLOAD_PATH, "archive")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    with zipfile.ZipFile(os.path.join(dir_path, f"{time.strftime('%Y-%m-%dT%H%M')}.zip"), "w") as f:
        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    f.write(file_path, os.path.relpath(file_path, dir_path))
    
    print("++++ archive finish ++++")

# download_data()
# [START instantiate_dag]
with DAG(
    "download_stock",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="A simple stock download task",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["stock"],
) as dag:
    # [END instantiate_dag]
    download_task = PythonOperator(
        task_id = "download_data",
        python_callable = download_data,
    )

    import_task = PythonOperator(
        task_id = "import_data",
        python_callable = import_data,
    )

    archive_task = PythonOperator(
        task_id = "archive_file",
        python_callable = archive_file,
    )

    merge_sql_task = PostgresOperator(
        task_id="merge_sql",
        postgres_conn_id= POSTGRES_CONN_ID,
        sql="merge_sql.sql"
    )

    download_task >> import_task >> merge_sql_task >> archive_task
# [END tutorial]