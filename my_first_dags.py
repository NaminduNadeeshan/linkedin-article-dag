# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import sqlite3
import pandas as pd
import os
import boto3

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Namindu Sanchila',
    'start_date': days_ago(0),
    'email': ['namindu@namindu.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'weather_signals',
    default_args=default_args,
    description='Weather Dags from the sensors',
    schedule_interval=timedelta(minutes=30),
)

def store_temperaly_in_Sql_light(data):
    conn = sqlite3.connect("/tmp/sqlite_default.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS weather_records (
                    sensor_id INTEGER NOT NULL,
                    tempreture INTEGER
                );
             ''')
    records.to_sql('weather_records', conn, if_exists='replace', index=False)

def convertor(data):
    return (data - 32) * 5.0/9.0

def convert_F_to_C(data):
    clearned = data['tempreture'].apply(convertor)

def convertedToPdFrame(data):
    columns = data[0].keys()
    pf = pd.DataFrame([i for i in data], columns=columns)
    return pf


#data extract function
def _get_data_from_database():
    data_from_db = [{'sensor_id': 1, 'tempreture in F': 24 }, {'sensor_id': 2, 'tempreture in F': 45 }]
    value_df = pd.DataFrame([i for i in data_from_db], columns=columns)
    value_transformed = convert_F_to_C(value_df)
    #temperaly store data in default sql light databae
    store_temperaly_in_Sql_light(value_transformed)



def load_data():
    conn = sqlite3.connect("/tmp/sqlite_default.db")
    db_df = pd.read_sql_query("SELECT * FROM error_log", conn)
    #im not correctly connected s3 bucket here
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(db_df.to_csv('/database.csv', index=False), 'waether_bucket', 'database.csv')


# define the first task - extract data from the weather database.
extract_transform = PythonOperator(
    task_id='extract_data_trasform',
    python_callable=_get_data_from_database,
    dag=dag
)

#load_data_to_csv_file in s3 bucket
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# task pipeline
extract_transform >> load_data