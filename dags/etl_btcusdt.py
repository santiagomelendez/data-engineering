import os
import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extraction import extract_data, fetch_klines

dags_args = {
    'owner': 'Santiago Melendez',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ticker = 'btcusdt'

def extract():
    output_path = f'{ticker}_raw.csv'
    print(f'Extracting data from Binance for ticker {ticker}')
    extract_data(ticker=ticker.upper(), filepath=output_path)
    return output_path


def transform(ti):
    input_file = ti.xcom_pull(task_ids='extraction')
    print('INPUT_FILE: ', {input_file})
    df = pd.read_csv(input_file)
    print("DATAFRAME_SIZE: ", df.size)

def load():
    print('Load data to database')

with DAG(dag_id='etl_btc',
        description='Dag for btcusdt etl from binance',
        schedule_interval='*/10 * * * *',
        catchup=False,
        default_args=dags_args,
        tags=['data']) as dag:
    extract = PythonOperator(task_id='extraction', python_callable=extract, dag=dag, provide_context=True)
    transform = PythonOperator(task_id='transform_data', python_callable=transform, dag=dag, provide_context=True)
    load = PythonOperator(task_id='load_data', python_callable=load, dag=dag, provide_context=True)

    extract >> transform >> load
