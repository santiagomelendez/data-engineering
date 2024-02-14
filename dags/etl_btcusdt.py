import os
import pandas as pd
from config.config import get_config
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.notifications import send_email_from_gmail
from etl.extraction import extract_data
from etl.transformation import transform_data
from etl.validations import validate_duplicates, filter_by_threshold
from etl.upload import upload_data

dags_args = {
    'owner': 'Santiago Melendez',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

config = get_config()
btc_conf = config['BTC']
ticker = btc_conf['ticker']
threshold = btc_conf['price_threshold']


def extract(**kwargs):
    output_path = f'{ticker}_raw.csv'
    print(f'Extracting data from Binance for ticker {ticker}')
    extract_data(ticker=ticker.upper(), filepath=output_path, 
                 from_date=kwargs['data_interval_start'], 
                 to_date=kwargs['data_interval_end'],
                 extraction_date=kwargs['execution_date'])
    return output_path


def transform(ti):
    input_file = ti.xcom_pull(task_ids='extraction')
    outut_file = f'{ticker}_transformed.csv'
    print('TRANSFORM TASK: Processing raw data')
    transform_data(input_file=input_file, symbol=ticker, outut_file=outut_file)
    print('TRANSFORM TASK: Transformation was successful')
    return outut_file

def validate(ti):
    input_file = ti.xcom_pull(task_ids='transformation')
    output_file = f'{ticker}_upload.csv'
    validate_duplicates(input_file=input_file, output_file=output_file)
    return output_file
    

def load(ti):
    input_file = ti.xcom_pull(task_ids='validations')
    upload_data(input_file=input_file)
    print('LOAD TASK: Loading was successful')


def send_email(ti):
    input_file = ti.xcom_pull(task_ids='extraction')
    price_threshold = float(threshold)
    output = filter_by_threshold(input_file=input_file, threshold=price_threshold)
    print(f"NOTIFICATIONS: FOUND {len(output)} WHERE PRICE THRESHOLD HAS BEEN REACHED")
    if not output.empty:
        send_email_from_gmail(body=f'The {ticker} ticker reached a price of $ {price_threshold} at the following times: \n {pd.to_datetime(output["open_time"], unit="ms")}', 
                              subject=f'PRICE THRESHOLD REACHED!!')
        

with DAG(dag_id='etl_btc',
        description='Dag for btcusdt etl from binance',
        schedule_interval='@daily',
        catchup=False,
        default_args=dags_args,
        tags=['data']) as dag:
    extract = PythonOperator(task_id='extraction', python_callable=extract, dag=dag, provide_context=True)
    transform = PythonOperator(task_id='transformation', python_callable=transform, dag=dag, provide_context=True)
    validate = PythonOperator(task_id='validations', python_callable=validate, dag=dag, provide_context=True)
    load = PythonOperator(task_id='load', python_callable=load, dag=dag, provide_context=True)
    send_email = PythonOperator(task_id='send_email', python_callable=send_email, dag=dag, provide_context=True)


    extract >> transform >> validate >> load
    extract >> send_email