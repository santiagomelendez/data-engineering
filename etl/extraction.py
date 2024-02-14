import requests
import json
import os
import pandas as pd
import pendulum
from json import JSONDecodeError
from utils.pandas_helpers import save_data

api_key = os.environ.get('BINANCE_API_KEY', '')
base_url = os.environ.get('BINANCE_URL', '')
fields = ['open_time', 'open_price', 'high_price', 
                    'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 
                    'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'unused_field', 'extraction_date', 'extraction_params']
    

interval = '1h'
limit = 24

def get_ms_timestamp(dt: pendulum):
    if not dt:
        return None
    epoch = pendulum.datetime(1970, 1, 1)
    timestamp = int((dt - epoch).total_seconds() * 1000)
    return timestamp


# Make the request
def fetch_klines(symbol, interval, limit=None, from_date=None, to_date=None):
    '''
    symbol	STRING	YES	
    interval	ENUM	YES	
    startTime	LONG	NO	
    endTime	LONG	NO	
    timeZone	STRING	NO	Default: 0 (UTC)
    limit	INT	NO
    '''
    params = {
        'symbol': symbol,
        'interval': interval,
    }
    optional_params = {
        'limit': limit,
        'startTime': get_ms_timestamp(from_date),
        'endTime': get_ms_timestamp(to_date)
    }
    
    for k,v in optional_params.items():
        if v:
            params[k] = v
    print(f'Fetching klines with params: {params}')

    klines_url = f'{base_url}/klines'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    response = requests.get(klines_url, params=params, headers=headers)
    try:
        data = response.json()
    except JSONDecodeError:
        print("Response is different as expected: ", response.text)
    return data, params


def extract_data(ticker, filepath, from_date, to_date, extraction_date):
    raw_data, params = fetch_klines(symbol=ticker, interval=interval, limit=limit, from_date=from_date, to_date=to_date)
    data = [d + [extraction_date] + [params] for d in raw_data]
    df = save_data(data=data, columns=fields, filepath=filepath)
    print(f'<<<<< Extraction successful for {ticker}. Data was saved in {filepath} >>>>>>>')