import requests
import json
import os
import pandas as pd
from json import JSONDecodeError
from utils.pandas_helpers import save_data

api_key = os.environ.get('BINANCE_API_KEY', '')
base_url = os.environ.get('BINANCE_URL', '')
fields = ['open_time', 'open_price', 'high_price', 
                    'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 
                    'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'unused_field']
    

interval = '1h'
limit = 24



# Make the request
def fetch_klines(symbol, interval, limit):
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
        'limit': limit,
    }
    klines_url = f'{base_url}/klines'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    response = requests.get(klines_url, params=params, headers=headers)
    try:
        data = response.json()
    except JSONDecodeError:
        print("Response is different as expected: ", response.text)
    return data


def extract_data(ticker, filepath):
    raw_data = fetch_klines(symbol=ticker, interval=interval, limit=limit)
    df = save_data(data=raw_data, columns=fields, filepath=filepath)
    print(f'<<<<< Extraction successful for {ticker}. Data was saved in {filepath} >>>>>>>')