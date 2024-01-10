import requests
import json
import os
from json import JSONDecodeError
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class Kline:
    open_time: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    close_time: str
    quote_asset_volume: float
    trades_amount: int

    def format_data(self):
        self.open_time = datetime.utcfromtimestamp(self.open_time / 1000.0).isoformat()
        self.open_price = float(self.open_price)
        self.high_price = float(self.high_price)
        self.low_price = float(self.low_price)
        self.close_price = float(self.close_price)
        self.volume = float(self.volume)
        self.close_time = datetime.utcfromtimestamp(self.close_time / 1000.0).isoformat()
        self.quote_asset_volume = float(self.quote_asset_volume)
        self.trades_amount = int(self.trades_amount)
        return self



api_key = os.environ.get('BINANCE_API_KEY', '')
base_url = os.environ.get('BINANCE_URL', '')



# Parameters for the request
'''
symbol	STRING	YES	
interval	ENUM	YES	
startTime	LONG	NO	
endTime	LONG	NO	
timeZone	STRING	NO	Default: 0 (UTC)
limit	INT	NO
'''

symbols = ['BTCUSDT', 'ETHUSDT', 'DAIUSDT']
interval = '1h'
limit = 24



# Make the request
def fetch_klines(symbol, interval, limit):
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


data = {s: fetch_klines(symbol=s, interval=interval, limit=limit) for s in symbols}

dict_data = {s: {index: asdict(Kline(*kline[:-3]).format_data()) for index, kline in enumerate(d) } for s, d in data.items()}
print(json.dumps(dict_data, indent=2))