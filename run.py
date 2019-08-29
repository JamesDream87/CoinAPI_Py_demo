import requests
import json

import config

con = config.config
url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1HRS&time_start=2016-12-31T00:00:00&time_end=2017-01-01T00:00:00&limit=8760'
headers = {'X-CoinAPI-Key' : con['key']}
response = requests.get(url, headers=headers)
json_str = json.dumps(response.json())
fo = open("test.json", "w", encoding='utf-8')
fo.write(json_str)
