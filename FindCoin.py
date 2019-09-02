import requests,json
import config

# Find the BlockCoin data start date
con = config.config
url = 'https://rest.coinapi.io/v1/symbols?filter_symbol_id=SPOT_EOS_USD'
headers = {'X-CoinAPI-Key' : con['key']}
response = requests.get(url, headers=headers)
json_str = json.dumps(response.json())
fo = open("test.json", "w", encoding='utf-8')
fo.write(json_str)