import requests
import json
import config
import datetime


def Write():
    con = config.config
    url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1HRS&time_start=2013-02-24T00:00:00&time_end=2013-02-24T22:00:00&limit=8760'
    headers = {'X-CoinAPI-Key': con['key']}
    response = requests.get(url, headers=headers)
    json_str = json.dumps(response.json())
    fo = open("test.json", "w", encoding='utf-8')
    fo.write(json_str)


def iterJson():
    fo = open("./DataFeed/BTC-1H-2015.json")
    json_str = json.loads(fo.read())
    num = len(json_str)-1

    for i in range(len(json_str)):
        if i < num:
            json_str[i]['time_period_start'] = json_str[i]['time_period_start'].replace('0000000Z', '000000Z')
            json_str[i+1]['time_period_start'] = json_str[i+1]['time_period_start'].replace('0000000Z', '000000Z')
            d1 = datetime.datetime.strptime(json_str[i]['time_period_start'], '%Y-%m-%dT%H:%M:%S.%fZ')
            d2 = datetime.datetime.strptime(json_str[i+1]['time_period_start'], '%Y-%m-%dT%H:%M:%S.%fZ')
            d3 = (d2-d1).days
            if d3 == 0 :
              d4 = (d2-d1).seconds
              if(d4 > 3600):
                print(f'缺失位置:{d1}  至  {d2},时长:{(d4-3600)/3600}小时')
            else:
              if(d3 > 1):
                print(f'缺失位置:{d1}  至  {d2},时长:{d3-1}天')

iterJson()
