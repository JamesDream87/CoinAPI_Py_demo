import requests
import json
import config
import pymysql
import datetime



def WriteJson(PTime):
  con = config.config
  base = 'https://rest.coinapi.io/v1/ohlcv/Binance_SPOT_BTC_USDT/history?'
  period = PTime
  limit = '50'

  # Set the Start time and End time
  now = datetime.datetime.now()
  end = now - datetime.timedelta(hours=now.hour-8, minutes=now.minute, seconds=now.second,microseconds=now.microsecond)
  # end = datetime.date.today()
  start = end + datetime.timedelta(days = -1)
  print(start, end)
  # format the time
  end = end.strftime('%Y-%m-%dT%H:%M:%S')
  start = start.strftime('%Y-%m-%dT%H:%M:%S')

  url = base + 'period_id=' + period + '&time_start=' + start + '&time_end=' + end + '&limit=' + limit
  headers = {'X-CoinAPI-Key': con['key']}
  response = requests.get(url, headers=headers)
  json_str = json.dumps(response.json())
  
  if PTime == '1Day':
    fo = open("1D.json", "w", encoding='utf-8')
  elif PTime == '1HRS':
    fo = open("1H.json", "w", encoding='utf-8')

  fo.write(json_str)

WriteJson('1Day')