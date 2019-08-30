import requests
import json
import config
import pymysql
import datetime


def WriteJson():
    con = config.config
    url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1Day&time_start=2019-08-29T00:00:00&time_end=2019-08-30T00:00:00&limit=8760'
    headers = {'X-CoinAPI-Key': con['key']}
    response = requests.get(url, headers=headers)
    json_str = json.dumps(response.json())
    fo = open("test.json", "w", encoding='utf-8')
    fo.write(json_str)


def CheckJson():
    fo = open("./DataFeed/BTC-1H-2015.json")
    json_str = json.loads(fo.read())
    num = len(json_str)-1

    for i in range(len(json_str)):
        if i < num:
            json_str[i]['time_period_start'] = json_str[i]['time_period_start'].replace('0000000Z', '000000Z')
            json_str[i+1]['time_period_start'] = json_str[i+1]['time_period_start'].replace('0000000Z', '000000Z')
            d1 = datetime.datetime.strptime(json_str[i]['time_period_start'], '%Y-%m-%dT%H:%M:%S.%sZ')
            d2 = datetime.datetime.strptime(json_str[i+1]['time_period_start'], '%Y-%m-%dT%H:%M:%S.%sZ')
            d3 = (d2-d1).days
            if d3 == 0 :
              d4 = (d2-d1).seconds
              if(d4 > 3600):
                print(f'缺失位置:{d1}  至  {d2},时长:{(d4-3600)/3600}小时')
            else:
              if(d3 > 1):
                print(f'缺失位置:{d1}  至  {d2},时长:{d3-1}天')


def WriteSQL(time):

  if time == '1Day':
    time = '1D'
  elif time == '1HRS':
    time = '1H'

  #Read the Json
  fo = open("./DataFeed/BTC-1D-ALL.json")
  json_str = json.loads(fo.read())

  #Connect the MySQL
  con = config.config
  db = pymysql.connect(con['host'], con['user'], con['password'], con['database'], charset='utf8mb4')
  cursor = db.cursor()
  '''  
    The UTC time format returned by CoinAPI is not standard. 
    The return date data is 7 digits after the decimal point before the letter Z, 
    but the standard is 6, so we need to convert the format.
    由于CoinAPI返回的日期格式并非标准的UTC时间，以下我们需要做一个小小的转化
  '''
  
  for i in range(len(json_str)):
    json_str[i]['time_period_start'] = json_str[i]['time_period_start'].replace('0000000Z', '000000Z')
    json_str[i]['time_period_end'] = json_str[i]['time_period_end'].replace('0000000Z', '000000Z')
    
    if time == '1D':
      sql = 'INSERT INTO candles_1d(exchange,start_at,end_at,open,high,low,close,volume,trades_count,interval_at)values("BitStamp",%s,%s,%s,%s,%s,%s,%s,%s,"1D")'
    elif time == '1H':
      sql = 'INSERT INTO candles_1h(exchange,start_at,end_at,open,high,low,close,volume,trades_count,interval_at)values("BitStamp",%s,%s,%s,%s,%s,%s,%s,%s,"1H")'
    
    cursor.execute(sql,(json_str[i]['time_period_start'],json_str[i]['time_period_end'],json_str[i]['price_open'],
    json_str[i]['price_high'],json_str[i]['price_low'],json_str[i]['price_close'],json_str[i]['volume_traded'],json_str[i]['trades_count']))
    db.commit()
    
  db.close()

WriteSQL('1Day')
