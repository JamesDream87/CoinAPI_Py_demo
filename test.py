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
  end = datetime.date.today()
  start = end + datetime.timedelta(-1)

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
  res = 1
  return res


def CheckJson(PTime):
  if PTime =='1Day':
    fo = open("1D.json")
  elif PTime == '1HRS':
    fo = open("1H.json")

  json_str = json.loads(fo.read())
  num = len(json_str)-1
  result = 0
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
          result += 1
      else:
        if(d3 > 1):
          print(f'缺失位置:{d1}  至  {d2},时长:{d3-1}天')
          result += 1
  return result


def WriteSQL(time):
  #Read the Json
  if time == '1Day':
    time = '1D'
    fo = open("1D.json")
  elif time == '1HRS':
    time = '1H'
    fo = open("1H.json")

  json_str = json.loads(fo.read())

  #Connect the MySQL
  con = config.config
  db = pymysql.connect(con['host'], con['user'], con['password'], con['database'], charset='utf8mb4')
  cursor = db.cursor()

  '''  
    Because of the UTC time format returned by CoinAPI is not standard. 
    The return date data is 7 digits after the decimal point, 
    but the standard is 6, so we need to convert the format.
    由于CoinAPI返回的日期格式并非标准的UTC时间，以下我们需要做一个小小的转化
  '''
  
  for i in range(len(json_str)):
    json_str[i]['time_period_start'] = json_str[i]['time_period_start'].replace('0000000Z', '000000Z')
    json_str[i]['time_period_end'] = json_str[i]['time_period_end'].replace('0000000Z', '000000Z')
    
    if time == '1D':
      sql = 'INSERT INTO Binance_candles_1d(exchange,start_at,end_at,open,high,low,close,volume,trades_count,interval_at)values("BitStamp",%s,%s,%s,%s,%s,%s,%s,%s,"1D")'
    elif time == '1H':
      sql = 'INSERT INTO Binance_candles_1h(exchange,start_at,end_at,open,high,low,close,volume,trades_count,interval_at)values("BitStamp",%s,%s,%s,%s,%s,%s,%s,%s,"1H")'
    
    cursor.execute(sql,(json_str[i]['time_period_start'],json_str[i]['time_period_end'],json_str[i]['price_open'],
    json_str[i]['price_high'],json_str[i]['price_low'],json_str[i]['price_close'],json_str[i]['volume_traded'],json_str[i]['trades_count']))
    db.commit()
    
  db.close()


def Main():
  list = ['1HRS']
  # Get data and check,Write it into database,if the function WriteJson return 1, run the funtion CheckJson.
  # If funtion CheckJson return 0, it means have not any error, run the funtion WriteSQL.
  # 如果WJRes等于1则代表写入Json文件成功，执行检查Json，检查Json函数如果返回0则代表没有错误
  for i in list:
    # WJRes = WriteJson(i)
    # if WJRes == 1:
      CJRes = CheckJson(i)
      if CJRes == 0:
        WriteSQL(i)
        # print(1)
      else:
        print('数据有误！请检查！')
    # else:
    #   print('写入Json文件失败')

Main()