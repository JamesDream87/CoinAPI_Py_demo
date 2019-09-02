import BitFinexETH
import BitStampBTC
import BinanceBTC

def Init():
  # write bitstamp btc
  BitStampBTC.Main()
  # write  binance btc
  BinanceBTC.Main()
  # write bitfinex eth 
  BitFinexETH.Main()

Init()