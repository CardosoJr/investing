import pandas as pd 
import numpy as np 
import requests
import io


class Getter:

    def __init__(self, key):
        self.Key = key
        self.Interval = {1 : '1min', 5 : '5min', 30 : '30min'}

        
    def get_history_data(self, symbol, start_time, end_time, interval_min = 1):
        query = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}&interval={}&slice={}&apikey={}".format(
            symbol, self.Interval[interval_min], 'year1month1', self.Key)
        data = requests.get(query).content
        result = pd.read_csv(io.StringIO(data.decode('utf-8')))
        return result


    def get_day_data(self, symbol, interval_min):
        pass
    
    def current_cripto_data(self, symbol, to_currency):
        pass

    def get_cripto_data(self, symbol, market):
        query = "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={}&market={}&apikey={}".format(
            symbol, market, self.Key
        )
        print(query)
        data = requests.get(query).json()
        result = pd.DataFrame.from_dict(data["Time Series (Digital Currency Daily)"], orient = 'index')
        return result
