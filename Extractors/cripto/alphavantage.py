from os import execlpe
import pandas as pd 
import numpy as np 
import requests
import io
import yaml

class Alphavantage:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.load(f)


        self.Key = config['key']
        self.Interval = {1 : '1min', 5 : '5min', 30 : '30min', 60 : '60min'}
        
    def __convert_months(self, months):
        if (months <= 12):
            return f"year1month{months}"
        else:
            return f"year2month{months - 12}"

    def get_history_data(self, symbol, past_months, interval_min = 1):
        if(past_months > 24 or past_months <= 0):
            raise Exception("past_months should be between 1 and 24")
        query = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={}&interval={}&slice={}&apikey={}".format(
            symbol, self.Interval[interval_min], self.__convert_months(past_months), self.Key)
        print(query)
        data = requests.get(query).content
        result = pd.read_csv(io.StringIO(data.decode('utf-8')))
        return result

    def get_day_data(self, symbol, interval_min):
        query = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval={}&outputsize=full&datatype=csv&apikey={}".format(
            symbol, self.Interval[interval_min], self.Key)
        print(query)
        data = requests.get(query).content
        result = pd.read_csv(io.StringIO(data.decode('utf-8')))
        return result

    def get_cripto_data(self, symbol, market):
        query = "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={}&market={}&apikey={}".format(
            symbol, market, self.Key
        )
        data = requests.get(query).json()
        result = pd.DataFrame.from_dict(data["Time Series (Digital Currency Daily)"], orient = 'index')
        return result
