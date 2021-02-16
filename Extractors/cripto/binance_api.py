import pandas as pd
import math
import os.path
import time
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
from tqdm import tqdm
import yaml

class Binance:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.load(f)
        self.binance_api_key = config['binance_key']
        self.binance_api_secret = config['binance_secret'] 
        ### CONSTANTS
        self.binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
        self.batch_size = 750
        self.binance_client = Client(api_key = self.binance_api_key, api_secret = self.binance_api_secret)

    def minutes_of_new_data(self, symbol, kline_size, data, source):
        if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
        elif source == "binance": old = datetime.strptime('1 Jan 2017', '%d %b %Y')
        if source == "binance": new = pd.to_datetime(self.binance_client.get_klines(symbol = symbol, interval=kline_size)[-1][0], unit='ms')
        return old, new

    def get_binance_data(self, symbol, kline_size, start, end):
        klines = self.binance_client.get_historical_klines(symbol, kline_size,
                                                            start, 
                                                            end)
        data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')

        return data

    def get_all_binance(self, symbol, kline_size, save = False):
        filename = '%s-%s-data.csv' % (symbol, kline_size)
        
        if os.path.isfile(filename):
             data_df = pd.read_csv(filename)
        else: 
            data_df = pd.DataFrame()
        
        oldest_point, newest_point = self.__class__minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
        delta_min = (newest_point - oldest_point).total_seconds()/60
        available_data = math.ceil(delta_min / self.binsizes[kline_size])
        if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'):
             print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
        else: 
            print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
        
        klines = self.binance_client.get_historical_klines(symbol, kline_size,
                                                            oldest_point.strftime("%d %b %Y %H:%M:%S"), 
                                                            newest_point.strftime("%d %b %Y %H:%M:%S"))
        data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        if len(data_df) > 0:
            temp_df = pd.DataFrame(data)
            data_df = data_df.append(temp_df)
        else: 
            data_df = data
        data_df.set_index('timestamp', inplace=True)
        
        if save:
             data_df.to_csv(filename)
        
        print('All caught up..!')
        return data_df
