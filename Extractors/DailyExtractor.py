from time import sleep
import pandas as pd 
import yaml
import sys
import numpy as np
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .fundos import fundos
from .DSManager import Manager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from tqdm import tqdm
import json

class DailyExtractor:
    def __init__(self, project_dir, baseline = None, assets = None, interval = "1m"):
        self.baseline = baseline
        self.path = Path(__file__).parent
        self.manager = Manager(project_dir)
        self.dir = Path(project_dir)
        self.assets = assets
        self.interval = interval
        self.b3_api = b3.B3()

    def run(self, baseline_date = None):
        print("Extracting Intraday data")
        dates = {}
        if baseline_date is None:
            dates = self.manager.get_latest_dates()
        else:
            for asset in self.assets:
                dates[asset] = baseline_date

        for asset in self.assets: 
            initial = dates[asset]
            data = self.get_data(initial, asset)
            if len(data) > 0:
                self.manager.append_data(data, asset)
            
            history = self.get_history(initial, asset)
            self.save_history(history, asset)

    def save_history(self, data, asset):
        if asset == "b3":
            df = pd.read_csv(self.dir / asset / "history.csv")
            df = df.append(data)
            df.to_csv(self.dir / asset / "history.csv", index = False)
        elif asset == "cripto":
            df = pd.read_csv(self.dir / asset / "cripto_history.csv")
            df = df.append(data)
            df.to_csv(self.dir / asset / "cripto_history.csv", index = False)
        elif asset == "b3_funds":
            padf = pd.read_csv(self.dir / asset / "fundos_b3_history.csv")
            df = df.append(data)
            df.to_csv(self.dir / asset / "fundos_b3_history.csv", index = False)
        elif asset == "funds":
            df = pd.read_csv(self.dir / asset / "fundos_history.csv")
            df = df.append(data)
            df.to_csv(self.dir / asset / "fundos_history.csv", index = False)
        else:
            raise Exception(f"Asset {asset} not supported")
    
    def __get_tickers(self, asset):
        with open(self.dir / asset / "config.json", 'r') as f:
            config = json.load(f)
        return config['TICKERS']

    def get_history(self, date, asset):
        data = pd.DataFramee([])
        if asset == "b3":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date, "1d")
        elif asset == "cripto":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date, "1d")
        elif asset == "b3_funds":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date , "1d")
        elif asset == "funds":
            data = self.__extract_funds_history(date)
        else:
            raise Exception(f"Asset {asset} not supported")
        return data

    def get_data(self, date, asset):
        data = pd.DataFramee([])
        if asset == "b3":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date, self.interval)
        elif asset == "cripto":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date, self.interval)
        elif asset == "b3_funds":
            data = self.__extract_intraday(asset, self.__get_tickers(asset), date, self.interval)
        elif asset == "funds":
            pass
        else:
            raise Exception(f"Asset {asset} not supported")

        return data

    def random_wait(self):
        wait_times = [0.2, 0.5, 1, 2, 4]
        probs = [0.3, 0.4, 0.2, 0.08, 0.02]
        choice = np.random.choice(wait_times, size=1, p=probs)
        sleep(choice[0])

    def __extract_funds_history(self, date):
        result = fundos.get_data(date.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"), min_cot = 100)
        result = result.rename(columns = {'DT_COMPTC' : 'DATE', 'CNPJ_FUNDO': "TICKER"})
        result = result[pd.to_datetime(result['DATE']) >= date]
        return result

    def __extract_intraday(self, asset, tickers, date, interval):
        delta = (datetime.now() - date).days
        if  delta > 30 and interval != "1d" and interval != "1h":
            date = datetime.now() + relativedelta(days = -30)

        all_data = pd.DataFrame([])
        errors = []
        for ticker in tqdm(tickers):
            try:
                data = self.b3_api.Extract_Data(ticker, start = date.strftime("%Y-%m-%d"), end = datetime.now().strftime("%Y-%m-%d"), interval = self.interval)
                data = data.reset_index().rename(columns = {"symbol" : "TICKER", "date" : "DATE"})
                all_data = all_data.append(data)
            except: 
                print("Could not load data from", ticker)
                errors.append(ticker)
            self.random_wait()

        with open(self.dir / asset / datetime.now().stftime("%Y%m%d") +"_log.txt", 'w') as f:
            f.write('\n'.join(errors))

        return all_data
        