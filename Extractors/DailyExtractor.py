from time import sleep
import pandas as pd 
import yaml
import sys
import numpy as np
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .fundos import fundos
from .cripto import binance_api
from .DSManager import Manager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from tqdm import tqdm

class DailyExtractor:
    def __init__(self, project_dir, baseline = None, assets = None):
        self.baseline = baseline
        self.path = Path(__file__).parent
        self.manager = Manager(project_dir)
        self.dir = project_dir
        self.assets = assets

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
            asset_data = pd.DataFrame([])
            while initial <= datetime.now():
                data = self.get_data(initial, asset)
                asset_data = asset_data.append(data)
                initial = initial + relativedelta(days = 1)
            self.manager.append_data(asset_data, asset)

        pass

    def get_data(self, date, asset):
        if asset == "b3":
            pass
        elif asset == "cripto":
            pass
        elif asset == "b3_funds":
            pass
        else:
            raise Exception(f"Asset {asset} not supported")

        random_wait()
        return None

    def __get_baseline_date(self):
        pass
        
    def random_wait():
        wait_times = [0.2, 0.5, 1, 2, 4]
        probs = [0.3, 0.4, 0.2, 0.08, 0.02]
        choice = np.random.choice(wait_times, size=1, p=probs)
        sleep(choice[0])