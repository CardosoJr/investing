from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd 
import numpy as np
from pandas._libs.tslibs import Timestamp
from tqdm import tqdm
from .file_handler import *
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .cripto import binance_api
import json

class Manager: 
    def __init__(self, dataset_dir, asset_config, date_col = 'DATE', format = "parquet"):
        self.dir = Path(dataset_dir) 
        self.latest_date = None
        self.format = format
        self.date_col = date_col
        self.params = asset_config
        self.handlers = {}

        for asset in asset_config.keys():
            self.handlers[asset] = file_handler(dataset_dir, asset)

        self.asset_types = self.handlers.keys()

    def __get_group(self, min_date, grouping):
        if grouping is None:
            return None
        elif grouping == "month":
            return min_date.strftime("%Y%m")
        else:
            return min_date.strftime("%Y")

    def __create_timestamp(self, df, date_col, asset):
        fmt = self.__get_fmt_str(asset)
        df[date_col] = pd.to_datetime(df[date_col])
        timestamp = df[date_col].dt.strftime(fmt)
        return timestamp

    def __get_latest_file(self, asset):
        p = self.dir / asset 
        files = [x.name for x in p.rglob('*') if x.is_file()]
        if len(files) == 0:
            return None
        extension = files[0].split(".")[-1]
        files = [x.split("_")[0] for x in files]
        dates = [int(x) for x in files if x.isdigit()]
        if len(dates) == 0:
            return None
        file_name = str(max(dates)) + "_data." + extension
        latest = p / file_name
        return latest

    def get_latest_files(self):
        paths = {}
        for asset in self.asset_types:
            latest_file = self.__get_latest_file(asset)
            paths[asset] = latest_file

        return paths

    def get_latest_dates(self):
        dates = {}
        for asset in self.handlers.keys():
            dates[asset] = self.get_baseline_date(asset)
        return dates

    def get_baseline_date(self, asset):
        config_file = self.dir / asset / "config.json"
        now = datetime.now()

        if not config_file.exists():
            return now

        with open(config_file) as f:
            data = json.load(f)

        if "LAST_DATE" in data.keys():
            return datetime.strptime(data['LAST_DATE'], "%Y-%m-%d")
        else:
            return now

    def read_data_assets(self, assets, start, end = None):
        result = {}
        for asset in assets:
            data = self.read_data(asset, start, end)
            result[asset] = data
        return result

    def read_data_all(self, start, end = None):
        result = {}
        for asset in self.asset_types:
            data = self.read_data(asset, start, end)
            result[asset] = data
        return result

    def __get_fmt_str(self, asset):
        mode = self.params[asset][0]
        if mode == 'week':
            return "%Y%W"
        elif mode == 'year':
            return "%Y"
        elif mode == 'date':
            return "%Y%m%d"
        else:
            raise Exception("Format not found", mode)

        return None

    def read_data(self, asset, start, end = None):
        if end is None: 
            end = datetime.now()

        if isinstance(start, str):
            start = datetime.strptime(start, "%Y-%m-%d")
        if isinstance(end, str):
            end = datetime.strptime(end, "%Y-%m-%d")

        fmt = self.__get_fmt_str(asset)
        periods = []
        current = start
        while current < end:
            periods.append(current)
            delta = relativedelta(days = 7)
            if self.params[asset][0] == "year":
                delta = relativedelta(years = 1)
            elif self.params[asset][0] == 'date':
                delta = relativedelta(days = 1)

            current = current + delta

        if current[-1].strftime(fmt) != end.strptime(fmt):
            periods.append(end)

        periods = [x.strftime(fmt) for x in periods]
        periods = list(dict.fromkeys(periods)) # Removing duplicates

        asset_dir = self.dir / asset
        all_asset_files = [x for x in asset_dir.rglob("*") if x.is_file()]

        files2read = [x for x in all_asset_files if x.name.split("_")[0] in periods]
        data = []
        for file_path in files2read:
            data.append(self.handlers[asset].read_data(file_path))
            data[-1][self.date_col] = pd.to_datetime(data[-1][self.date_col])
        df = pd.concat(data, ignore_index = True)
        return df

    def append_data(self, df, asset):
        self.__save_config(df[self.date_col].max(), asset)
        df["__custom_timestamp"] = self.__create_timestamp(df, self.date_col, asset)
        for ts, df_ts in df.groupby("__custom_timestamp"):
            df_ts = df_ts.drop(columns = "__custom_timestamp")
            folder_group = self.__get_group(df_ts[self.date_col].min(), self.params[asset][1])
            self.handlers[asset].save_data(df_ts, timestamp = ts, group = folder_group, format = self.format, errors = False)

    def __save_config(self, latest_date, asset):
        config_file = self.dir / asset / "config.json"

        if config_file.exists():
            with open(config_file, 'r') as f:
                data = json.load(f)
        else:
            data = {}
        data['LAST_DATE'] = latest_date.strftime("%Y-%m-%d")

        with open(config_file, 'w') as f:
            json.dump(data, f)
    
    def append_errors(self, df):
        pass

    def update_config(self, update_from_file = None):
        for asset in self.asset_types:
            self.__update_config(asset, update_from_file)

    def __update_config(self, asset, update_from_file = None):
        p = self.dir / asset / 'config.json'

        if p.exists():
            with open(p, 'r') as f: 
                data = json.load(f)
        else:
            data = {}

        if "history" not in asset:
            last_file = self.__get_latest_file(asset + "_history") # read always from history
        else:
            last_file = self.__get_latest_file(asset)
            
        df = self.handlers[asset].read_data(last_file)
        tickers = df['TICKERS'].unique.ravel().tolist()

        if update_from_file is not None:
            new_tickers = pd.read_csv(Path(update_from_file))['TICKER'].ravel().tolist()
            tickers = list(set(tickers) + set(new_tickers))

        data['TICKERS'] = tickers

        with open(p, 'w') as f:
            json.dump(data, f)


