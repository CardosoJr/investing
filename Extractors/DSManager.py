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

class Manager: 
    def __init__(self, dataset_dir, date_col = 'DATE', format = "parquet", resolution = "week", grouping = "month"):
        self.dir = Path(dataset_dir) 
        self.latest_date = None
        self.format = format
        self.resolution = resolution
        self.grouping = grouping
        self.date_col = date_col
        self.handlers = {
            "cripto" : file_handler(dataset_dir, "cripto"),
            "b3"  : file_handler(dataset_dir, "b3"),
            "b3_funds" : file_handler(dataset_dir, "b3_funds"),
            "funds" : file_handler(dataset_dir, "funds")
        }
        self.modes = self.handlers.keys()

    def __get_group(self, min_date):
        if self.grouping == "month":
            return min_date.strftime("%Y%m")
        else:
            return min_date.strftime("%Y")

    def __create_timestamp(self, df, date_col):
        if self.resolution == "date":
            timestamp = df[date_col].dt.strftime("%Y%m%d")
        elif self.resolution == "week":
            timestamp = df[date_col].dt.strftime("%Y%W")
        return timestamp

    def __get_latest_folder(self, type):
        p = self.dir / "type"
        folders = [x.name for x in p.glob('*') if x.is_dir()]
        if len(folders) == 0:
            return None
        dates = [datetime.strptime(x, "%Y%m") for x in folders]
        latest = self.dir / "type" /  max(dates).strftime("%Y%m")
        return latest

    def __get_latest_file(self, file_path):
        if file_path is None:
            return None
        p = file_path
        files = [x.name for x in p.glob('*') if x.is_file()]
        if len(files) == 0:
            return None
        extension = files[0].split(".")[-1]
        files = [x.split(".")[0] for x in files]
        dates = [int(x) for x in files]
        latest = p.parent / str(max(dates)) + "." + extension
        return latest

    def get_latest_files(self):
        paths = {}
        for mod in self.modes:
            latest = self.__get_latest_folder(mod)
            latest_file = self.__get_latest_file(latest)
            paths[mod] = latest_file

        return paths

    def get_latest_dates(self):
        dates = {}
        files = self.get_latest_files()
        for mod, file in files.items():
            dates[mod] = self.get_baseline_date(mod, file)
        
        return dates

    def get_baseline_date(self, asset, file):
        if file is None:
            return datetime.now()
        data = self.handlers[asset].read_data(file)
        latest_date = pd.to_datetime(data[self.date_col]).max() + relativedelta(days = 1)
        now = datetime.now()
        if latest_date > now:
            latest_date = now
        return latest_date

    def append_data(self, df, asset):
        df["__custom_timestamp"] = self.__create_timestamp(df, self.date_col)

        for ts, df_ts in df.groupby("__custom_timestamp"):
            df_ts = df_ts.drop(columns = "__custom_timestamp")
            folder_group = self.__get_group(df_ts[self.date_col].min())
            self.handlers[asset].save_data(df_ts, timestamp = ts, group = folder_group, format = self.format, errors = False)

    def append_errors(self, df):
        pass

