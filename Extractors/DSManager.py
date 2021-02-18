from datetime import date
import pandas as pd 
import numpy as np
from pandas._libs.tslibs import Timestamp
from tqdm import tqdm
from .file_handler import *
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .cripto import binance_api

class Manager: 
    def __init__(self, dataset_dir, format = "parquet", resolution = "week", grouping = "month"):
        self.dir = dataset_dir 
        self.latest_date = None
        self.format = format
        self.resolution = resolution
        self.grouping = grouping
        self.handlers = {
            "cripto" : file_handler(dataset_dir, "cripto"),
            "b3"  : file_handler(dataset_dir, "b3"),
            "b3_funds" : file_handler(dataset_dir, "b3_funds"),
            "fundamentus" : file_handler(dataset_dir, "fundamentus")
        }

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
        p = Path(self.dir) / "type"
        folders = [x.name for x in p.glob('*') if x.is_dir()]
        dates = [datetime.strptime(x, "%Y%m") for x in folders]
        latest = Path(self.dir) / "type" / max(dates).strptime("%Y%m")
        return latest

    def __get_latest_file(self, file_path):
        p = Path(file_path)
        files = [x.name for x in p.glob('*') if x.is_file()]
        extension = files[0].split(".")[-1]
        files = [x.split(".")[0] for x in files]
        dates = [datetime.strptime(x, "%Y%W") for x in files]
        latest = p.parent / max(dates).strptime("%Y%W") + "." + extension
        return latest


    def get_latest_files(self):
        paths = {
            "cripto" : None,
            "b3"  : None,
            "b3_funds" : None,
            "fundamentus" : None
        }

        return paths

    def append_data(self, df, type, date_col):
        df["__custom_timestamp"] = self.__create_timestamp(df, date_col)

        for ts, df_ts in df.groupby("__custom_timestamp"):
            df_ts = df_ts.drop(columns = "__custom_timestamp")
            folder_group = self.__get_group(df_ts[date_col].min())
            self.handlers[type].save_data(df_ts, timestamp = ts, group = folder_group, format = self.format, errors = False)

    def append_errors(self, df):
        pass

    def random_wait(self):
        wait_times = [0.2, 0.5, 1, 2]
        probs = [0.3, 0.4, 0.2, 0.1 ]
        choice = np.random.choice(wait_times, size=1, p=probs)
        return choice
