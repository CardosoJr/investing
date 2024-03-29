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
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from tqdm import tqdm

def build_price_history(path_dir, tickers):
    path = Path(path_dir)
    api = b3.B3()
    history = pd.DataFrame([])
    ticker_errors = []
    for ticker in tqdm(tickers):
        try:
            df = api.Extract_History(ticker + ".SA")
            df = df.reset_index()
            sleep(random_wait())
            history = history.append(df)

        except:
            ticker_errors.append(ticker)
            print("Could not read history from", ticker, "\n")

    history.to_csv(path / "history.csv", index = False)

    with open(path / "log_b3.txt", 'w') as f:
        f.write('\n'.join(ticker_errors))

    return history

def build_fundos_history(path_dir):
    path = Path(path_dir)
    end = datetime.now()
    start = end + relativedelta(years = -2)
    result = fundos.get_data(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), min_cot = 100)
    result.to_csv(path / "fundos_history.csv", index = False)
    return result

def build_fundos_b3_history(path_dir):
    path = Path(path_dir)
    api = b3.B3()
    history = pd.DataFrame([])
    assets = pd.DataFrame([])
    ticker_errors = []
    tickers = fundos.get_b3_fundos_tickers()
    for ticker in tqdm(tickers):
        try:
            df = api.Extract_History(ticker + ".SA")
            df = df.reset_index()
            sleep_time = random_wait()
            sleep(sleep_time)
            history = history.append(df)
        except:
            ticker_errors.append(ticker)
            print("Could not read history from", ticker, "\n")

        try:
            df = pd.DataFrame([api.Get_Summary(ticker + ".SA")])
            df['Ticker'] = [ticker] * len(df)
            sleep_time = random_wait()
            sleep(sleep_time)
            assets = assets.append(df)
        except:
            ticker_errors.append(ticker)
            print("Could not read summary from", ticker, "\n")

    history.to_csv(path / "fundos_b3_history.csv", index = False)
    assets.to_csv(path / "assets_fundos_b3.csv", index = False)

    with open(path / "log_fundos_b3.txt", 'w') as f:
        f.write('\n'.join(ticker_errors))

    return history

def build_cripto_history(path_dir, tickers):
    path = Path(path_dir)
    api = b3.B3()
    history = pd.DataFrame([])
    ticker_errors = []
    for ticker in tqdm(tickers):
        try:
            df = api.Extract_History(ticker + "-USD")
            df = df.reset_index()
            sleep_time = random_wait()
            sleep(sleep_time)
            history = history.append(df)

        except:
            ticker_errors.append(ticker)
            print("Could not read history from", ticker, "\n")

    history.to_csv(path / "cripto_history.csv", index = False)

    with open(path / "log_cripto.txt", 'w') as f:
        f.write('\n'.join(ticker_errors))

    return history

def build_full_assets(path_dir, fundamentos):
    path = Path(path_dir)
    segments = pd.read_csv(Path('./Extractors/b3/segments.csv'), encoding = "ISO-8859-1")
    segments = segments[['Setor', 'Subsetor', 'Codigo']]
    fundamentos['Codigo'] = fundamentos['Ticker'].str[:4]

    asset_df = pd.merge(left = segments, right = fundamentos, on = "Codigo", how = 'left')
    asset_df.to_csv(path / 'assets.csv', index = False)
    return asset_df

def get_fundamentos_simple():
    return pd.DataFrame.from_dict(fundamentus.get_data(), orient = 'index')
     

def build_fundamentos_tickers(path_dir, tickers, year = None, quarter = None):
    print('Building Fundamentus data\n')
    path = Path(path_dir)
    fundamentos = pd.DataFrame([])
    ticker_errors = []
    for ticker in tqdm(tickers):
        try:
            df = advfn.get_fundamentos(ticker, year = year, quarter = quarter)
            sleep_time = random_wait()
            sleep(sleep_time)
            fundamentos = fundamentos.append(df)
        except: 
            print("\nerror in trying to get data from ticker", ticker)
            ticker_errors.append(ticker)

    fundamentos.to_csv(path / "bkp_fund.csv", index = False)   

    # fundamentos = pd.merge(right = tickers, left = fundamentos, on = "Ticker", how = 'left')
    new_columns = [x[-1] if x[-1] != "" else x[0] for x in fundamentos.columns.ravel()]
    fundamentos.columns = new_columns
    fundamentos = fundamentos.loc[:,~fundamentos.columns.duplicated()] 
    fundamentos.to_csv(path / "fundamentos.csv", index = False)

    with open(path / "log.txt", 'w') as f:
        f.writelines('\n'.join(ticker_errors))

    return fundamentos
    

def build_fundamentos(path_dir):
    tickers = fundamentus.get_tickers()
    tickers.rename(columns = {"Papel":"Ticker"}, inplace = True)
    return build_fundamentos_tickers(path_dir, tickers['Ticker'].unique().ravel())

def random_wait():
    wait_times = [0.2, 0.5, 1, 2, 4]
    probs = [0.3, 0.4, 0.2, 0.08, 0.02]
    choice = np.random.choice(wait_times, size=1, p=probs)
    return choice[0]