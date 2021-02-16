from time import sleep
import pandas as pd 
import yaml
import sys
import numpy as np
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .cripto import binance_api
from .DSManager import Manager

from pathlib import Path
from tqdm import tqdm

def build_full_assets(path_dir, fundamentos):
    path = Path(path_dir)
    segments = pd.read_csv(Path('./Extractors/b3/segments.csv'), encoding = "ISO-8859-1")
    segments = segments[['Setor', 'Subsetor', 'Codigo']]
    fundamentos['Codigo'] = fundamentos['Ticker'].str[:4]

    asset_df = pd.merge(left = segments, right = fundamentos, on = "Codigo", how = 'left')
    new_columns = [(x.split("'")[-1] if len(x.split("'")) == 1 else (x.split("'")[-2] if x.split("'")[-2] != "" else x.split("'")[1])) for x in asset_df.columns.ravel()]
    asset_df.columns = new_columns
    asset_df = asset_df.loc[:,~asset_df.columns.duplicated()] 
    asset_df.to_csv(path / 'asset_df', index = False)
    return asset_df

def get_fundamentos_simple():
    return pd.DataFrame.from_dict(fundamentus.get_data(), orient = 'index')
     

def build_fundamentos_tickers(path_dir, tickers):
    print('Building Fundamentus data\n')
    path = Path(path_dir)
    fundamentos = pd.DataFrame([])
    ticker_errors = []
    for ticker in tqdm(tickers):
        try:
            df = advfn.get_fundamentos(ticker)
            sleep_time = random_wait()
            sleep(sleep_time)
            fundamentos = fundamentos.append(df)
        except: 
            print("\nerror in trying to get data from ticker", ticker)
            ticker_errors.append(ticker)
        

    # fundamentos = pd.merge(right = tickers, left = fundamentos, on = "Ticker", how = 'left')
    fundamentos.to_csv(path / "fundamentos.csv", index = False)

    with open(path / "log.txt", 'w') as f:
        f.writelines('\n'.join(ticker_errors))

    return fundamentos
    

def build_fundamentos(path_dir):
    tickers = fundamentus.get_tickers()
    tickers.rename(columns = {"Papel":"Ticker"}, inplace = True)
    return build_fundamentos_tickers(path_dir, tickers['Ticker'].unique().ravel())

def random_wait():
    wait_times = [0.2, 0.5, 1, 2]
    probs = [0.3, 0.4, 0.2, 0.1 ]
    choice = np.random.choice(wait_times, size=1, p=probs)
    return choice[0]