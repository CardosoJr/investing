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

def build_fundamentos(path_dir):
    path = Path(path_dir)
    tickers = fundamentus.get_tickers()
    tickers.rename(columns = {"Papel":"Ticker"}, inplace = True)

    fundamentos = pd.DataFrame([])
    ticker_errors = []
    for ticker in tqdm(tickers['Ticker'].unique().ravel()):
        try:
            df = advfn.get_fundamentos(ticker)
            sleep_time = random_wait()
            sleep(sleep_time)
            fundamentos = fundamentos.append(df)
        except: 
            print("error in trying to get data from ticker", ticker)
            ticker_errors.appendq(ticker)
        

    fundamentos = pd.merge(right = tickers, left = fundamentos, on = "Ticker", how = 'left')
    fundamentos.to_csv(path / "fundamentos.csv", index = False)

    with open(path / "log.txt", 'w') as f:
        f.writelines(ticker_errors)

    segments = pd.read_csv('./Extractors/b3/segments.csv')
    segments = segments[['Setor', 'Subsetor', 'Codigo']]
    fundamentos['Codigo'] = fundamentos['Ticker'].str[:4]

    asset_df = pd.merge(left = segments, right = fundamentos, on = "Codigo", how = 'left')
    asset_df.to_csv(path / 'asset_df', index = False)

    return fundamentos

def random_wait():
    wait_times = [0.2, 0.5, 1, 2]
    probs = [0.3, 0.4, 0.2, 0.1 ]
    choice = np.random.choice(wait_times, size=1, p=probs)
    return choice