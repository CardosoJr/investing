from re import S
from time import sleep
import pandas as pd 
import yaml
import sys
import numpy as np
from .Extractors.fundamentus import fundamentus, advfn
from .Extractors.b3 import b3
from .Extractors.cripto import binance_api
from .Extractors.DSManager import Manager
from tqdm import tqdm

def build_fundamentos(path_dir):
    tickers = fundamentus.get_tickers()
    tickers.rename(columns = {"Papel":"Ticker"}, inplace = True)

    fundamentos = pd.DataFrame([])
    ticker_errors = []
    for ticker in tickers['Ticker'].unique().ravel():
        try:
            df = advfn.get_fundamentos(ticker)
            sleep_time = random_wait()
            sleep(sleep_time)
            fundamentos = fundamentos.append(df)
        except: 
            print("error in trying to get data from ticker", ticker)
            ticker_errors.appendq(ticker)
        

    fundamentos = pd.merge(right = tickers, left = fundamentos, on = "Ticker", how = 'left')
    fundamentos.to_csv(path_dir + "/fundamentos.csv", index = False)

    with open("log.txt", 'w') as f:
        f.writelines(ticker_errors)

    return fundamentos

def random_wait():
    wait_times = [0.2, 0.5, 1, 2]
    probs = [0.3, 0.4, 0.2, 0.1 ]
    choice = np.random.choice(wait_times, size=1, p=probs)
    return choice


if __name__ == "__main__":
    config_path = sys.argv[1]
    method = sys.argv[2]
    path = sys.argv[3]

    print('Reading Config in', config_path)
    with open(config_path, 'r') as f:
        config = yaml.load(f)

    if method == 'build_initial_dataset':
        portfolio = sys.argv[4] 
        port_df = pd.read_csv(portfolio)
        segments = pd.read_csv('./Extractors/b3/segments.csv')
        segments = segments[['Setor', 'Subsetor', 'Codigo']]
        
        fundamentos = build_fundamentos(path)
        fundamentos['Codigo'] = fundamentos['Ticker'].str[:4]

        asset_df = pd.merge(left = segments, right = fundamentos, on = "Codigo", how = 'left')
        asset_df.to_csv(path + '/asset_df', index = False)
