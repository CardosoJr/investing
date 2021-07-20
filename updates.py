from Extractors.b3.b3 import B3
from pandas.core.frame import DataFrame
import Extractors.DailyExtractor as daily
from pathlib import Path
import pandas as pd 
from datetime import datetime
from argparse import ArgumentParser
import Extractors.fundamentus.fundamentus as fundamentus
import Extractors.fundamentus.advfn as advfn
import Extractors.fundos.fundos as fundos
import json
from tqdm import tqdm
import yfinance as yf
import numpy as np
from time import sleep

import Extractors.b3 as b3 

"""
Updating company fundamental data, ticker list and so on
"""

def main():
    pass
    parser = ArgumentParser()
    parser.add_argument("--mode", type = str, default = '')
    parser.add_argument("--data_path", type = str, default = './DATA')
    parser.add_argument('--fundos_csv', type = str, default = None)
    args = parser.parse_args()
    path = Path(args.data_path)

    if args.mode == "fundamental":
        update_fundamental_data(path)
    elif args.mode == "ticker":
        update_ticker_list(path)
    elif args.mode == "ticker_fundos":
        update_fundos(path, parser.fundos_csv)
    else:
        print("Error - mode not found")

def update_fundamental_data(path):
    with open (path / "b3/config.json", 'r') as f:
        config = json.load(f) 

    tickers = config['TICKERS']

    fundamental_df = pd.read_csv(path / "b3_history/fundamentos.csv")
    data = []

    for t in tqdm(tickers):
        sleep(random_wait())
        if sum(fundamental_df['TICKER'] == t) > 0:
            df = advfn.get_all_data(t.replace(".SA", ""), get_all = False)
        else:
            df = advfn.get_all_data(t.replace(".SA", ""), get_all = True)
        if len(df) == 0:
            continue
        df = df.rename(columns = {'Ticker' : 'TICKER', 'Data' : 'DATE'})
        data.append(df)
    data.append(fundamental_df)
    result = pd.concat(data, ignore_index = True)

    result = result.drop_duplicates(subset = ['TICKER', 'DATE'])
    result.to_csv(path / "b3_history/fundamentos.csv", index = False)

def update_fundos(path, csv_file):
    if csv_file is None:
        raise Exception("argument fundos_csv must not be null")

    fii = Path(csv_file) / "fii.csv"
    etf = Path(csv_file) / "eft.csv"

    fii_df = pd.read_csv(fii)
    etf_df = pd.read_csv(etf)

    all_tickers = fii_df['Código'].ravel() + etf_df['Codigo'].ravel()
    all_tickers = [x + "11.SA" for x in all_tickers]

    __update_files([path / "b3_fundos/config.json", path / "b3_fundos_history/config.json"], all_tickers)

    fii_df = fii_df.rename(columns = {"Razão Social" :  "LongName", "Fundo" : "ShortName", "Segment" : "Sector", "Código" : "TICKER"})
    etf_df = etf_df.rename(columns = {"Razão Social" :  "LongName", "Fundo" : "ShortName", "Segment" : "Sector", "Código" : "TICKER"})
    
    fii_df['TICKER'] = fii_df["TICKER"] + "11"
    etf_df['TICKER'] = etf_df["TICKER"] + "11"

    fii_df['Type'] = ["FII"] * len(fii_df)
    etf_df['Type'] = ["ETF"] * len(etf_df)

    assets = pd.read_csv(path / "b3_fundos_history/assets.csv")
    assets = pd.concat([assets, fii_df, etf_df], ignore_index = True)
    assets = assets.drop_duplicates(subset = ["TICKER"])
    assets.to_csv(path / "b3_fundos_history/assets.csv")

    ## TODO: Updating fundamentals (develop FII fundamental extractor)
    b3 = B3()
    for t in tqdm(all_tickers):
        try:
            data = b3.Get_Summary(t)
        except: 
            print("Could not load fundamentals from", t)


def update_ticker_list(path):
    df = fundamentus.get_tickers(validate_tickers = True)
    ticker_series = df['Papel'] + ".SA"
    valid_tickers = ticker_series.unique().tolist()
    
    files_to_update = [path / "b3/config.json", 
                       path / "b3_history/config.json"]

    __update_files(files_to_update, valid_tickers)

    # with open(path / "b3/config.json", "r") as f:
    #     valid_tickers = json.load(f)['TICKERS']

    ## Update assets file / with sector info
    file = path / "b3_history/assets.csv"

    if file.exists():
        assets = pd.read_csv(path / "b3_history/assets.csv")
    else:
        assets = pd.DataFrame([])
    
    new_assets = {"TICKER" : [], "Sector" : [], "Industry" : [], "ShortName" : [], "LongName" : []}
    for t in tqdm(valid_tickers):
        if len(assets) > 0 and t in assets['TICKER'].ravel():
            continue
        try:
            ticker_info = __get_ticker_info(t)
            new_assets['TICKER'].append(t)
            new_assets['Sector'].append(ticker_info[0])
            new_assets['Industry'].append(ticker_info[1])
            new_assets['ShortName'].append(ticker_info[2])
            new_assets['LongName'].append(ticker_info[3])
        except Exception as e:
            print(str(e))
            print("Could not get ticker info from", t)

    new_assets = pd.DataFrame(new_assets)
    assets = pd.concat([assets, new_assets], ignore_index = True)
    assets.to_csv(path / "b3_history/assets.csv", index = False)

def __get_ticker_info(ticker):
    info = yf.Ticker(ticker).info
    return info['sector'], info['industry'], info['shortName'].split("  ")[0], info['longName'], 

def __update_files(files, valid_ticker_list):
    for file in files:
        with open(file, "r") as f:
            data = json.load(f)
        data['TICKERS'] = valid_ticker_list

        with open(file, "w") as f: 
            json.dump(data, f)

def random_wait():
    wait_times = [0.2, 0.5, 1, 2, 4]
    probs = [0.3, 0.4, 0.2, 0.08, 0.02]
    choice = np.random.choice(wait_times, size=1, p=probs)
    return choice[0]

if __name__ == "__main__":
    main()
