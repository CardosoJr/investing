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
        if False and sum(fundamental_df['TICKER'] == t) > 0:
            df = advfn.get_all_data(t, get_all = False)
        else:
            df = advfn.get_all_data(t, get_all = True)
        df = df.rename(columns = {'Ticker' : 'TICKER', 'Data' : 'DATE'})
        data.append(df)
    data.append(fundamental_df)
    result = pd.concat(data, ignore_index = True)

    result = result.drop_duplicates(subset = ['TICKER', 'DATE'])
    result.to_csv(path / "b3_history/fundamentos.csv", index = False)

def update_fundos(path, csv_file):
    if csv_file is None:
        raise Exception("argument fundos_csv must not be null")
    pass

def update_ticker_list(path):
    df = fundamentus.get_tickers(validate_tickers = True)
    valid_tickers = df['Papel'].unique().tolist()
    
    files_to_update = [path / "b3/config.json", 
                       path / "b3_history/config.json"]

    __update_files(files_to_update, valid_tickers)

    ## Update assets file / with sector info
    file = path / "b3_history/assets.csv"

    if file.exists():
        assets = pd.read_csv(path / "b3_history/assets.csv")
    else:
        assets = pd.DataFrame([])
    
    new_assets = {"TICKER" : [], "Sector" : [], "Industry" : [], "ShortName" : [], "LongName" : []}
    for t in valid_tickers:
        if len(assets) > 0 and t in assets['TICKER'].ravel():
            continue
        try:
            ticker_info = __get_ticker_info(t)
            new_assets['TICKER'].append(t)
            new_assets['Sector'].append(ticker_info[0])
            new_assets['Industry'].append(ticker_info[1])
            new_assets['ShortName'].append(ticker_info[2])
            new_assets['LongName'].append(ticker_info[3])
        except:
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


if __name__ == "__main__":
    main()
