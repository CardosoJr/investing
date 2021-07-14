import Extractors.DailyExtractor as daily
from pathlib import Path
import pandas as pd 
from datetime import datetime
from argparse import ArgumentParser
import Extractors.fundamentus.fundamentus as fundamentus
import json


"""
Updating company fundamental data, ticker list and so on
"""

def main():
    pass
    parser = ArgumentParser()
    parser.add_argument("--mode", type = str, default = 'fundamental')
    parser.add_argument("--data_path", type = str, default = './DATA')
    args = parser.parse_args()
    path = Path(args.data_path)

    if args.mode == "fundamental":
        update_fundamental_data()
    elif args.mode == "ticker":
        update_ticker_list(path)

    else:
        print("Error - mode not found")

def update_fundamental_data():
    pass

def update_ticker_list(path):
    df = fundamentus.get_tickers(validate_tickers = True)
    valid_tickers = df['Papel'].unique().tolist()
    
    files_to_update = [path / "b3/config.json", 
                       path / "b3_history/config.json"]

    __update_files(files_to_update, valid_tickers)

def __update_files(files, valid_ticker_list):
    for f in files:
        with open(f, "r") as f:
            data = json.load(f)
        
        data['TICKERS'] = valid_ticker_list

        with open(f, "w") as f: 
            json.dump(data, f)


if __name__ == "__main__":
    main()
