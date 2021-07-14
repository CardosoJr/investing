import Extractors.DailyExtractor as daily
from pathlib import Path
import pandas as pd 
from datetime import datetime
from argparse import ArgumentParser

def main():
    # arguments...
    parser = ArgumentParser()
    parser.add_argument("--data_path", type = str, default = './DATA')
    parser.add_argument("--date", type = str, default = None)
    parser.add_argument("--max_date", type = str, default = None)
    args = parser.parse_args()

    data_path = args.data_path
    path = Path(data_path)

    date = args.date # If None will consider last day in config files 
    max_date = args.max_date # If None will consider today 
    interval = "1m"

    if date is not None: 
        date = datetime.strptime(date, "%Y-%m-%d")
    if max_date is not None: 
        max_date = datetime.strptime(max_date, "%Y-%m-%d")

    assets = ["b3", "b3_funds", "cripto"]
    extractor = daily.DailyExtractor(data_path, assets = assets, interval = interval)
    extractor.run(baseline_date = date, max_date = max_date)

    assets = ["b3_history", "b3_funds_history", "cripto_history", "funds_history", "KPIs_history"]
    extractor = daily.DailyExtractor(data_path, assets = assets, interval = interval)
    extractor.run(baseline_date = date, max_date = max_date)

if __name__ == "__main__":
    main()