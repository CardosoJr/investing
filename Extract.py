from .Extractors.DSManager import Manager
import pandas as pd 
import yaml
import sys
from .Extractors.fundamentus import fundamentus, advfn
from .Extractors.b3 import b3
from .Extractors.cripto import binance_api

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


