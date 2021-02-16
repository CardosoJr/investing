import yaml 
import pandas as pd
import alphavantage
import binance_api
from tqdm import tqdm

def find_top_cripto():
    api = binance_api.Binance("config.yaml")
    ranking = pd.read_csv('digital_currency_list.csv')['currency code'].ravel()
    results = {}

    for code in tqdm(ranking):
        try:
            df = api.get_binance_data(code + "USDC", "1d", "08 Feb 2021", "12 Feb 2021")
            results[code] = df['trades'].mean()
        except: 
            print('could get data from', code)

    result_df = pd.DataFrame.from_dict(results, orient = 'index', columns = ['volume']).reset_index()
    result_df.to_csv('cripto_rankings.csv', index = False)

    return result_df

def run():
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)

    api = alphavantage.Alphavantage(config['key'])
    result = pd.DataFrame([])

    ranking = pd.read_csv('digital_currency_list.csv').sort_values(by = 'Ranking').head(100)['currency code']

    for code in ranking:
        print('reading', code)
        try:
            df = api.get_cripto_data(code, 'usd')
        except:
            print("could not load currency", code)
        df['code'] = code
        result = result.append(df)

    result.to_csv('data.csv')

