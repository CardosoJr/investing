import yaml 
import pandas as pd
import alphavantage

def run():
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)

    api = alphavantage.Getter(config['key'])
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

