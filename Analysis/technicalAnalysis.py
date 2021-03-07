import pandas as pd
from ta import add_all_ta_features
from ta.utils import dropna


class TAKpis:
    def __init__(self, date_col = 'DATE', ticker_col = 'TICKER'):
        self.date_col = date_col
        self.ticker_col = ticker_col

    def process_kpis(self, df):
        df = df.sort_values(by = [self.ticker_col, self.date_col])
        dfs = []
        for _, df_t in df.groupby(self.ticker_col):
            dfs.append(add_all_ta_features(df_t, open="open", high="high", low="low", close="close", volume="Volume"))
                
        result = df = pd.concat(dfs, ignore_index = True)

        return result



