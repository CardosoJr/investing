import pandas as pd 
import numpy as np
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns 
import plotly.express as px
from .Analysis import TableStyling
from .Extracotrs.b3 import b3
from .Extractors import DSManager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go

class CurrentViewAnalysis:
    """
        Builds Current Market View with Porfolio
    """

    def __init__(self, path_dir):
        self.path = Path(path_dir)
        self.port = pd.read_excel(self.path / "portfolio.xlsx")
        self.__preprocess_portfolio()
        self.manager = DSManager.Manager("../DATA/")
        self.now = datetime.now()
        self.b3_api = b3.B3()
        self.daily_view()

    def __preprocess_portfolio(self):
        self.port['TICKER'] = np.where(self.port['TYPE'] == "FII" or 
                                                        self.port['TYPE'] == "STOCK" or
                                                        self.port['TYPE'] == "BDR" or 
                                                        self.port['TYPE'] == "ETF", 
                                       self.port['TICKER'] + ".SA", self.port['TICKER'])

        self.port['TICKER'] == np.where(self.port['TYPE'] == "CRIPTO", np.port['TICKER'] + "-BTC", self.port['TICKER'])


    def __get_fundamentals(self):
        self.fundamentals = {}
        self.fundamentals['b3'] = pd.read_csv(path / "b3" / "assets.csv")
        self.fundamentals['b3_funds'] = pd.read_csv(path / "b3_funds" / "assets_fundos_b3.csv")

    def __get_kpis(self):
        pass

    def __get_latest_price(self):
        self.recent_results = self.manager.read_data_all(self.now + relativedelta(months = -6), self.now)
        self.history = {}
        self.history['b3'] = pd.read_csv(path / "b3" / "history.csv")
        self.history['b3_funds'] = pd.read_csv(path / "b3_funds" / "fundos_b3_history.csv")
        self.history['cripto'] = pd.read_csv(path / "cripto" / "cripto_history.csv")
        self.history['funds'] = pd.read_csv(path / "funds" / "fundos_history.csv")

    def __get_rt_price(self):
        b3_tickers = self.port[self.port['TYPE'] != "FUNDS"]['TICKER'].ravel()
        results = {}
        for ticker in b3_tickers:
            try:
                result[ticker] = self.b3_api.Get_Real_Time_Quote(ticker)
            except:
                result[ticker] = np.nan
                print("could not get rt price from", ticker, "\n")
        funds_tickers = self.port[self.port['TYPE'] == "FUNDS"]['TICKER'].ravel()
        price = self.recent_results['funds'][self.recent_results['funds']['TICKER'].isin(funds_tickers)]
        max_dt = price['DATE'].max()
        latest_price = price[price['DATA'] == max_dt][['TICKER', "PRICE"]]
        latest_price = latest_price.set_index('TICKER')['PRICE'].to_dict()
        result = dict(**result, **latest_price)
        self.port['RT_PRICE'] = self.port['TICKER'].apply(lambda x: result[x])

    def __process_port(self):
        # Calculating metrics per ticket
        gpr = self.port.groupby('TICKER')
        new_df = pd.DataFrame([])
        new_df['TOTAL'] = gpr['TOTAL'].sum()
        new_df['PRICE'] = gpr['PRICE'].multiply(gpr['QUANTITY'])).sum() / gpr['QUANTITY'].sum() # calculating mean price as current
        new_df['ASSET'] = gpr['ASSET'].first()
        new_df['TYPE'] = gpr['TYPE'].first()
        new_df['QUANTITY'] = gpr['QUANTITY'].sum()
        self.port = new_df.reset_index()

        self.port['REPRESENTATION'] = self.port['TOTAL'] / self.port['TOTAL'].sum()
        self.port['CHG'] = (self['RT_PRICE'] - self['PRICE']) / self['PRICE']
        self.port['CHG_VOL'] = self.port['RT_PRICE'] - self.port['PRICE'])
        self.port['CURRENT_TOTAL'] = (self.['CHG'] + 1).multiply(self.port['TOTAL'])
        self.port['CURRENT_REPRESENTATION'] = self.port['CURRENT_TOTAL'] / self.port['CURRENT_TOTAL'].sum()

    def daily_view(self):
        self.__get_latest_price()
        self.__get_fundamentals()
        self.__get_rt_price()
        self.__process_port()

    def __build_hierarchical_dataframe(df, levels, value_column, color_column, aux_color_column):
        """
        Build a hierarchy of levels for Sunburst or Treemap charts.

        Levels are given starting from the bottom to the top of the hierarchy,
        ie the last level corresponds to the root.
        """
        df_all_trees = pd.DataFrame(columns=['id', 'parent', 'value', 'color'])
        for i, level in enumerate(levels):
            df_tree = pd.DataFrame(columns=['id', 'parent', 'value', 'color'])
            dfg = pd.DataFrame([])
            group = df.groupby(levels[i:])
            dfg[value_column] = group[value_column].sum()
            dfg[color_column] = (group[aux_color_column].sum() - group[value_column].sum()) / group[value_column].sum()
            dfg = dfg.reset_index()
            df_tree['id'] = dfg[level].copy()
            if i < len(levels) - 1:
                df_tree['parent'] = dfg[levels[i+1]].copy()
            else:
                df_tree['parent'] = 'total'
            df_tree['value'] = dfg[value_column]
            df_tree['color'] = dfg[color_column]
            df_all_trees = df_all_trees.append(df_tree, ignore_index=True)

        total = pd.Series(dict(id='total', parent='',
                                value = df[value_column].sum(),
                                color = (df[aux_color_column].sum() - df[value_column].sum()) / df[value_column].sum() ))
        df_all_trees = df_all_trees.append(total, ignore_index=True)
        return df_all_trees

    def plot_treemap(self, port):
        levels = ['ASSET', 'TYPE'] # levels used for the hierarchical chart
        color_column = 'CHG'
        aux_color_column = 'CURRENT_TOTAL'
        value_column = 'TOTAL'
        df_all_trees = self.__build_hierarchical_dataframe(df, levels, value_column, color_column, aux_color_column)
        trace = go.Treemap(
            textinfo = "label+percent root",
            labels=df_all_trees['id'],
            parents=df_all_trees['parent'],
            values=df_all_trees['value'],
            branchvalues='total',
            marker=dict(
                colors=df_all_trees['color'],
                colorscale='RdBu',
                cmid=0),
            hovertemplate='<b>%{label} </b> <br> Total: R$%{value:,.2f}<br> Chg: %{color:.2%}',
            name='')

        layout = go.Layout(autosize=False,
            width=1000,
            height=500,
            margin=go.layout.Margin(
                l=10,
                r=10,
                b=10,
                t=10,
                pad = 4
            ))

        fig = go.Figure(data = [trace], layout = layout)
        fig.show()


class PortfolioView(self):
    def __init__(self):
        pass

class AssetsView:
    def __init__(self):
        pass
    ]
class FundsView():
    def __init__(self):
        pass

class HistoryView:
    def __init__(self):
        pass