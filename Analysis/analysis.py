
import sys 
from pathlib import Path
sys.path.append(Path(__file__).parent)


import pandas as pd 
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns 
import plotly.express as px
from . import TableStyling
from Extractors.b3 import b3
from Extractors import DSManager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go

class CurrentViewAnalysis:
    """
        Builds Current Market View with Porfolio
    """
    def __init__(self, path_dir):
        self.path = Path(path_dir)
        self.portfolio = PortfolioView(path_dir)
        self.manager = DSManager.Manager(path_dir)
        self.now = datetime.now()
        self.b3_api = b3.B3()
        self.daily_view()

    def __get_fundamentals(self):
        self.fundamentals = {}
        self.fundamentals['b3'] = pd.read_csv(self.path / "b3" / "assets.csv")
        self.fundamentals['b3_funds'] = pd.read_csv(self.path / "b3_funds" / "assets_fundos_b3.csv")

    def __get_latest_price(self):
        self.recent_results = self.manager.read_data_all(self.now + relativedelta(months = -6), self.now)

    def __build_summary(self, asset):
        last_date =  self.recent_results[asset]['DATE'].max()
        self.recent_results[asset]
        pass

    def __build_features(self, data):
        data['DATE'] = pd.to_datetime(data['DATE'])
        last_date = data['DATE'].max()
        data = data.sort_values(by = ['TICKER', 'DATE'])
       
        data['previous_close'] = data.groupby('TICKER')['close'].shift(1)
        summary = data.groupby('TICKER').last().reset_index()
        summary['ch'] = summary['close'] - summary['previous_close']
        summary['%ch'] = summary['ch'] / summary['previous_close']
       
        ygroup = data[data['DATE'] >= last_date - relativedelta(weeks = 52)].groupby(['TICKER', pd.Grouper(key = 'DATE', freq ='52W')])
        maxClose = ygroup['close'].max().reset_index().drop(columns = ['DATE']).rename(columns = {'close' : '52w high'})
        minClose = ygroup['close'].min().reset_index().drop(columns = ['DATE']).rename(columns = {'close' : '52w low'})
        summary = pd.merge(left = summary, right = maxClose, on = 'TICKER', how = 'left')
        summary = pd.merge(left = summary, right = minClose, on = 'TICKER', how = 'left')
        summary['%52w high'] = summary['close'] / summary['52w high']
        summary['%52w low'] = summary['close'] / summary['52w low']

        return summary

    def __process_kpis(self, kpi):
        now  = datetime.now()
        kpi = kpi[kpi['DATE'] <= now]
        daily_rates = ['cdi', 'selic']
        monthly_rates = ['igpm', 'ipca']
        summary_kpis = kpi[kpi['TICKER'].isin(monthly_rates)].groupby(['TICKER', pd.Grouper(key = 'DATE', freq = 'M')])['close'].last().reset_index()
        daily = kpi[kpi['TICKER'].isin(daily_rates)].groupby(['TICKER', pd.Grouper(key = 'DATE', freq = 'M')])['close'].sum().reset_index()
        summary_kpis = summary_kpis.append(daily, ignore_index = True)
        summary_kpis['close'] = summary_kpis['close'] / 100.0   
        ibov = '^BVSP'
        igroup = kpi[kpi['TICKER'] == ibov].groupby(['TICKER', pd.Grouper(key = 'DATE', freq = 'M')])
        summary_kpis = summary_kpis.append(((igroup['close'].last() - igroup['close'].first()) / igroup['close'].first()).reset_index())
        return summary_kpis

    def daily_view(self):
        self.__get_latest_price()
        self.__get_fundamentals()
        self.portfolio.daily_view(self.recent_results)

class PortfolioView:
    """
        Analyze portfolio
    """
    def __init__(self, path_dir):
        self.path = Path(path_dir)
        self.port = pd.read_excel(self.path / "portfolio.xlsx")
        self.__preprocess_portfolio()
        self.now = datetime.now()
        self.b3_api = b3.B3()

    def __preprocess_portfolio(self):
        self.port['TICKER'] = np.where(self.port['TYPE'] == "FII" or 
                                                        self.port['TYPE'] == "STOCK" or
                                                        self.port['TYPE'] == "BDR" or 
                                                        self.port['TYPE'] == "ETF", 
                                       self.port['TICKER'] + ".SA", self.port['TICKER'])

        self.port['TICKER'] == np.where(self.port['TYPE'] == "CRIPTO", np.port['TICKER'] + "-BTC", self.port['TICKER'])

    def __get_rt_price(self, recent_results):
        self.recent_results = recent_results
        b3_tickers = self.port[self.port['TYPE'] != "FUNDS"]['TICKER'].ravel()
        results = {}
        for ticker in b3_tickers:
            try:
                results[ticker] = self.b3_api.Get_Real_Time_Quote(ticker)
            except:
                results[ticker] = np.nan
                print("could not get rt price from", ticker, "\n")
        funds_tickers = self.port[self.port['TYPE'] == "FUNDS"]['TICKER'].ravel()
        price = self.recent_results['funds'][self.recent_results['funds']['TICKER'].isin(funds_tickers)]
        max_dt = price['DATE'].max()
        latest_price = price[price['DATA'] == max_dt][['TICKER', "PRICE"]]
        latest_price = latest_price.set_index('TICKER')['PRICE'].to_dict()
        results = dict(**results, **latest_price)
        self.port['RT_PRICE'] = self.port['TICKER'].apply(lambda x: results[x])

    def __process_port(self):
        # Calculating metrics per ticket
        gpr = self.port.groupby('TICKER')
        new_df = pd.DataFrame([])
        new_df['TOTAL'] = gpr['TOTAL'].sum()
        new_df['PRICE'] = gpr['PRICE'].multiply(gpr['QUANTITY']).sum() / gpr['QUANTITY'].sum() # calculating mean price as current
        new_df['ASSET'] = gpr['ASSET'].first()
        new_df['TYPE'] = gpr['TYPE'].first()
        new_df['QUANTITY'] = gpr['QUANTITY'].sum()
        self.port = new_df.reset_index()

        self.port['REPRESENTATION'] = self.port['TOTAL'] / self.port['TOTAL'].sum()
        self.port['CHG'] = (self.port['RT_PRICE'] - self.port['PRICE']) / self.port['PRICE']
        self.port['CHG_VOL'] = self.port['RT_PRICE'] - self.port['PRICE']
        self.port['CURRENT_TOTAL'] = (self.port['CHG'] + 1).multiply(self.port['TOTAL'])
        self.port['CURRENT_REPRESENTATION'] = self.port['CURRENT_TOTAL'] / self.port['CURRENT_TOTAL'].sum()

    def daily_view(self, recent_results):
        self.__get_rt_price(recent_results)
        self.__process_port()

    def __build_hierarchical_dataframe(self, df, levels, value_column, color_column, aux_color_column):
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
        df_all_trees = self.__build_hierarchical_dataframe(self.port, levels, value_column, color_column, aux_color_column)
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

class AssetsView:
    def __init__(self):
        pass

class FundsView():
    def __init__(self):
        pass

class HistoryView:
    def __init__(self):
        pass


"""
TODO: 

Overview (value, change, risk - std, )
    - Renda fixa 
    - Regions (usa, eu, emerging mkts)
    - Indices
    - Commodities
    - Industries / segments

Portfolio: 
    - % in port / ticker / name / industry 
    - mkt cap / price / ch / 6 month insiders and value
    - % to 52 week high / % to 52 week low
    - fundamentals 

- insider trading

Pulse:
    - largest companies (mkt cap, daily change )

Biggest Movers
    - list of assets with biggest change day / week

Business (per year)
    - DRE data
    - cash flow / ... several years
    - fundamentals kpis 
    
Universe of Assets:
    - Portfolio
    - Tags 
    - Monitored / watchlist


"""