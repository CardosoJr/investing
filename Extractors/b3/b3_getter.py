import pandas as pd
from yahooquery import Ticker

class Getter:
    def Extract_Data(self, asset_id, interval = "5m"):
        ticker = Ticker(asset_id)
        data = ticker.history(period='2d', interval = interval)
        return data

    def Extract_History(self, asset_id, start, end):
        ticker = Ticker(asset_id)   
        data = ticker.history(period = 'max')
        return data

    def Financial_Info(self, asset_id):
        ticker = Ticker(asset_id)     
        data = ticker.income_statement()
        data = data.transpose()       
        data.columns = data.iloc[0,:] 
        data = data.iloc[2:,:-1]      
        data = data.iloc[:, ::-1]     
        return data