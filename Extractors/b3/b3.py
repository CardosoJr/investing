import pandas as pd
from yahooquery import Ticker

class B3:
    def Extract_Data(self, asset_id, start, end, interval):
        ticker = Ticker(asset_id)
        data = ticker.history(start = start, end = end, interval = interval)
        return data

    def Extract_History(self, asset_id):
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

    def Get_Summary(self, asset_id):
        return Ticker(asset_id).summary_detail[asset_id]

    def Get_Full_Real_Time_Quote(self, asset_id):
        ticker = Ticker(asset_id)   
        data = ticker.quotes
        return data

    def Get_Real_Time_Quote(self, asset_id):
        return self.Get_Full_Real_Time_Quote(asset_id)['regularMarketPrice']
