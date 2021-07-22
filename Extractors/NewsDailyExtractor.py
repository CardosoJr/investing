
import Extractors.news.gnews.gnews_api as gnews
import Extractors.news.reddit.reddit_extractor as reddit
import Extractors.news.twitter.twitter_extractor as twitter
import Extractors.news.text_processing as tp 
import pandas as pd
import numpy as np 
from tqdm import tqdm 
from pathlib import Path 
from .DSManager import Manager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .DailyExtractor import DailyExtractor
import json

class NLPDailyExtractor(DailyExtractor): 
    def __init__(self, project_dir, assets, batch_size = 32):
        self.batch_size = batch_size
        self.path = Path(__file__).parent
        self.dir = Path(project_dir)
        self.assets_types = assets
        self.news_api = gnews.NewsExtractor()
        self.reddit_api = reddit.RedditExtractor("./config.yaml")
        self.twitter_api = twitter.TwitterExtractor("./config.yaml")

        with open(self.dir / "b3/config.json", 'r') as f:
            config = json.load(f)
        ticker_list = config["TICKERS"]
        assets_df = pd.read_csv(self.dir / "b3_history/assets.csv")
        assets_df = assets_df[assets_df['TICKER'].isin(ticker_list)]
        self.tickers = dict(zip(assets_df['TICKER'].ravel(), assets_df["ShortName"].ravel()))

        asset_config = {
            "news" : ('week', 'month'),
            "twitter"  : ('week', 'month'),
            "reddit" :('week', 'month'),
        }

        self.manager = Manager(project_dir, asset_config, id_col = "ID") ## TODO: develop text manager (saving text to better storage system)
        self.model = tp.SentimentPipeline()

    def run(self, baseline_date = None, max_date = None):
        print("Extracting news data \n")
        if max_date is None:
            self.now = datetime.now()
        else:
            self.now = max_date

        dates = {}
        if baseline_date is None:
            dates = self.manager.get_latest_dates()
        else:
            for asset in self.assets:
                dates[asset] = baseline_date

        for asset in self.assets_types:
            print("Extracting data from asset type:", asset)
            date = dates[asset]
            data = self.extract_daily_data(asset, date)
            if len(data) > 0:
                self.manager.append_data(data, asset)

    def extract_daily_data(self, asset, date):
        data = []
        for ticker, name in tqdm(self.tickers.items()):
            df = pd.DataFrame([])
            if asset == "news": 
                df = self.news_api.extract_daily(date, name)
                df2 = self.news_api.extract_daily(date, ticker.replace(".SA", ""))
                if len(df) > 0 and len(df2) > 0:
                    df = pd.concat([df, df2], ignore_index = True)
                elif len(df2) > 0:
                    df = df2

                if len(df) > 0:
                    df = df.rename(columns = {"published date" : "DATE"})
                    df['DATE'] = pd.to_datetime(df['DATE'])
                    df['ID'] = df["title"] + df["publisher"]
                    df['ID'] = df["ID"].str.replace(" ", "")
                    df = df.drop_duplicates(subset = ['ID', 'DATE'])
            elif asset == "twitter":
                df = self.twitter_api.extract_twint(from_date = date, to_date = self.now, filters = name) 
                df2 = self.twitter_api.extract_twint(from_date = date, to_date = self.now, filters = name) 
                if len(df) > 0 and len(df2) > 0:
                    df = pd.concat([df, df2], ignore_index = True)
                elif len(df2) > 0:
                    df = df2
                if len(df) > 0:
                    df = df.rename(columns = {'date' : 'DATE', 'id' : "ID"})
                    df = df.drop_duplicates(subset = ['ID', 'DATE'])
            elif asset == "reddit":
                pass
            else:
                raise Exception("Asset not implemented: " + asset)

            if len(df) > 0:
                df['TICKER'] = [ticker] * len(df)
                data.append(df)

        if len(data) > 0:
            result = pd.concat(data, ignore_index = True)
            # result.to_csv(self.dir / "b3_history/teste_news.csv", index = False)
            return self.process(asset, result)
        else:
            return pd.DataFrame([])

    def process(self, asset, df):
        def clean_text(text):
            return self.__preprocess_text(text)
        func = np.vectorize(clean_text)
        full_text = []
        if asset == "news":
            df['title'] = df['title'].apply(func)
            df['description'] = df['description'].apply(func)
            if "full_text" in df.columns:
                df['full_text'] = df['full_text'].apply(func)
            full_text_series = df['title'] + ". " + df['description']
            full_text = full_text_series.ravel().tolist()
        elif asset == "twitter":
            pass
        elif asset == "reddit":
            pass
        else:
            raise Exception("Asset not implemented: " + asset)

        result = None 
        num_groups = len(full_text) // self.batch_size         
        for group in tqdm(np.array_split(full_text, num_groups)):
            predictions = self.model.process(group.tolist())

            if result is None:
                result = predictions
            else: 
                result = np.append(result, predictions, axis = 0)                

        df[['Negative', "Neutral", "Positive"]] = result
        return df

    def __preprocess_text(self, text):
        text = tp.clean_text(text)
        return text