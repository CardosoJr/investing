
import news.gnews.gnews_api as gnews
import news.reddit.reddit_extractor as reddit
import news.twitter.twitter_extractor as twitter
import news.text_processing as tp 
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
    def __init__(self, project_dir, assets):
        self.path = Path(__file__).parent
        self.dir = Path(project_dir)
        self.assets_types = assets
        self.news_api = gnews.NewsExtractor()
        self.reddit_api = reddit.RedditExtractor()
        self.twitter_api = twitter.TwitterExtractor()

        with open(self.dir / "b3/config.json", 'r') as f:
            config = json.load(f)
        ticker_list = config["TICKERS"]
        assets_df = pd.read_csv(self.dir / "b3_history/assets.csv")
        assets_df = assets_df[assets_df['TICKER'].isin(ticker_list)]
        self.tickers = dict(zip(self.assets['TICKER'].ravel(), self.assets["ShortName"].ravel()))

        asset_config = {
            "news" : ('week', 'month'),
            "twitter"  : ('week', 'month'),
            "reddit" :('week', 'month'),
        }

        self.manager = Manager(project_dir, asset_config) ## TODO: develop text manager (saving text to better storage system)
        self.model = tp.SentimentPipeline()

    def run(self, baseline_date = None, max_date = None):
        print("Extracting news data \n")
        if max_date is None:
            self.now = datetime.now()
        else:
            self.now = max_date + relativedelta(days = 1)

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
        for ticker, name in tqdm(self.tickers.items()):
            data = []
            df = pd.DataFrame([])
            if asset == "news": 
                df = self.news_api.extract_daily(date, name)
                df = df.rename(columns = {"published date" : "DATE"})
            elif asset == "twitter":
                pass
            elif asset == "reddit":
                pass
            else:
                raise Exception("Asset not implemented: " + asset)

            if len(df) > 0:
                df['TICKER'] = [ticker] * len(df)
                data.append(df)

        return self.process(asset, pd.concat(data, ignore_index = True))

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
            full_text = df['title'] + ". " + df['description']
        elif asset == "twitter":
            pass
        elif asset == "reddit":
            pass
        else:
            raise Exception("Asset not implemented: " + asset)
        df[['Negative', "Neutral", "Positive"]] = self.model.process(full_text)
        return df

    def __preprocess_text(self, text):
        text = tp.clean_text(text)
        return text

    def __calculate_polarity(self, text):
        scores = tp.PredictSentimentScores(text)
        return dict(zip(["Negative", "Neutral", "Positive"], scores))

    def __translate_text(self, text):
        return tp.TranslatePt2En(text)


