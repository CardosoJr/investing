from dateutil import parser as dparse
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import yaml 
from pathlib import Path
import requests
import json 
from datetime import datetime

class TwitterExtractor: 
    def __init__(self, config_path):
        with open(Path("config.yaml"), 'r') as f: 
            config = yaml.safe_load(f)

        self.bearer_token = config['twitter_token']
        self.all_url = "https://api.twitter.com/2/tweets/search/all"
        self.recent_url = "https://api.twitter.com/2/tweets/search/recent"
    
    def __create_headers(self, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    def __connect_to_endpoint(self, url, headers, params, encoding = "ISO-8859-1"):
        response = requests.request("GET", url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def extract(self, from_date, to_date, filters, max_count = None):
        params = {
            "query": fr"(\{filters}) (lang:pt)",
            "tweet.fields": "created_at,lang,author_id,public_metrics",
        }

        if max_count:
            params['max_results'] = str(max_count)
        if from_date:
            params["start_time"] = from_date
        if to_date:
            params["end_time"] = to_date

        headers = self.__create_headers(self.bearer_token)
        json_response = self.__connect_to_endpoint(self.recent_url, headers, params)

        for d in json_response['data']:
            d['reply_count'] = d['public_metrics']['reply_count']
            d['like_count'] = d['public_metrics']['like_count']
            d['quote_count'] = d['public_metrics']['quote_count']
            d['retweet_count'] = d['public_metrics']['retweet_count']
            d.pop('public_metrics', 'None')

        df = pd.DataFrame(json_response['data'])

        def clean_dt(tweet):
            if "+" in tweet["created_at"]:
                s_datetime = tweet["created_at"].split(" +")[0]
            else:
                s_datetime = datetime.strptime(tweet["created_at"].split(".")[0], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

            return s_datetime

        func = np.vectorize(clean_dt)
        df['created_at'] = pd.to_datetime(df['created_at'].apply(func))

        return df
        