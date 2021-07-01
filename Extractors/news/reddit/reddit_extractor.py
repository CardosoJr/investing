import praw
import matplotlib.pyplot as plt
import math
import datetime as dt
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import re

class Reddit:
    def __init__(self, config_path):
        with open(Path(config_path), 'r') as f: 
            config = yaml.safe_load(f) 

        self.reddit = praw.Reddit(client_id = config['client_id'],
                    client_secret = config['secret'],
                    user_agent = config['user_agent'])

    def __extract_post_info(self):
        pass

    def read_all_recent(self, baseline_date):
        pass

    def find_tickers(self, submission):
        ls_text = list()
        ls_text.append(submission.selftext)
        ls_text.append(submission.title)

        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            ls_text.append(comment.body)

        l_tickers_found = list()
        for s_text in ls_text:
            for s_ticker in set(re.findall(r"([A-Z]{3,5} )", s_text)):
                l_tickers_found.append(s_ticker.strip())

        return l_tickers_found