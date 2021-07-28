import praw
import matplotlib.pyplot as plt
import math
import datetime as dt
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import re

def get_avg_sentiment(sentiment):
    """
    Compiles and returnes the average sentiment
    of all titles and bodies of our query
    """
    average = {}

    for coin in sentiment:
        # sum up all compound readings from each title & body associated with the
        # coin we detected in keywords
        average[coin] = sum([item['compound'] for item in sentiment[coin]])

        # get the mean compound sentiment if it's not 0
        if average[coin] != 0:
            average[coin] = average[coin] / len(sentiment[coin])

    return average


def analyse_posts(posts):
    """
    analyses the sentiment of each post with a keyword
    """
    sia = SentimentIntensityAnalyzer()
    sentiment = {}
    for post in posts:
        if posts[post]['coin'] not in sentiment:
            sentiment[posts[post]['coin']] = []

        sentiment[posts[post]['coin']].append(sia.polarity_scores(posts[post]['title']))
        sentiment[posts[post]['coin']].append(sia.polarity_scores(posts[post]['body']))

    return sentiment

def find_keywords(posts, keywords):
    """
    Checks if there are any keywords int he posts we pulled
    Bit of a mess but it works
    """
    key_posts = {}

    for post in posts:
        for key in keywords:
            for item in keywords[key]:
                if item in posts[post]['title'] or item in posts[post]['body']:
                    key_posts[post] = posts[post]
                    key_posts[post]['coin'] = key

    return key_posts

def compare_posts(fetched, stored):
    """
    Checks if there are new posts
    """
    i=0
    for post in fetched:
        if not fetched[post] in [stored[item] for item in stored]:
            i+=1

    return i

def get_post():
    """
    Returns relevant posts based the user configuration
    """
    posts = {}
    for sub in config['SUBREDDITS']:
        subreddit = reddit.subreddit(sub)
        relevant_posts = getattr(subreddit, config['SORT_BY'])(limit=config['NUMBER_OF_POSTS'])
        for post in relevant_posts:
            if not post.stickied:
                posts[post.id] = {"title": post.title,
                                  "subreddit": sub,
                                  "body": post.selftext,

                                  }
    return posts

class RedditExtractor:
    def __init__(self, config_path):
        with open(Path(config_path), 'r') as f: 
            config = yaml.safe_load(f) 

        self.reddit = praw.Reddit(client_id = config['reddit_client_id'],
                    client_secret = config['reddit_secret'],
                    user_agent = config['reddit_user_agent'])

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



"""
  BTC:
    - bitccoin
    - BTC
    - btc
    - BITCOIN
    - Bitcoin
  ETH:
    - Ethereum
    - Eth
    - ETH


      SUBREDDITS:
    - Cryptocurrency
    - crypto_currency
    - cryptocurrencies
    - worldnews

"""