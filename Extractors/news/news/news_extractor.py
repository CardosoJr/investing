from itertools import count
from timeit import default_timer as timer
import os, time
import xml.etree.ElementTree as ET
import aiohttp
import asyncio
from datetime import date, datetime, timedelta
import csv
import json
import numpy as np
import nltk
import pytz
from nltk.sentiment import SentimentIntensityAnalyzer

## TODO: Get from securities files (crypto, stocks, ETFS....)
keywords = {
    'XRP': ['ripple', 'xrp', 'XRP', 'Ripple', 'RIPPLE'],
    'BTC': ['BTC', 'bitcoin', 'Bitcoin', 'BITCOIN'],
    'XLM': ['Stellar Lumens', 'XLM'],
    #'BCH': ['Bitcoin Cash', 'BCH'],
    'ETH': ['ETH', 'Ethereum'],
    'BNB' : ['BNB', 'Binance Coin'],
    'LTC': ['LTC', 'Litecoin']
    }

## TODO: get file from parameters. Need to specify which feed (crpyto, stocks ...)
with open('Crypto feeds.csv') as csv_file:

    # open the file
    csv_reader = csv.reader(csv_file)

    # remove any headers
    next(csv_reader, None)

    # create empty list
    feeds = []

    # add each row cotaining RSS url to feeds list
    for row in csv_reader:
        feeds.append(row[0])



# Make headlines global variable as it should be the same across all functions
headlines = {'source': [], 'title': [], 'pubDate' : [] }

async def get_feed_data(session, feed, headers):
    '''
    Get relevent data from rss feed, in async fashion
    :param feed: The name of the feed we want to fetch
    :param headers: The header we want on the request
    :param timeout: The default timout before we give up and move on
    :return: None, we don't need to return anything we append it all on the headlines dict
    '''
    try:
        async with session.get(feed, headers=headers, timeout=60) as response:
            # define the root for our parsing
            text = await response.text()
            root = ET.fromstring(text)

            channel = root.find('channel/item/title').text
            pubDate = root.find('channel/item/pubDate').text
            # some jank to ensure no alien characters are being passed
            title = channel.encode('UTF-8').decode('UTF-8')

            # convert pubDat to datetime
            published = datetime.strptime(pubDate.replace("GMT", "+0000"), '%a, %d %b %Y %H:%M:%S %z')
            # calculate timedelta
            time_between = datetime.now(pytz.utc) - published

            #print(f'Czas: {time_between.total_seconds() / (60 * 60)}')

            if time_between.total_seconds() / (60 * 60) <= HOURS_PAST:
                # append the source
                headlines['source'].append(feed)
                # append the publication date
                headlines['pubDate'].append(pubDate)
                # append the title
                headlines['title'].append(title)
                print(channel)

    except Exception as e:
        # Catch any error and also print it
        print(f'Could not parse {feed} error is: {e}')


async def get_headlines():
    '''
    Creates a an async task for each of our feeds which are appended to headlines
    :return: None
    '''
    # add headers to the request for ElementTree. Parsing issues occur without headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0'
    }

    # A nifty timer to see how long it takes to parse all the feeds
    start = timer()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for feed in feeds:
            task = asyncio.ensure_future(get_feed_data(session, feed, headers))
            tasks.append(task)

        # This makes sure we finish all tasks/requests before we continue executing our code
        await asyncio.gather(*tasks)
    end = timer()
    print("Time it took to parse feeds: ", end - start)


def categorise_headlines():
    '''arrange all headlines scaped in a dictionary matching the coin's name'''
    # get the headlines
    asyncio.run(get_headlines())
    categorised_headlines = {}

    # this loop will create a dictionary for each keyword defined
    for keyword in keywords:
        categorised_headlines['{0}'.format(keyword)] = []

    # keyword needs to be a loop in order to be able to append headline to the correct dictionary
    for keyword in keywords:

        # looping through each headline is required as well
        for headline in headlines['title']:
            # appends the headline containing the keyword to the correct dictionary
            if any(key in headline for key in keywords[keyword]):
                categorised_headlines[keyword].append(headline)

    return categorised_headlines

def analyse_headlines():
    '''Analyse categorised headlines and return NLP scores'''
    sia = SentimentIntensityAnalyzer()
    categorised_headlines = categorise_headlines()

    sentiment = {}

    for coin in categorised_headlines:
        if len(categorised_headlines[coin]) > 0:
            # create dict for each coin
            sentiment['{0}'.format(coin)] = []
            # append sentiment to dict
            for title in categorised_headlines[coin]:
                sentiment[coin].append(sia.polarity_scores(title))

    return sentiment

def compile_sentiment():
    '''Arranges every compound value into a list for each coin'''
    sentiment = analyse_headlines()
    compiled_sentiment = {}

    for coin in sentiment:
        compiled_sentiment[coin] = []

        for item in sentiment[coin]:
            # append each compound value to each coin's dict
            compiled_sentiment[coin].append(sentiment[coin][sentiment[coin].index(item)]['compound'])

    return compiled_sentiment

def compound_average():
    '''Calculates and returns the average compoud sentiment for each coin'''
    compiled_sentiment = compile_sentiment()
    headlines_analysed = {}

    for coin in compiled_sentiment:
        headlines_analysed[coin] = len(compiled_sentiment[coin])

        # calculate the average using numpy if there is more than 1 element in list
        compiled_sentiment[coin] = np.array(compiled_sentiment[coin])

        # get the mean
        compiled_sentiment[coin] = np.mean(compiled_sentiment[coin])

        # convert to scalar
        compiled_sentiment[coin] = compiled_sentiment[coin].item()

    return compiled_sentiment, headlines_analysed
    
def sanitise_tweet(some_string):
    """
    Removes links and special characters
    from a given string
    """
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w +: /  / \S +)", " ", some_string).split())


def strip_punctuation(some_string):
    """
    Removes punctuation from a given string
    """
    some_string = some_string.replace("'", " '")
    translator = str.maketrans('', '', string.punctuation)
    return some_string.translate(translator)

    ## TODO:
    ## Using text blob to separate sentences (??)
    """ 
    https://github.com/alvarobartt/twitter-stock-recommendation/blob/master/main.py
    blob = TextBlob(tw)
        polarity = 0
        for sentence in blob.sentences:
            polarity += sentence.sentiment.polarity
            global_polarity += sentence.sentiment.polarity


    Some filters and clean method
    https://github.com/shirosaidev/stocksight/blob/master/sentiment.py





    """
    