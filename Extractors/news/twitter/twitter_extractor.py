from dateutil import parser as dparse
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import yaml 
from pathlib import Path

"""
Extract 

tweet[0] -> Tweet message
        tweet[1] -> Number of retweets, basically our measure of how important/wide-spread this tweet is
        tweet[2] -> Tweet timestamp, when the tweet was given

"""

class TwitterExtractor: 
    def __init__(self):
        pass

    