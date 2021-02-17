from time import sleep
import pandas as pd 
import yaml
import sys
import numpy as np
from .fundamentus import fundamentus, advfn
from .b3 import b3
from .fundos import fundos
from .cripto import binance_api
from .DSManager import Manager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from tqdm import tqdm

class DailyExtractor:
    def __init__(self, baseline = None):
        self.baseline = baseline
        self.path = Path(__file__).parent
        pass

    def run(self):
        print("Extracting Intraday data")
        pass