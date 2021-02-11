import pandas as pd 
import numpy as np
from tqdm import tqdm
from .file_handler import *

class Manager: 
    def __init__(self, dataset_dir):
        self.dir = dataset_dir 
        self.latest_date = None
        self.btc_data = file_handler(dataset_dir, "btc")
        self.b3_data = file_handler(dataset_dir, "b3")
        self.fund_data = file_handler(dataset_dir, "fundamentus")

    def append_data(self, df):
        pass

    def random_wait(self):
        wait_times = [0.2, 0.5, 1, 2]
        probs = [0.3, 0.4, 0.2, 0.1 ]
        choice = np.random.choice(wait_times, size=1, p=probs)
        return choice
