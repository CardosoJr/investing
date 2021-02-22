import pandas as pd 
import numpy as np
from pathlib import Path

class FinanceAnalysis:
    def __init__(self, path_dir):
        self.path = Path(path_dir)
        self.df = pd.read_excel(self.path / "portfolio.xlsx")
        
    def daily_view(self):
        pass
    