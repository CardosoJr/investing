import pandas as pd 
import numpy as np
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import squarify # pip install squarify (algorithm for treemap)&lt;/pre&gt;
import seaborn as sns 

class FinanceAnalysis:
    def __init__(self, path_dir):
        self.path = Path(path_dir)
        self.port = pd.read_excel(self.path / "portfolio.xlsx")
        self.__preprocess_portfolio()

    def __preprocess_portfolio(self):
        self.port['TICKER'] = np.where(self.port['TYPE'] == "FII" or 
                                                        self.port['TYPE'] == "STOCK" or
                                                        self.port['TYPE'] == "BDR" or 
                                                        self.port['TYPE'] == "ETF", 
                                       self.port['TICKER'] + ".SA", self.port['TICKER'])

        self.port['TICKER'] == np.where(self.port['TYPE'] == "CRIPTO", np.port['TICKER'] + "-BTC", self.port['TICKER'])


    def daily_view(self):
        pass

    def treemap(self, data, performance, labels):
        # Create a dataset:
        my_values=[i**3 for i in range(1,100)]
        
        # create a color palette, mapped to these values
        cmap_reds = matplotlib.cm.Reds
        mini=min(my_values)
        maxi=max(0.0)
        norm_reds = matplotlib.colors.Normalize(vmin=mini, vmax=maxi)

        cmap_greens = matplotlib.cm.Greens
        mini=min(0.0)
        maxi=max(my_values)
        norm_greens = matplotlib.colors.Normalize(vmin=mini, vmax=maxi)

        colors = [cmap_reds(norm_reds(value)) if x <= 0 else cmap_greens(norm_greens(value))  for value in my_values]
        
        labels = [x + "\n" + str(performance[i]) for x,i in enumerate(labels)]

        # Change color
        squarify.plot(sizes=my_values, labels = labels, alpha=.8, color=colors )
        plt.axis('off')
        plt.show()

    