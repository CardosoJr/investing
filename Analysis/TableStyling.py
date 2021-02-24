import pandas as pd 
import seaborn as sns 
import matplotlib.pyplot as plt 
from matplotlib import colors
import matplotlib   

class Table:
    def __init__(self, df):
        self.df = df 
        pass


    def make_tickers_hyperlinks(self, ticker_col = 'TICKER'):
        self.df.style.format({'TICKER': self.__make_clickable_both})

    def __make_clickable_both(self, val): 
        url = f"https://finance.yahoo.com/quote/{val}?p={val}"
        return f'<a href="{url}">{val}</a>'

    def hide_index(self):
        self.df.hide_index()

    def percent_format(self):
        self.df.format("{:.2%}")

    def money_format(self):
        self.df.format('${0:,.0f}')

    def get_format_dict(self):
        format_dict = {'sum':'${0:,.0f}', 'date': '{:%m-%Y}', 'pct_of_total': '{:.2%}'}
        self.df.style.format(format_dict)

    def bar_plot(self):
        self.df.style.bar(color= 'lightblue')
        self.df.style.bar(color=['#FFA07A','skyblue'], vmin=0, subset=['quantity'], align='zero')


    def set_css_properties(self):
        # Set CSS properties for th elements in dataframe
        th_props = [('font-size', '12px'),
                    ('text-align', 'center'),
                    ('font-weight', 'bold'),
                    ('color', 'white'),
                    ('background-color', 'slategray')]
        # Set CSS properties for td elements in dataframe
        td_props = [('font-size', '14px')]
        # Set table styles
        styles = [dict(selector="th", props=th_props), dict(selector="td", props=td_props)]
        
        (self.df.head(10).style
        .applymap(None, subset=['quantity','unit price','ext price'])
        .format({'unit price': "${:.2f}",'ext price': "${:.2f}"})
        .set_table_styles(styles))

    def background_with_norm(self, s):
        cmap = matplotlib.cm.get_cmap('RdGn') # RdYlGn
        norm = matplotlib.colors.DivergingNorm(vmin=s.values.min(), vcenter=0, vmax=s.values.max())
        return ['background-color: {:s}'.format(matplotlib.colors.to_hex(c.flatten())) for c in cmap(norm(s.values))]

    def direction(self):
        self.df['direction'].map({0: '↓', 1: '↑'})


# %%HTML
# <style>.dataframe th{
# background:#3f577c; 
# font-family:monospace; 
# color:white; 
# border:3px solid white; 
# text-align:left !important;}
# </style>

import sparklines

def sparkline_str(x):
    bins=np.histogram(x)[0]
    sl = ''.join(sparklines(bins))
    return sl

sparkline_str.__name__ = "sparkline"


# https://pbpython.com/styling-pandas.html
# https://medium.com/@kristina.reut96/you-think-you-know-stylish-tables-ee59beadb487
# https://pandas.pydata.org/pandas-docs/stable/user_guide/style.html
# https://codepen.io/alassetter/pen/cyrfB
# https://codepen.io/websanity/pen/fxCwg
# https://codepen.io/njessen/pen/naLCv
# https://catalin.red/practical-css3-tables-with-rounded-corners/
# https://jasonet.co/posts/scheduled-actions/