from lxml.html import fragment_fromstring
from collections import OrderedDict
from decimal import Decimal
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

from pandas.core import base

def get_data(start_date, end_date, filter = None):
  base_url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_[ANOMES].csv"


  initial = datetime.strptime(start_date, "%Y-%m-%d")
  final = datetime.strptime(end_date, "%Y-%m-%d") 
  
  anomes = []
  while initial <= final:
      week = initial.strftime("%Y%W")
      anomes.append(initial.strftime("%Y%m"))
      initial = initial + relativedelta(months = 1)

  result = pd.DataFrame([])
  for am in anomes:
    url = base_url.replace("[ANOMES]", am)
    df = pd.read_csv(url)
    
    if filter is not None:
      df = df[df['CNPJ_FUNDO'].isin(filter)]

    result = result.append(df)

  return result