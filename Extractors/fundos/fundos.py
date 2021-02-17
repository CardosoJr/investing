from lxml.html import fragment_fromstring
from collections import OrderedDict
from decimal import Decimal
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
from tqdm import tqdm

from pandas.core import base

def get_info_cadastral():
  #http://dados.cvm.gov.br/dataset/fi-cad
  #http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv
  base_url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
  df = pd.read_csv(base_url, sep = ';', encoding = "ISO-8859-1")
  return df


def get_data(start_date, end_date, filter = None, min_cot = None):
  base_url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_[ANOMES].csv"

  initial = datetime.strptime(start_date, "%Y-%m-%d")
  final = datetime.strptime(end_date, "%Y-%m-%d") 
  
  anomes = []
  while initial <= final:
      anomes.append(initial.strftime("%Y%m"))
      initial = initial + relativedelta(months = 1)

  result = pd.DataFrame([])
  for am in tqdm(anomes):
    url = base_url.replace("[ANOMES]", am)
    try:
      df = pd.read_csv(url, sep = ';')
      
      if filter is not None:
        df = df[df['CNPJ_FUNDO'].isin(filter)]

      if min_cot is not None:
        df = df[df["NR_COTST"] >= min_cot]

      result = result.append(df)
    except:
      print('Could not get data for', am, "\n")

  info = get_info_cadastral()
  info = info[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', "CLASSE", "FUNDO_EXCLUSIVO", "TAXA_PERFM", "TAXA_ADM", "INF_TAXA_PERFM", "INVEST_QUALIF"]]
  result = pd.merge(left = result, right = info, how = 'left', on = "CNPJ_FUNDO")
  result = result[result['SIT'] == "EM FUNCIONAMENTO NORMAL"]
  result = result[result['FUNDO_EXCLUSIVO'] == "N"]

  return result