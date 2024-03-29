from lxml.html import fragment_fromstring
from collections import OrderedDict
from decimal import Decimal
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
from tqdm import tqdm

from pandas.core import base
from yahooquery import ticker
from pathlib import Path

def get_info_cadastral():
  #http://dados.cvm.gov.br/dataset/fi-cad
  #http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv
  base_url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
  df = pd.read_csv(base_url, sep = ';', encoding = "ISO-8859-1")
  return df

def get_b3_fundos_tickers(path):
  path = Path(path)
  fiis = pd.read_csv(path / "fiis.csv", sep = ";", header = None)
  etfs = pd.read_csv(path / "etfs.csv", sep = ";", header = None)
  # bdrs = pd.read_csv(path / "bdrs.csv", sep = ",", encoding = "ISO-8859-1")

  tickers = []

  tickers.extend((fiis[3] + "11").ravel())
  tickers.extend((etfs[3] + "11").ravel())
  # tickers.extend(bdrs['CODIGO'].ravel())

  return tickers


def get_data(start_date, end_date, filter = None, min_cot = None):
  base_url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_[ANOMES].csv"

  initial = datetime.strptime(start_date, "%Y-%m-%d")
  final = datetime.strptime(end_date, "%Y-%m-%d") 
  
  anomes = []
  while int(initial.strftime("%Y%m")) <= int(final.strftime("%Y%m")):
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

  if result is None or len(result) == 0:
    return pd.DataFrame([])

  info = get_info_cadastral()
  info = info[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', "CLASSE", "FUNDO_EXCLUSIVO", "TAXA_PERFM", "TAXA_ADM", "INF_TAXA_PERFM", "INVEST_QUALIF"]]
  result = pd.merge(left = result, right = info, how = 'left', on = "CNPJ_FUNDO")
  result = result[result['SIT'] == "EM FUNCIONAMENTO NORMAL"]
  result = result[result['FUNDO_EXCLUSIVO'] == "N"]

  return result