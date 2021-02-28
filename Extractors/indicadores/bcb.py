import json 
import pandas as pd 
from datetime import datetime


bcb_series = {
    'ipca' : 433,
    'igpm' : 189,
    'selic' : 11,
    'meta_selic' : 432,
    'cdi' : 12,
    'pnad' : 24369,
    'cambio' : 1,
    'pib' : 4380
}


def get_data_bcb(series, start, end):
    now = datetime.now()
    if start is None:
        start = now

    if end is None:
        end = now

    if isinstance(start, str):
        start = datetime.strptime(start, "%Y-%m-%d")

    if isinstance(end, str):
        end = datetime.strptime(end, "%Y-%m-%d")

    if start > end:
        raise Exception("Start datetime is greater than end")

    dataInicial = start.strftime("%d/%m/%Y")
    dataFinal = end.strftime("%d/%m/%Y")

    if series.lower() in bcb_series.keys():
        codigo_serie = bcb_series[series.lower()]
    else:
        raise Exception("Series not found")

    url = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={dataInicial}&dataFinal={dataFinal}"

    return pd.read_json(url)
    