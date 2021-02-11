import re
import urllib.request
import urllib.parse
import http.cookiejar

from lxml.html import fragment_fromstring
from collections import OrderedDict
from decimal import Decimal

def get_data(*args, **kwargs):
    url = 'http://www.fundamentus.com.br/resultado.php'
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201'),
                         ('Accept', 'text/html, text/plain, text/css, text/sgml, */*;q=0.01')]

    # Aqui estão os parâmetros de busca das ações
    # Estão em branco para que retorne todas as disponíveis
    data = {'pl_min': '',
            'pl_max': '',
            'pvp_min': '',
            'pvp_max' : '',
            'psr_min': '',
            'psr_max': '',
            'divy_min': '',
            'divy_max': '',
            'pativos_min': '',
            'pativos_max': '',
            'pcapgiro_min': '',
            'pcapgiro_max': '',
            'pebit_min': '',
            'pebit_max': '',
            'fgrah_min': '',
            'fgrah_max': '',
            'firma_ebit_min': '',
            'firma_ebit_max': '',
            'margemebit_min': '',
            'margemebit_max': '',
            'margemliq_min': '',
            'margemliq_max': '',
            'liqcorr_min': '',
            'liqcorr_max': '',
            'roic_min': '',
            'roic_max': '',
            'roe_min': '',
            'roe_max': '',
            'liq_min': '',
            'liq_max': '',
            'patrim_min': '',
            'patrim_max': '',
            'divbruta_min': '',
            'divbruta_max': '',
            'tx_cresc_rec_min': '',
            'tx_cresc_rec_max': '',
            'setor': '',
            'negociada': 'ON',
            'ordem': '1',
            'x': '28',
            'y': '16'}

    with opener.open(url, urllib.parse.urlencode(data).encode('UTF-8')) as link:
        content = link.read().decode('ISO-8859-1')

    pattern = re.compile('<table id="resultado".*</table>', re.DOTALL)
    content = re.findall(pattern, content)[0]
    page = fragment_fromstring(content)
    result = OrderedDict()

    for rows in page.xpath('tbody')[0].findall("tr"):
        result.update({rows.getchildren()[0][0].getchildren()[0].text: {'Cotacao': todecimal(rows.getchildren()[1].text),
                                                                        'P/L': todecimal(rows.getchildren()[2].text),
                                                                        'P/VP': todecimal(rows.getchildren()[3].text),
                                                                        'PSR': todecimal(rows.getchildren()[4].text),
                                                                        'DY': todecimal(rows.getchildren()[5].text),
                                                                        'P/Ativo': todecimal(rows.getchildren()[6].text),
                                                                        'P/Cap.Giro': todecimal(rows.getchildren()[7].text),
                                                                        'P/EBIT': todecimal(rows.getchildren()[8].text),
                                                                        'P/ACL': todecimal(rows.getchildren()[9].text),
                                                                        'EV/EBIT': todecimal(rows.getchildren()[10].text),
                                                                        'EV/EBITDA': todecimal(rows.getchildren()[11].text),
                                                                        'Mrg.Ebit': todecimal(rows.getchildren()[12].text),
                                                                        'Mrg.Liq.': todecimal(rows.getchildren()[13].text),
                                                                        'Liq.Corr.': todecimal(rows.getchildren()[14].text),
                                                                        'ROIC': todecimal(rows.getchildren()[15].text),
                                                                        'ROE': todecimal(rows.getchildren()[16].text),
                                                                        'Liq.2meses': todecimal(rows.getchildren()[17].text),
                                                                        'Pat.Liq': todecimal(rows.getchildren()[18].text),
                                                                        'Div.Brut/Pat.': todecimal(rows.getchildren()[19].text),
                                                                        'Cresc.5anos': todecimal(rows.getchildren()[20].text)}})
    
    return result
    
def todecimal(string):
  string = string.replace('.', '')
  string = string.replace(',', '.')

  if (string.endswith('%')):
    string = string[:-1]
    return Decimal(string) / 100
  else:
    return Decimal(string)


import requests
import xlrd
import pandas as pd
import io
import os
import string
import random
from zipfile import ZipFile
from .utils import convert_type, DataNotFound


def get_tickers():
    '''Downloads tickers' codes from fundamentus website
    :Returns:
    A pandas DataFrame with three columns:
    Papel (Ticker),
    Nome Comercial (Trade Name),
    Razão Social (Corporate Name)
    '''
    headers = {
        'User-Agent': ''.join(
            random.choices(string.ascii_letters, k=10)
        )
    }
    html_src = requests.get(
        'http://fundamentus.com.br/detalhes.php', headers=headers).text
    return pd.read_html(html_src)[0]


def _get_sheets(ticker, quarterly, ascending):
    '''Downloads sheets from fundamentus website.
    available sheets:
        \'Bal. Patrim.\' for balance sheet
        \'Dem. Result.\' for income statement
    '''

    ticker = ticker.upper()

    # Apparently fundamentus is blocking requests library's standard user-agent
    # To solve this problem, I'm generating random user-agents

    headers = {
        'User-Agent': ''.join(
            random.choices(string.ascii_letters, k=10)
        )
    }

    r = requests.get('https://www.fundamentus.com.br/balancos.php',
                     params={'papel': ticker},
                     headers=headers)

    SID = r.cookies.values()[0]

    response_sheet = requests.get(
        'https://www.fundamentus.com.br/planilhas.php',
        params={'SID': SID}, headers=headers)

    if response_sheet.text.startswith('Ativo nao encontrado'):
        raise DataNotFound(
            f'Couldn\'t find any data for {ticker}')

    with io.BytesIO(response_sheet.content) as zip_bytes:
        with ZipFile(zip_bytes) as z:
            xls_bytes = z.read('balanco.xls')

    # Supress warnings
    wb = xlrd.open_workbook(file_contents=xls_bytes,
                            logfile=open(os.devnull, 'w'))

    dfs = {
        'Bal. Patrim.': pd.read_excel(wb, engine='xlrd',
                                      index_col=0,
                                      sheet_name='Bal. Patrim.'),
        'Dem. Result.': pd.read_excel(wb, engine='xlrd',
                                      index_col=0,
                                      sheet_name='Dem. Result.')
    }

    for sheet, df in dfs.items():

        # Cleaning the DataFrame
        df.columns = df.iloc[0, :]
        df = df.iloc[1:].T.applymap(convert_type)
        df.index.name = 'Data'
        df.columns.name = ticker
        df = df.loc[df.index.notnull()]
        df.index = pd.to_datetime(
            df.index, format='%d/%m/%Y')

        if not quarterly:
            rows_to_drop = [x for x in df.index.year
                            if list(df.index.year).count(x) != 4]
            df = df.groupby(df.index.year).sum()
            df.drop(rows_to_drop, inplace=True)
            df.index = [str(x) for x in df.index]

        dfs[sheet] = df.sort_index(ascending=ascending).astype(int)

    return dfs


def get_balanco(ticker, quarterly=False, ascending=True, separated=True):
    '''Get the balance sheet for one brazilian stock listed on fundamentus website.
    NOTE: Values are in thousands!
    :Arguments:
    ticker[str]:
        Ticker to download data from
    quarterly[bool]:
        Whether to download quarterly or annualy data.
        Default is False.
    ascending[bool]:
        Whether the date index should be sorted ascendingly on the DataFrame
        Default is True
    separated[bool]:
        If True, the DataFrame will be hierarchically divided by super columns:
            \'Ativo Total\' (Total Assets),
            \'Ativo Circulante\' (Current Assets),
            \'Ativo Não Circulante\' (Non-current Assets),
            \'Passivo Total\' (Total Liabilities),
            \'Passivo Circulante\' (Current Liabilities),
            \'Passivo Não Circulante\' (Non-current Liabilities),
            \'Patrimônio Líquido\' (Net Worth)
        Default is True (highly recommended, as some infra columns are
        duplicated which could lead to confusion).
    :Raises:
    DataNotFound(IndexError) if there's no data available for that ticker
    :Returns:
    A pandas DataFrame
    '''

    df = _get_sheets(ticker, quarterly=quarterly,
                     ascending=ascending)['Bal. Patrim.']

    if separated:
        super_cols = [
            'Ativo Total',
            'Ativo Circulante',
            'Ativo Não Circulante',
            'Ativo Realizável a Longo Prazo',
            'Passivo Total',
            'Passivo Circulante',
            'Passivo Não Circulante',
            'Passivo Exigível a Longo Prazo',
            'Patrimônio Líquido'
        ]

        cols = list(df.columns)

        # Handling different balance sheets for banks and other companies
        if 'Ativo Não Circulante' in cols:
            super_cols.remove('Ativo Realizável a Longo Prazo')
        else:
            super_cols.remove('Ativo Não Circulante')

        if 'Passivo Não Circulante' in cols:
            super_cols.remove('Passivo Exigível a Longo Prazo')
        else:
            super_cols.remove('Passivo Não Circulante')

        idxs = [cols.index(x) for x in cols if x in super_cols]

        slices = [slice(idxs[i], idxs[i + 1])
                  if i < (len(idxs) - 1)
                  else slice(idxs[i], None)
                  for i, _ in enumerate(idxs)]

        tuples = []

        for s in slices:
            sup = super_cols.pop(0)

            # Renaming columns to standardize different companies' DataFrames
            if sup == 'Ativo Realizável a Longo Prazo':
                sup = 'Ativo Não Circulante'
            if sup == 'Passivo Exigível a Longo Prazo':
                sup = 'Passivo Não Circulante'

            for col in cols[s]:
                tuples.append((sup, col))

        df.columns = pd.MultiIndex.from_tuples(tuples)

    return df


def get_dre(ticker, quarterly=False, ascending=True):
    '''Get the income statement for one brazilian stock listed on fundamentus website.
    NOTE: Values are in thousands!
    :Arguments:
    ticker[str]:
        Ticker to download data from
    quarterly[bool]:
        Whether to download quarterly or annualy data.
        Default is False.
    ascending[bool]:
        Whether the date index should be sorted ascendingly on the DataFrame
        Default is True
    :Raises:
    DataNotFound(IndexError) if there's no data available for that ticker
    :Returns:
    A pandas DataFrame
    '''

    df = _get_sheets(ticker, quarterly=quarterly,
                     ascending=ascending)['Dem. Result.']

    return df