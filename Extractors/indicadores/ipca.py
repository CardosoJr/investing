import requests
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import json
import pandas as pd

import urllib.request
import urllib.parse
import http.cookiejar

def get_period(start, end):
    meses = []

    current = start 

    end = last_day_of_month(end)

    while current <= end:
        meses.append(current.strftime("%Y%m"))
        current = current + relativedelta(months = 1)

    return ','.join(meses)

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def get_sidra_ipca(start, end):
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

    url = "https://apisidra.ibge.gov.br/values" + "/t/1737/n1/all/v/63,69/p/" + get_period(start, end) 

    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201'),
                         ('Accept', 'text/html, text/plain, text/css, text/sgml, */*;q=0.01')]

    with opener.open(url) as link:
        content = json.loads(link.read().decode('UTF-8'))

    return handle_sidra_json(content)

def handle_sidra_json(content):
    return pd.DataFrame([(x['D2N'], x['V'], datetime.strptime(x['D3C'], "%Y%m")) for x in content[1:]],
                         columns = ['KPI', 'Value', 'Date'])