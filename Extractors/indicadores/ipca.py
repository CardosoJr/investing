# EXTRACTED FROM https://github.com/fortinbras/python_ipca_ibge/blob/master/get_ipca_ibge.py


import os, sys, time

import requests
import datetime
import json

data_corrente_obj = datetime.date.today()

def get_range_meses():
    """ 
    Monta a range de meses para capturar o indice de correcao.
    Eh passado no params do request para o IBGE.
    """
    
    # Escolha o ano de inicio. 
    # Julho de 1994 eh a adocao da moeda corrente (Real).     
    ano_inicio = 1994
    ano_corrente = data_corrente_obj.year

    range_meses = ''
    while ano_inicio <= ano_corrente:
        mes = 1
        while mes < 13:
            data_loop = "%d%d" % (ano_inicio, mes)
            data_loop_obj = datetime.datetime.strptime(data_loop, '%Y%m').date()

            if data_loop_obj> data_corrente_obj:
                break
            if mes < 10:
                mes_str = '0' + str(mes)
            else:
                mes_str = mes
            range_meses += "%s%s%s" % (str(ano_inicio), mes_str, ',')
            mes += 1
        ano_inicio += 1

    range_meses = range_meses[:-1]
    return range_meses

def get_params():
    """ Monta a string params para passar no request para o IBGE """
    range_meses = get_range_meses()

    # Adiciona valores a string params.
    params = 't/1737/f/c/h/n/n1/all/V/2266/P/' + range_meses + '/d/v2266 13'
    return params

def get_ipca_ibge(start, end):
    if isinstance(start, str):
        start = datetime.datetime.strptime(start, "%Y-%m-%d")

    if isinstance(end, str):
        end = datetime.datetime.strptime(end, "%Y-%m-%d")

    sess = requests.Session()
    IBGE_IPCA_AJAX_URL = 'https://sidra.ibge.gov.br/Ajax/JSon/Valores/1/1737'

    data = {
        'params': get_params(),
        'versao': '-1',
        'desidentifica': 'false'
    }

    # Download do arquivo
    json_response = sess.post(IBGE_IPCA_AJAX_URL, data=data)
    return json_response