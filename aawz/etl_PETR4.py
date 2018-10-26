# coding: utf-8

import datetime
import lxml.html
import requests
import pandas as pd
from bs4 import BeautifulSoup

""" Construção da URL"""
# datas
year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day
data_inicial = (f'{day}/{month}/{year-10}')
data_final = (f'{day}/{month}/{year}')

# ativo
ativo = 'PETR4'

#url
url = requests.get(f"https://www.infomoney.com.br/Pages/Download/Download.aspx?dtIni={data_inicial}&dtFinish={data_final}&Ativo={ativo}&Semana=null&Per=null&type=2&Stock={ativo}&StockType=1")

# criação do parser para navegação na árvore DOM
content = BeautifulSoup(url.content, "html.parser")
table = content.find_all("tr")
#(HTML(str(table)))

# Removing Multple spaces
def remove_multiple_spaces(string):
    """
    Se é String, então faz remoção de espaços em branco 
    replace: / por espaço em branco
    replace: , por . para facilitar a conversão de tipos
    """
    if type(string)==str:
        string_n = ' '.join(string.split()).replace('/',' ').replace(',','.')
        string = string_n.replace('Histórico', '-1').replace('Fech.', '-1')
        return string
    return string

# neste dataframe vou armazenar a data e o fechamento diário
df_petr4 = pd.DataFrame(columns=['Ano', 'Mes', 'Fechamento'])

# percorre table
for row in table:
    text = row.text
    text = remove_multiple_spaces(text)
    
    # insere dados diferentes a cada linha
    text_dados = text.split(sep=" ")
    
    # convert list to String
    ano = ''.join(text_dados[2:3])
    mes = ''.join(text_dados[1:2])
    fechamento = ''.join(text_dados[3:4])

    # variável p organizar a inserção no dataframe
    dados = pd.DataFrame([[ano, mes, fechamento]], columns=['Ano', 'Mes', 'Fechamento'])    
    df_petr4 = df_petr4.append(dados)

# remove rows with Strings
df_petr4 = df_petr4[1:]

# Conversão de tipos object
df_petr4.Ano = df_petr4.Ano.astype('int16') # int8 não aceitou
df_petr4.Mes = df_petr4.Mes.astype('int8')
df_petr4.Fechamento = pd.to_numeric(df_petr4.Fechamento, downcast='float', errors='coerence')
print(df_petr4.info(),"\n")

# Salvamento do dataframe
df_petr4.to_csv('df_petr4.csv', 
                    index=False, sep=',', encoding='utf-8', decimal='.')
print('(df_petr4.csv) salvo!')

