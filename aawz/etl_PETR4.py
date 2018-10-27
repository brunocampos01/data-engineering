# coding: utf-8
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3
from sqlalchemy import create_engine

# construção da URL
# datas
year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day
data_inicial = (f'{day}/{month}/{year-10}')
data_final = (f'{day}/{month}/{year}')

# ativo
ativo = 'PETR4'

#url
response = requests.get(f"https://www.infomoney.com.br/Pages/Download/Download.aspx?dtIni={data_inicial}&dtFinish={data_final}&Ativo={ativo}&Semana=null&Per=null&type=2&Stock={ativo}&StockType=1", timeout=60)
print("get page: ", response)

# criação do parser para navegação na árvore DOM
content = BeautifulSoup(response.content, "html.parser")
table = content.find_all("tr")

# Removing Multple spaces
def remove_multiple_spaces(string):
    """
    Se é String, então faz remoção de espaços em branco 
    replace: / por espaço em branco
    replace: , por . para facilitar a conversão de tipos
    replace: todas as palavras que entraram na tabela
    """
    if type(string)==str:
        string_n = ' '.join(string.split()).replace('/',' ').replace(',','.')
        string = string_n.replace('Fech.', '-1').replace('Var.Dia', '-1')
        return string
    return string

"""
Contrução da tabela
"""
# neste dataframe vou armazenar a data e o fechamento diário
df_petr4 = pd.DataFrame(columns=['Ano', 'Fechamento'])

# percorre table
for row in table:
    # .text vem do requests e garante o scrapping
    text = row.text 
    text = remove_multiple_spaces(text)
    
    # insere dados diferentes a cada linha
    text_dados = text.split(sep=" ")
    
    # convert list to String
    # quando é feito o slice do text_dados é retornado um list
    ano = np.asarray(text_dados[2:3])
    fechamento = np.asarray(text_dados[3:4])

    # variável p organizar a inserção no dataframe
    dados = pd.DataFrame([[ano, fechamento]], columns=['Ano', 'Fechamento'])    
    df_petr4 = df_petr4.append(dados)

# remove rows with Strings
df_petr4 = df_petr4[1:]

# Conversão de tipos object
df_petr4.Ano = df_petr4.Ano.astype('int16') # int8 não aceitou, pequeno demais
df_petr4.Fechamento = df_petr4.Fechamento.astype('float16')


""" Granularidade anual: 
- tanto a taxa selic quanto a PETR4 devem ser de mesma granularidade
- nível do grão = ano
""" 
# dataframe somente com os fechamentos do ano
df_petr4_clean = pd.DataFrame(columns=['Ano','Fechamento'])

# for serve para get fechamento do ano
for ano in range(year-10, year+1): 
    #print(ano)
    df_petr4_year = df_petr4[(df_petr4['Ano']==ano)]
    
    # get value 
    fechamento_ano = df_petr4_year.Fechamento.iloc[0]
    #print(fechamento_ano)
    
    # variável p organizar a inserção no dataframe
    dados = pd.DataFrame([[ano, fechamento_ano]], columns=['Ano', 'Fechamento'])    
    df_petr4_clean = df_petr4_clean.append(dados)

print(df_petr4.info())

# Salvamento do dataframe no sqlite
engine = create_engine('sqlite:///desafio_AAWZ.db')
df_petr4_clean.to_sql('petr4', con=engine, if_exists='replace', index=False)
print('\n(petr4.db) salvo!')
