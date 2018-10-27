# coding: utf-8
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3
from sqlalchemy import create_engine

response = requests.get("https://www.bcb.gov.br/pec/copom/port/taxaselic.asp", timeout=60)
print("get page: ", response)

# criação do parser para navegação na árvore DOM
content = BeautifulSoup(response.content, "html.parser")
table = content.find_all("tr")

## Removing Multple spaces
def remove_multiple_spaces(string):
    """
    Se é String, então faz remoção de espaços em branco 
    trans_string: remove os valores textuais por espaços em branco 
    trans_n: / por espaço em branco AND (,) por (.) para facilitar a conversão de tipos
    """
    if type(string)==str:
        trans_string = string.replace('baixa',' ').replace('alta',' ').replace(' ex.','').replace('uso',' ')
        divisao = ' '.join(trans_string.split())
        trans_n = divisao.replace('/',' ').replace(',','.')
        return trans_n
    return string

"""
Contrução da tabela
"""
# neste dataframe vou armazenar a data e a taxa SELIC
df_selic = pd.DataFrame(columns=['Ano', 'Taxa SELIC'])

# percorre table
for row in table:
    text = row.text
    text = remove_multiple_spaces(text)
    
    # insere dados diferentes a cada linha
    text_dados = text.split(sep=" ")
    
    # convert list to String
    # quando é feito o slice do text_dados é retornado um list
    ano = np.asarray(text_dados[6:7])
    taxa = np.asarray(text_dados[13:])
                
    # DataFrame p organizar a inserção no dataframe
    dados = pd.DataFrame([[ano, taxa]], columns=['Ano', 'Taxa SELIC'])    
    df_selic = df_selic.append(dados)

# drop rows with Strings
df_selic = df_selic[3:]

# select 10 years
year = (datetime.date.today().year)
data_inicial = (f'{year-10}')
df_selic = df_selic[df_selic.Ano >= data_inicial]

# Conversão de tipos object
df_selic.Ano = df_selic.Ano.astype('int16') # int8 não aceitou, pequeno demais
df_selic['Taxa SELIC'] = df_selic['Taxa SELIC'].astype('float16')


""" Granularidade anual: 
- tanto a taxa selic quanto a PETR4 devem ser de mesma granularidade
- nível do grão = ano
""" 
# dataframe somente com as taxas de fechamento de cada ano
df_selic_clean = pd.DataFrame(columns=['Ano','Taxa SELIC'])

# for serve para get fechamento do ano
for ano in range(year-10, year+1): 
    #print(ano)
    df_selic_year = df_selic[(df_selic['Ano']==ano)]
    
    # get value 
    taxa_ano = df_selic_year['Taxa SELIC'].iloc[0]
    #print(taxa_ano)
    
    # variável p organizar a inserção no dataframe
    dados = pd.DataFrame([[ano, taxa_ano]], columns=['Ano', 'Taxa SELIC'])    
    df_selic_clean = df_selic_clean.append(dados)

print(df_selic.info())

# Salvamento do dataframe no sqlite
engine = create_engine('sqlite:///desafio_AAWZ.db')
df_selic_clean.to_sql('selic', con=engine, if_exists='replace', index=False)
print('\n(selic.db) salvo!')
