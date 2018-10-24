# coding: utf-8
# DESAFIO ESTÁGIO DATA ENGINEERING
# Load Data TSVs
import pandas as pd
# usei utf8 para corrigir a leitura dos acentos
# tsv é separado por tab
deals = pd.read_csv('deals.tsv', sep='\t',
                 encoding='utf-8')
sectors = pd.read_csv('sectors.tsv', sep='\t',
                 encoding='utf-8')
companies = pd.read_csv('companies.tsv', sep='\t',
                 encoding='utf-8')
contacts = pd.read_csv('contacts.tsv', sep='\t',
                 encoding='utf-8')

# Análise e limpeza dos dados
# renomeei cada coluna para garantir que não há espaços em branco
contacts.columns = ['contactsId', 'contactsName', 'contactsDateCreated', 'contactsCreatedBy',
                    'contactsEmails', 'contactsPhones', 'contactsEmployers', 'employersId', 
                    'contactsHomeAdress', 'contactsLatLong', 'contactsRelatedToLead', 'contactsResponsible']
#print(contacts.info())

# ### Conversão de tipos (date)
deals['dealsDateCreated'] = pd.to_datetime(deals['dealsDateCreated'])
#deals.info()

contacts['contactsDateCreated'] = pd.to_datetime(contacts['contactsDateCreated'])
#contacts.info()

companies['companiesDateCreated'] = pd.to_datetime(companies['companiesDateCreated'])
#companies.info()

# Valores inválidos
# Não é possível haver dealsDateCreated, contactsDateCreated e companiesDateCreated no futuro
# Então, me basendo na data que recebi o desafio '2018-10-09' para ser o fator seletivo

# drop de linhas inválidas em deals
deals_clean = deals[deals.dealsDateCreated < "2018-10-09"]
print(f'Número de linhas descartadas em deals: {len(deals) - len(deals_clean)}')

# drop de linhas inválidas em contacts
contacts_clean = contacts[contacts.contactsDateCreated < "2018-10-09"]
print(f'Número de linhas descartadas em contacts: {len(contacts) - len(contacts_clean)}')

# drop de linhas inválidas em companies
companies_clean = companies[companies.companiesDateCreated < "2018-10-09"]
print(f'Número de linhas descartadas em companies: {len(companies) - len(companies_clean)}')

# # OUTPUT 01: Gráficos
# O primeiro OUTPUT deve conseguir servir de base para 2 gráficos: número de vendas por
# contato e valor total vendido por mês.

# ### Join entre as tabels _deals_ e _contacts_
# O join vai servir para mapear o nome dos contacts
# vai auxiliar na exibição do gráfico com os nomes dos contacts
colunas_necessarias_contacts = ['contactsId','contactsName']
deals_contacts = pd.merge(deals_clean, contacts_clean[colunas_necessarias_contacts], left_on='contactsId', right_on='contactsId')

import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')

# para visualizar de forma ampla dentro do notebook
plt.rcParams['figure.figsize'] = (15, 8)

# organização dos dados
n_vendas_contato = deals_contacts.groupby('contactsName')['dealsPrice'].sum().sort_values(ascending=False)

# visualização do gráfico
n_vendas_contato = n_vendas_contato.plot(kind='bar', color='blue', label='Valor Vendido')
plt.xlabel('Vendedores')
plt.ylabel('Valor')
plt.title("Valor Total Vendido por Contato")
plt.legend(loc="upper right")
plt.legend()
plt.show()

# salvamento da imagem
fig = n_vendas_contato.get_figure()
fig.savefig(f'Valor Total Vendido por Contato.jpg')
print(f"Output (Valor_Total_Vendido_por_Contato) salvo.")

# organização dos dados: valor total vendido por mês a partir do ano
# percorre ano
for ano in range(2017,2019): 
    #print(ano)
    query_year = deals_clean[(deals_clean['dealsDateCreated'].dt.year==ano)]
    extrai_mes = pd.DatetimeIndex(query_year['dealsDateCreated']).month
    n_vendas_mes = query_year.groupby(extrai_mes)['dealsPrice'].sum()
    
    # para renomear as colunas do gráfico sera necessário renomear os índices do pandas series n_vendas_mes
    n_vendas_mes = n_vendas_mes.rename(index={1:"janeiro", 2:"fevereiro", 3:"março", 4:"abril",
            5:"maio", 6:"junho", 7:"julho", 8:"agosto", 9:"setembro", 10:"outubro", 11:"novembro", 12:"dezembro"})

    # visualização do gráfico
    n_vendas_mes_grafico = n_vendas_mes.plot(kind='bar', color='blue', label='Valor Vendido')
    plt.xlabel('Meses')
    plt.ylabel('Negócios Fechado')
    plt.title(f"Valor Vendido por Mês no Ano de {ano}")
    plt.legend(loc="upper right")
    plt.legend()
    plt.show()

    # salvamento da imagem
    fig = n_vendas_mes_grafico.get_figure()
    fig.savefig(f'Valor_total_vendido_por_mês-{ano}.jpg')
    print(f"Output (Valor_total_vendido_por_mês-{ano}) salvo.")

# # OUTPUT 02: Lista das vendas por setores ordenado pelo mês
# O segundo output deve ser uma lista dos setores de empresa, ordenado por quanto esse setor representa no total vendido pela empresa no mês. Por exemplo, considerando que a empresa vendeu 10k total, se o 8k foi vendido para empresas do setor 1 e 2k para empresas do setor 2 então a lista resultante seria:
# - 1 Bens De Consumo 0.8
# - 2 Serviços 0.2
# 
# Note que o nome do setor deve constar na lista.

# O join vai servir para mapear o nome dos sectors
colunas_necessarias_companies = ['companiesId', 'sectorKey' ]
companies_sectors = pd.merge(companies_clean[colunas_necessarias_companies], sectors,
                             left_on='sectorKey', right_on='sectorKey')

# join entre deals e companies_sectors
deals_companies_sectors = pd.merge(deals_clean, companies_sectors,
                                   left_on='companiesId', right_on='companiesId')

# Deixar a tabela já sorted por data
deals_companies_sectors.sort_values(by=['dealsDateCreated'], ascending=False)

# neste dataframe vou armazenar o valor total vendido por mês, separado por setor
table_result = pd.DataFrame(columns=['Ano','Mês', 'Setor', 'Valor Total Vendido Mensal'])

# conversão das colunas Object para int
table_result.Ano = table_result.Ano.astype(int)
table_result.Mês = table_result.Ano.astype(int)
table_result.Setor = table_result.Setor.astype(int)
table_result['Valor Total Vendido Mensal'] = table_result['Valor Total Vendido Mensal'].astype(int)

# percorre ano
for ano in range(2017,2019): 
    #print(ano)
    query_year = deals_companies_sectors[(deals_companies_sectors['dealsDateCreated'].dt.year==ano)]

    # percorre mês
    for mes in range(1,13):
        #print(mes)
        query_year_month = query_year[(query_year['dealsDateCreated'].dt.month==mes)]        
        
        # percorre setor (é menos custoso comparar valores numéricos, então query_year_month['sectorKey'])
        for setor in query_year_month['sectorKey']:
            #print(setor)
            query_sector_month = query_year_month[(query_year_month['sectorKey']==setor)]
                            
            # group by dealsPrice
            grouping = query_sector_month.groupby('sector')['dealsPrice'].sum().sort_values(ascending=False)

            # variável p organizar a inserção no dataframe grouping_sector_month
            data = pd.DataFrame([[ano, mes, setor, grouping[0]]], 
                                columns=['Ano', 'Mês', 'Setor', 'Valor Total Vendido Mensal'])
            table_result = table_result.append(data)
            
            table_result = table_result.drop_duplicates(keep='first')
#table_result.info()

# Mapeamento e ordenação
# mapeamento do nome do setor. Aqui a tabela é bem menor e não é necessário fazer comparações
dict_setor = {1:"Bens de Consumo", 2:"Serviços", 3:"Tecnologia", 4:"Indústria", 5:"Varejo", 6:"Atacado"}
table_result['Setor'] = table_result['Setor'].map(dict_setor)

table_result = table_result.sort_values(by=['Ano', 'Mês', 'Valor Total Vendido Mensal', 'Setor'],
                                        ascending=False)

# Salvamento do dataframe
table_result.to_csv('output.csv', 
                    index=False, sep=',', encoding='utf-8')
print('OUTPUT 02: Lista das vendas por setores ordenado pelo mês foi salvo no arquivo: output.csv')
