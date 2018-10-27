# DESAFIO DEV BACK-END 

## About

### Título
Desenvolver uma análise quantitativa e gráfica relacionando o aumento das ações da
Petrobrás (PETR4) com as variações da taxa SELIC para os últimos 10 anos.

### Objetivo
Fazer um programa em python para:
- Extrair as informações dos sites: https://www.infomoney.com.br/petrobras-petr4/cotacoes e 
https://www.bcb.gov.br/pec/copom/port/taxaselic.asp e tratá-las;
- Armazenar as informações tratadas no SQLite;
  - Utilizar, de preferência, ORM (object relational mapping)
- Ler as informações do banco e apresentar em formato gráfico.

### Restrições
- Utilizar a biblioteca pandas do Python

## Quickstart
Para visualizar os códigos e análises, abra o arquivo [desafio_AAWZ.ipynb](https://github.com/brunocampos01/challenges/blob/master/aawz/desafio_AAWZ.ipynb) 

## Pre Requirements:
- Python 3.6 ou superior 
- Bibliotecas: Numpy, Pandas, matplotlib, sqlite3, sqlalchemy, bs4, requests 
- Git instalado no computador que irá executar

## Running
1. Abra o terminal e clone o repositório: <br/>
`git clone https://github.com/brunocampos01/challenges/`<br/>
`cd challenges/aawz`
2. Execute o script:<br/>
`python3 desafio_AAWZ.py`

---
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Bruno A. R. M. Campos</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
