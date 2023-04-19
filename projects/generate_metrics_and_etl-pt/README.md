# Resolução: Hiring Challenge (Data Engineers)

## Como Executar
Fiz dois modos para executar o desafio.
<br/>
No primeiro, [Exec Rapida](#exec-rapida), o ambiente esta todo disponibilizado no Google Colab. Dentro dos notebooks é feita a preparação dos ambientes, desde a instalção do Postgres 9.6.24 até a execução dos comandos SQL e Python.
<br/>
No segundo modo, [Exec Normal](#exec-normal), será exigido alguns requisitos. A execução dos códigos Python ocorrerá em um ambiente virtual.

## Exec Rapida
1. Faça o upload dos arquivos `questao_01/kanastra_data_eng_test_part_01.ipynb` e `questao_02/kanastra_data_eng_test_part_02.ipynb` para o Google Drive.
2. Abra os arquivos com o Google Colab. Caso o Colab não esta connectado na sua conta Google faça o seguinte:
- Dê dois cliques no arquivo `ipynb` em seguida, `Abrir com -> Conectar mais aplicativos -> Pesquisar (Colaboratory) -> Instalar`
3. Execute as células: `Runtime -> Run all` ou `Ambiente de execução -> Executar tudo`

## Exec Normal
### Requisitos

| Requisite  | Version |
| ---------- | ------- |
| Python     | 3.9.7   |
| Virtualenv | 20.13.0 |
| PostgreSQL | 9.6.24  |

### Criar e Preparar o Ambiente Virtual de Python
Execute no terminal os seguintes comandos:
```bash
virtualenv -p python3 venv
source venv/bin/activate

pip3 install --require-hashes -r requirements.txt
```

**NOTES**
- Estou utilizando o `--require-hashes` para garantir a prevenção contra ataque de DNS.

### Questão 1 - New York Taxi Trips
Depois de executar a etapa de criação e preparação do ambiente virtual de Python, é possível seguir com os passos abaixo:
1. Download dos datasets
```bash
mkdir /tmp/datasets
cd /tmp/datasets
gdown '1DOvZ-lUlRwyc8jStSSe4Ps0kncHSvhkT'
gdown '1ilCYiB72T8UPerLiku1c6qdRh94vAUhK'
gdown '1-UD_8gnTO1UwW-ZQYbW-2WXlwAK7wsl4'
gdown '10eAuCp7pdUzmBj1SuN_zae3Vo59Wsrfy'

ls -lt /tmp/datasets/
```
2. Crie as tables no Postgres. Para isso execute o arquivo `create_tables.sql`
3. Copie os datasets para a `temp` table no Postgres. Para isso execute no terminal os seguintes comandos:
```bash
psql -U root -d postgres -c "\COPY temp (data) FROM '/tmp/datasets/data-nyctaxi-trips-2009.json';"
psql -U root -d postgres -c "\COPY temp (data) FROM '/tmp/datasets/data-nyctaxi-trips-2010.json';"
psql -U root -d postgres -c "\COPY temp (data) FROM '/tmp/datasets/data-nyctaxi-trips-2011.json';"
psql -U root -d postgres -c "\COPY temp (data) FROM '/tmp/datasets/data-nyctaxi-trips-2012.json';"
```
4. Execute as queries no Postgres:
```
3-vendor_realizou_mais_viagens.sql
4-vendor_percorreu_a_maior_distancia.sql
5-tipo_de_pagamento_mais_utilizado.sql
```

**NOTES**
- Utilizei uma tabela intermediária para fazer o input dos arquivos json para o Postgres e depois migrei para outra tabela já com a estruturada desejada. Para saber mais detalhes [veja o código.](questao_01/kanastra_data_eng_test_questao_01.ipynb)


### Questão 2 - ETL
Depois de executar a etapa de criação e preparação do ambiente virtual de Python, é possível seguir com os passos abaixo:

1. Execute os comandos para gerar o arquivo csv.
```bash
cd questao_02/
ipython kanastra_data_eng_test_questao_02.ipynb
```

---
