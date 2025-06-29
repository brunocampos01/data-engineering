import pandas as pd
import requests

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from requests.auth import HTTPBasicAuth

# Connection
username = '<login>'
password = '<key>'
url = '<http://elastic>:<port>'
index = '<database>'


def start_conn(username: str, password: str, url: str):
  response = requests.get(url, auth = HTTPBasicAuth(username, password))
  print(response)
  
  try:     
      return Elasticsearch(hosts=url, http_auth=(username, password), timeout=5000)
  except es.connection.Error as err:
      print(f"Failed connection: {err}, response: {response}, username: {username}, password: {password}")


def get_query(index: str):
    return {
      "_source": index,
      "query": {
          "match_all": {}
      }
  }


def execute_query(es, query: str):
    list_p = []

    results = helpers.scan(es,
                           index=index,
                           preserve_order=True,
                           query=query_p)

    for item in results:
        # print(item['_id'], item['_source'])
        list_p.append(item)

    return list_p


def convert_to_dataframe(list_element: list):
    df = pd.DataFrame(list_p)
    return df.info()


def save_data_processed(df: 'dataframe' = None, path: str = 'data/processed/') -> None:
    df.to_csv(path_or_buf=path,
              sep=',',
              index=False,
              encoding='utf8',
              header=True)

    return "Data recorded!"


def main():
    query_p = get_query(index=index)
    conn = start_conn(username=username, password=password, url=url)
    list_element = execute_query(es=conn, query=query_p)
    df = convert_to_dataframe(list_element)
    save_data_processed(df=df, path='name_file.csv')


if __name__ == "__main__":
    main()
