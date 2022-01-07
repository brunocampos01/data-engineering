import json
import os

here = os.path.abspath(os.path.dirname(__file__))


def get_list_docs(path: str) -> 'generator':
    list_data_name = os.listdir(path)
    return (os.path.splitext(os.path.basename(doc))[0] for doc in list_data_name)


def read_data_config(data_name: str) -> dict:
    with open(''.join(here + f'/../../configs/data/{data_name}.json'), 'r') as f:
        return json.load(f)
