import os

import pytest
from airflow.models import DagBag

from utils.config_helper import get_list_docs
from utils.config_helper import read_data_config

here = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture()
def load_dag_names():
    list_data = []
    for data_name in get_list_docs(path=''.join(here + f'/../configs/data/')):
        dict_cfg_data = read_data_config(data_name=data_name)
        cfg = dict_cfg_data[data_name]

        if cfg['doc_type'] == 'table':
            list_data.append(data_name)

    return sorted(list_data)


@pytest.fixture()
def get_dags_import_table():
    dag_bag = DagBag(include_examples=False)

    return [
        dag_bag.get_dag('import_table_registry'),
        dag_bag.get_dag('import_table_payment')
    ]


@pytest.fixture()
def get_dags_import_file():
    dag_bag = DagBag(include_examples=False)

    return [
        dag_bag.get_dag('import_table_file_name')
    ]
