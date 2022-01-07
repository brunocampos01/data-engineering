import json
import logging
import os

import urllib3
from airflow.models import Variable

from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.db.hive_helper import HiveHelper
from hooks.db.impala_helper import ImpalaHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from utils.config_helper import get_list_docs
from utils.config_helper import read_data_config
from utils.data_helper import prepare_avro_schema

urllib3.disable_warnings()
here = os.path.abspath(os.path.dirname(__file__))

# configs
with open(''.join(here + '/../../configs/dag_config.json'), 'r') as f:
    dag_cfg = json.load(f)

with open(''.join(here + '/../../configs/conn_config.json'), 'r') as f:
    dict_config_conn = json.load(f)


def main():
    template = '-' * 79
    list_data = get_list_docs(path=''.join(here + f'/../../configs/data/'))

    for tag in ('oracle', 'redis', 'big_data'):
        logging.info(f'\n{template}\nCreating connections\n{template}')
        for conn_name in dict_config_conn[tag]:
            c = dict_config_conn[tag][conn_name]
            AirflowMetaStoreHelper(airflow_conn=c['conn']) \
                .create_conn(conn_type=c['conn_type'],
                             host=c['host'],
                             login=c['login'],
                             passwd=c['password'],
                             port=c['port'],
                             extra=c['extra'],
                             describe=c['describe'])

    impala = ImpalaHelper(hive_conn='impala')
    hive = HiveHelper(hive_conn='hive')
    hdfs = HdfsHelper(hdfs_conn='webhdfs')

    Variable.set(key='total_rows', value='100000')
    Variable.set(key='env', value=os.environ.get('ENV').lower())

    for data_name in list_data:
        dict_cfg_data = read_data_config(data_name=data_name)
        cfg = dict_cfg_data[data_name]
        cfg_hdfs = dag_cfg["paths"]["path_hdfs"]

        list_all_layers = list(cfg["hdfs_data_schema"].keys())
        list_layers = [layer for layer in list_all_layers if layer != 'db_comment']

        for layer in list_layers:

            # control var
            try:
                logging.info(f'\n{template}\nLoading {data_name} control_var to airflow variables\n{template}')
                val_control_var = hdfs.get_control_var(data_name=data_name, layer=layer)
                Variable.set(key=f'{data_name}_control_var', value=val_control_var)
                logging.info(f'control_var = {val_control_var}')
            except Exception as err:
                if layer == 'raw':
                    logging.error(err.__class__)
                    logging.info(f'File does not have exists. Creating ...')
                    hdfs.create_file_state(data_name=data_name, layer=layer)

            # avro schema
            logging.info(f'\n{template}\nGenerating and saving {data_name} avro schema\n{template}')
            avro_schema = prepare_avro_schema(layer=layer,
                                              data_name=data_name,
                                              template=template,
                                              path_ojdbc=f"{dag_cfg['paths']['path_libs']}/{dag_cfg['libs']['ojdbc']}",
                                              path_native_lib=dag_cfg['paths']['path_native_lib'],
                                              doc_type=cfg['doc_type'],
                                              list_dict_cols=cfg["hdfs_data_schema"][layer]["cols"])
            hdfs \
                .save_avro_schema_hdfs(data_name=data_name,
                                       hdfs_path=cfg_hdfs["path_avro_schemas"],
                                       layer=layer,
                                       avro_schema=avro_schema)
            hdfs \
                .download_file(hdfs_path=f'{cfg_hdfs["path_avro_schemas"]}/{layer}/{data_name}.avsc',
                               local_path=f'{dag_cfg["paths"]["path_local_avro_schemas"]}/{layer}')

            # db, tables, partitions
            hive \
                .hive_prepare_env(env='prod',
                                  layer=layer,
                                  data_name=data_name,
                                  cfg=dict_cfg_data[data_name],
                                  cfg_hdfs=cfg_hdfs,
                                  template=template)

            logging.info(f'\n{template}\nUpdating DB statistics\n{template}')
            impala \
                .execute_invalidate_metadata()

            logging.info(f'\n{template}\nDownload {data_name} avro schemas to'
                         f' {dag_cfg["paths"]["path_local_avro_schemas"]}\n{template}')


if __name__ == '__main__':
    main()
