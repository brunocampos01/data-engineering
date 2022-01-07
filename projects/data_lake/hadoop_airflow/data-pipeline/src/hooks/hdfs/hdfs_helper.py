import json
import subprocess
import tempfile
from datetime import datetime
from typing import Optional

from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from security.kerberos import kerberos_auth


class HdfsHelper(WebHDFSHook):
    __slots__ = 'hdfs_conn'   # space savings in memory + faster attribute access

    def __init__(self, hdfs_conn: Optional[str] = None, *args, **kwargs):
        super(HdfsHelper, self).__init__(*args, **kwargs)
        self.hdfs_conn = hdfs_conn

    def _connect(self) -> 'hdfs.ext.kerberos.KerberosClient':
        return kerberos_auth(self.hdfs_conn)

    def get_conn(self) -> 'hdfs.ext.kerberos.KerberosClient':
        return self._connect()

    def list_filename(self, path: str) -> list:
        return self._connect().list(hdfs_path=path)

    def mv_files(self, hdfs_src_path: str, hdfs_dst_path: str) -> None:
        return self._connect().rename(hdfs_src_path=hdfs_src_path,
                                      hdfs_dst_path=hdfs_dst_path)

    def make_dirs(self, hdfs_path: str) -> None:
        return self._connect().makedirs(hdfs_path, permission='0775')

    def read_avro_schema(self, path_avro_schema: str, layer: str, dag_name: str) -> str:
        with self._connect().read(f'{path_avro_schema}'
                                  f'/{layer}'
                                  f'/{dag_name}.avsc', encoding='utf-8') as reader:
            return reader.read()

    def load_data(self, hdfs_path: str, hdfs_filename: str, data: memoryview) -> None:
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(data)
            temp.flush()
            self._connect().upload(hdfs_path=hdfs_path + '/' + hdfs_filename,
                                   local_path=temp.name,
                                   n_threads=8,
                                   chunk_size=65536,
                                   overwrite=True,
                                   cleanup=True)

    def download_file(self, hdfs_path: str, local_path: str) -> None:
        self._connect().download(hdfs_path=hdfs_path,
                                 local_path=local_path,
                                 overwrite=True)

    def clear_inconsistency_data(self, dag_name: str, list_inconsistency: list, layer: str) -> None:
        for dt in list_inconsistency:
            self._connect().delete(hdfs_path=f'/data/{layer}/{dag_name}'
                                             f'/{dt[-4:]}/{dt[-7:-5]}/{dt[-10:-8]}',
                                   recursive=True)

    def get_absolute_all_path(self, hdfs_doc_path: str) -> list:
        """Get all hdfs path start by hdfs_doc_path. Walk year, month and day.
        :hdfs_doc_path
            example: /data/raw/file_name

        :return
            example:  ['/data/raw/dime/2020/01/02', '/data/raw/dime/2020/01/03', ... ]

        :bash
            hadoop fs -ls /data/raw/file* | awk '{printf "%s ", $NF}'
        """
        self.log.info(f'Getting file ...')
        list_paths = []
        conn = self._connect()
        list_years = sorted(conn.walk(hdfs_path=hdfs_doc_path,
                                      depth=1,
                                      ignore_missing=True))[0][1]
        for year in list_years:
            self.log.info(f'year:{year}')
            list_months = sorted(conn.walk(hdfs_path=f'{hdfs_doc_path}/{year}',
                                           depth=1,
                                           ignore_missing=True))[0][1]

            for month in list_months:
                self.log.info(f'month:{month}')
                list_days = sorted(conn.walk(hdfs_path=f'{hdfs_doc_path}/{year}/{month}',
                                             depth=1,
                                             ignore_missing=True))[0][1]

                for day in list_days:
                    list_paths.append(hdfs_doc_path + '/' + year + '/' + month + '/' + day)

        return list_paths

    def remove_empty_files(self, list_hdfs_path: list) -> None:
        conn = self._connect()

        for path in list_hdfs_path:
            conn.delete(path + '/' + '_SUCCESS')

    def remove_list_files(self, path: str, list_filename: list) -> None:
        conn = self._connect()
        [conn.delete(path + '/' + filename) for filename in list_filename]

    def remove_all_files(self, hdfs_path: str) -> None:
        self._connect().delete(hdfs_path=hdfs_path + '/',
                               recursive=True,
                               skip_trash=True)

    def generate_list_path_to_concat(self, list_hdfs_path: list) -> list:
        """
        :return:
            example:
             ['/data/raw/doc_name/2000/02/01', '/data/raw/doc_name/2000/02/22', ...]
        """
        conn = self._connect()
        list_path_to_concat = []

        for path in list_hdfs_path:
            list_files = sorted(conn.list(hdfs_path=path))
            self.log.info(f'Total files = {len(list_files)}')

            # concatenation done
            if len(list_files) == 1:
                continue
            list_path_to_concat.append(path)

        return list_path_to_concat

    def generate_list_path_to_rename(self, list_hdfs_path: list, dag_name: str) -> list:
        """
        :return:
            example:
             ['/data/raw/doc_name/2000/01/02', '/data/raw/doc_name/2000/01/03', ...]
        """
        conn = self._connect()
        list_path_to_rename = []

        for path in list_hdfs_path:
            list_files = sorted(conn.list(hdfs_path=path))
            list_matching_dag_name = [file for file in list_files if file.startswith(dag_name)]
            self.log.info(f'Total files = {len(list_files)}')

            # concatenation done
            if len(list_files) == 1 and len(list_matching_dag_name) > 0:
                continue

            # if, one file by dir else, multiples files by dir
            if len(list_files) == 1 and list_files[0].startswith('part-'):
                list_path_to_rename.append(path)

        return list_path_to_rename

    @staticmethod
    def remove_path_without_dt_ref(list_path: list) -> list:
        return [path for path in list_path if not path.endswith('/1900/01/01')]

    @staticmethod
    def remove_path_first_day(list_path: list) -> list:
        return [path for path in list_path if not path.endswith('/01')]

    def create_dir_first_day(self, list_hdfs_path: list) -> None:
        conn = self._connect()

        for path in list_hdfs_path:
            if path.endswith('/01'):
                continue

            conn.makedirs(path[:-2] + '01')

    def mv_files_to_first_day(self, list_path_to_mv: list) -> None:
        conn = self._connect()

        for path in list_path_to_mv:
            list_file_name = sorted(conn.list(hdfs_path=path))

            for file in list_file_name:
                conn.rename(hdfs_src_path=path + '/' + file, hdfs_dst_path=path[:-2] + '01')

            conn.delete(path)

    def count_files(self, path_avro_tools: str, path: str) -> str:
        self.log.info(f'counting total files in this directory: {path}')
        cmd_total_avro = f'hadoop jar {path_avro_tools}' \
                         f' count {path}'

        self.log.info(f'Executing count:\n{cmd_total_avro}')
        total = subprocess.run(cmd_total_avro,
                               shell=True,
                               check=True,
                               executable='/bin/bash',
                               stdout=subprocess.PIPE)

        return total.stdout.decode('utf-8').replace('\n', '')

    def concat_files(self,
                     path: str,
                     path_avro_tools: str,
                     dag_name: str,
                     total_registry: str,
                     extra: Optional[str] = '') -> None:
        conn = self._connect()
        list_files = sorted(conn.walk(hdfs_path=f'{path}',
                                      depth=1,
                                      ignore_missing=True))[0][2]
        self.log.info(f'Concatenating this files: {list_files}')

        cmd_printf = '{printf "%s ", $NF}'
        cmd_get_all_name_files = f"hadoop fs -ls {path}/* | awk '{cmd_printf}'"
        filename_concat = f'{dag_name}_original_{total_registry}{extra}.avro'
        cmd_compress = f'''hadoop jar {path_avro_tools} \
                        concat $({cmd_get_all_name_files}) \
                        {path}/{filename_concat}'''
        self.log.info(f'Executing concat:\n{cmd_compress}')

        try:
            subprocess.run(cmd_compress,
                           shell=True,
                           check=True,
                           executable='/bin/bash')
            [conn.delete(hdfs_path=f'{path}/{file_name}',
                         recursive=False) for file_name in list_files]

        except subprocess.CalledProcessError as err:
            self.log.error(f'Exception: {err.__class__}')
            conn.delete(f'{path}/{filename_concat}')

    def rename_files(self, path: str, path_avro_tools: str, dag_name: str) -> None:
        total_avro = f'hadoop jar {path_avro_tools} count {path}'
        cmd_rename = f'''hdfs dfs -mv {path}/* \
                    {path}/{dag_name}_original_$({total_avro}).avro'''

        self.log.info(f'Executing:\n{cmd_rename}')
        subprocess.run(cmd_rename,
                       shell=True,
                       check=True,
                       executable='/bin/bash')

    @staticmethod
    def generate_hdfs_path(env: str,
                           layer: str,
                           dag_id: str,
                           date: str = None,
                           is_partitioned: bool = True) -> str:
        if is_partitioned:
            date = datetime.strptime(str(date), '%d-%m-%Y').date()
            return f'/data' \
                   f'/{env}' \
                   f'/{layer}' \
                   f'/{dag_id}' \
                   f'/{date.year:04d}' \
                   f'/{date.month:02d}' \
                   f'/{date.day:02d}'

        return f'/data' \
               f'/{env}' \
               f'/{layer}' \
               f'/{dag_id}'

    @staticmethod
    def save_pyspark_df(df: 'pyspark.sql.dataframe.DataFrame',
                        mode: str,
                        format: str,
                        avro_schema: str,
                        compress_type: str,
                        hdfs_path: str,
                        partitions: int = 1) -> None:
        df.repartition(partitions).write \
            .format(format) \
            .mode(mode) \
            .option('batchsize', 50000) \
            .option('avroSchema', avro_schema) \
            .option('compression', compress_type) \
            .save(path=f'hdfs://{hdfs_path}')

    @staticmethod
    def load_pyspark_df(spark: 'pyspark.sql.session.SparkSession',
                        data_format: str,
                        path: str) -> 'pyspark.sql.dataframe.DataFrame':
        return spark \
            .read \
            .format(data_format) \
            .load(path=path)

    def save_avro_schema_hdfs(self,
                              data_name: str,
                              hdfs_path: str,
                              layer: str,
                              avro_schema: dict) -> None:
        tmp_file = tempfile.NamedTemporaryFile(mode="w+")
        json.dump(avro_schema, tmp_file, indent=4)
        tmp_file.flush()

        hdfs_absolute_path = f'{hdfs_path}/{layer}/{data_name}.avsc'
        self._connect().upload(hdfs_path=hdfs_absolute_path,
                               local_path=tmp_file.name,
                               overwrite=True,
                               replication=256)

    def get_control_var(self, data_name: str, layer: str) -> str:
        path_state = f'/user/job/data_pipeline/{data_name}/{layer}_state.json'
        with self._connect().read(path_state, encoding='utf-8') as r:
            dict_state = json.load(r)
            return dict_state['control_var']

    def save_execution_state_hdfs(self, **context) -> None:
        data = {
            'control_var': context['control_var']
        }

        tmp_file = tempfile.NamedTemporaryFile(mode="w+")
        json.dump(data, tmp_file, indent=4)
        tmp_file.flush()

        self.log.info(f'Save last state: {data}')
        self._connect().upload(hdfs_path=f'/user/job/data_pipeline'
                                         f'/{context["dag_name"]}'
                                         f'/{context["layer"]}_state.json',
                               local_path=tmp_file.name,
                               overwrite=True)

    def create_file_state(self, data_name: str, layer: str) -> None:
        data = {
            'control_var': '000000000000000'
        }

        tmp_file = tempfile.NamedTemporaryFile(mode="w+")
        json.dump(data, tmp_file, indent=4)
        tmp_file.flush()

        self._connect().upload(hdfs_path=f'/user/job/data_pipeline'
                                         f'/{data_name}'
                                         f'/{layer}_state.json',
                               local_path=tmp_file.name,
                               overwrite=True)

    def save_execution_state_in_local(self, **context) -> None:
        with open(context['path_json'], "r") as json_file:
            data = json.load(json_file)

        data[context["dag_name"]]['control_var'] = context['control_var']

        self.log.info(f'Save last state: {data[context["dag_name"]]["control_var"]}')
        with open(context['path_json'], "w") as json_file:
            json.dump(data, json_file, indent=4)
