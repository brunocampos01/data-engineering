import configparser
import datetime
import logging
import os

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import trim
from pyspark.sql.types import StringType

# config
path_name_file = os.path.basename(__file__)
path_directory = os.path.dirname(os.path.abspath(__file__))
path_config = ''.join(path_directory + '/../configs/etl_config.ini')

config = configparser.ConfigParser()
config.read(path_config)

PATH_DOCS = config['DOC']['PATH_DOCS']
PATH_DICT = config['DOC']['PATH_DICT']
PATH_INDEX = config['DOC']['PATH_INDEX']

LOG_FILE = config['LOG']['LOG_FILE']
LOG_FILEMODE = config['LOG']['FILEMODE']

# log
log_format = '%(asctime)s - %(levelname)s: %(message)s'
date_format = '%d-%b-%y %H:%M:%S'

logging.basicConfig(filename=LOG_FILE,
                    filemode=LOG_FILEMODE,
                    format=log_format,
                    datefmt=date_format)
start_time = datetime.datetime.now()
logging.warning(f'Start: {start_time}')


class JobMapWordIdDocId(object):

    def __init__(self,
                 path_files: str,
                 path_index: str,
                 path_dict: str,
                 file_name: str,
                 num_partition: int):
        path = ''.join(path_files + file_name)
        self.__file_name = file_name

        conf = SparkConf()
        conf.setAll(
            [
                ('spark.app.name', 'Challenge Data Engineer'),
                ('spark.driver.cores', '4'),
                ('spark.executor.cores', '4'),
                ('spark.driver.maxResultSize', '10g'),
                ('spark.executor.memory', '4g'),
                ('spark.executor.memoryOverhead	', '4g'),
                ('spark.driver.memory', '10g'),
                ('spark.local.dir', PATH_INDEX),
                ('spark.driver.extraJavaOptions', '-Xmx1024m'),
                ('spark.memory.offHeap.enabled', 'true'),
                ('spark.memory.offHeap.size', '20g')
            ]
        )

        self.__spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .getOrCreate()

        self.__df_dict = self.__spark \
            .read \
            .parquet(path_dict) \
            .repartition(numPartitions=num_partition)

        self.__df_doc = self.__spark \
            .read \
            .text(path)

        self.__df_wordid_docid = self.__spark \
            .read \
            .parquet(path_index) \
            .rdd \
            .unpersist() \
            .repartition(numPartitions=1000)

        self.__df_wordid_docid = self.__df_wordid_docid.toDF()

        logging.warning(f"Processing doc: {path}")

    def __del__(self):
        self.__spark.catalog.clearCache()
        self.__spark.stop()
        end_time = datetime.datetime.now()
        logging.warning(f'Total time: {end_time - start_time}')

    def clean_data(self, list_words_col: str) -> pyspark.sql.DataFrame:
        """Pre-processing data
        Processing executed in function:
        - Lower case
        - Words start with letters or whitespace
        - Remove whitespaces into start and final words
        - Remove rows empty
        - Transform each row in list
        :Args:
            :param list_words_col: column's name of Dataframe
        :Returns:
            :return: Dataframe with a word list in each row
        :Samples:
            +--------------------+
            |               value|
            +--------------------+
            |[over, in, one, n...|
        """
        self.__df_doc = self.__df_doc \
            .withColumn(list_words_col, lower(col(list_words_col))) \
            .withColumn(list_words_col, regexp_replace(str=col(list_words_col),
                                                       pattern='[^a-z ]',
                                                       replacement='')) \
            .withColumn(list_words_col, trim(col(list_words_col))) \
            .filter(self.__df_doc[list_words_col] != "") \
            .withColumn(list_words_col, split(list_words_col, ' '))

        return self

    def generate_word_by_row(self, col_words: str) -> pyspark.sql.DataFrame:
        """
        Params:
            :param col_words: a column in df contains word list in each row
        Returns:
            :return: df with all words split by row
        Samples:
            +-----------+
            |        col|
            +-----------+
            |    project|
        """
        self.__df_doc = self.__df_doc \
            .select(explode(self.__df_doc[col_words]))
        return self

    def get_word_id(self, col_words: str, join_operation: str,
                    col_words_dict: str) -> pyspark.sql.DataFrame:
        """
        Params:
            :param col_words: a column in df contains word list in each row
            :param join_operation: type join
            :param col_words_dict: name column of words
        Returns:
            :return: 'pyspark.sql.dataframe.DataFrame' with all word id
        Samples:
            +------+
            |   key|
            +------+
            |101780|
        """
        self.__df_doc = self.__df_doc \
            .join(self.__df_dict,
                  on=self.__df_doc[col_words_dict]
                     == self.__df_dict[col_words],
                  how=join_operation) \
            .drop(col_words) \
            .drop(col_words_dict)

        return self

    def generate_wordid_docid(self, col_word_key: str):
        """ Follow documentation:
        https://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=flatmap

        Params:
            :param col_word_key: a column in df contains word list in each row
        Returns:
            :return: dataframe with column doc_id and column word_id
        Samples:
            +---+--------+
            | _1|      _2|
            +---+--------+
            | 27|[101780]|
        """
        list_key_by_doc = self.__df_doc.select(col_word_key).collect()

        doc_rdd = self.__spark \
            .sparkContext \
            .parallelize([(self.__file_name, list_key_by_doc)])

        def f(doc_rdd): return doc_rdd

        self.__df_doc = doc_rdd.flatMapValues(f).toDF()

        return self

    def prepare_df(self,
                   name_original_col: str,
                   new_name_key: str,
                   new_name_doc: str):
        """
        Returns:
            :return: dataframe with column doc_id and column word_id
        Samples:
            +------+-------+
            |doc_id|word_id|
            +------+-------+
            |    27|      0|
            |    27|      1|
        """
        self.__df_doc = self.__df_doc \
            .withColumn(name_original_col,
                        self.__df_doc[name_original_col].cast(StringType())) \
            .withColumn(name_original_col,
                        regexp_replace(str=col(name_original_col),
                                       pattern='[^0-9]',
                                       replacement='')) \
            .orderBy(name_original_col, ascending=True) \
            .dropDuplicates([name_original_col]) \
            .withColumnRenamed('_1', new_name_doc) \
            .withColumnRenamed(name_original_col, new_name_key)

        print('\nReading data')
        self.__df_doc.show(n=2)

        return self

    def append_df(self):
        self.__df_wordid_docid = self.__df_doc \
            .union(self.__df_wordid_docid)

        return self

    def storage_data(self, path_to_storage: str, mode: str):
        """Persist word dict
        :Args:
            :param path_to_storage: the path
            :param format: the format used to save
            :param mode: operation when data already exists.
        :Returns:
            :return: a word dict storage
        """
        self.__df_wordid_docid.show()
        logging.warning(f'total words = {self.__df_wordid_docid.count()}')

        return self.__df_wordid_docid.write.parquet(path=path_to_storage,
                                                    mode=mode)


def main():
    # prepare dataframe docid_wordid
    spark = pyspark.sql.SparkSession(pyspark.SparkContext())
    df_index = spark.createDataFrame(data=[('0', '0')],
                                     schema=('doc_id', 'word_id'))
    df_index.write.parquet(path=PATH_INDEX, mode='append')

    list_docs = os.listdir(PATH_DOCS)

    for doc in list_docs:
        JobMapWordIdDocId(path_files=PATH_DOCS,
                          file_name=doc,
                          path_dict=PATH_DICT,
                          path_index=PATH_INDEX,
                          num_partition=1) \
            .clean_data(list_words_col='value') \
            .generate_word_by_row(col_words='value') \
            .get_word_id(col_words='value',
                         join_operation='right',
                         col_words_dict='col') \
            .generate_wordid_docid(col_word_key='key') \
            .prepare_df(name_original_col='_2',
                        new_name_doc='doc_id',
                        new_name_key='word_id') \
            .append_df()\
            .storage_data(path_to_storage=PATH_INDEX,
                          mode='append')


if __name__ == '__main__':
    main()
