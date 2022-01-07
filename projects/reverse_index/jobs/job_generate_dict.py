import configparser
import datetime
import logging
import os

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import trim
from pyspark.sql.functions import udf

# config
path_name_file = os.path.basename(__file__)
path_directory = os.path.dirname(os.path.abspath(__file__))
path_config = ''.join(path_directory + '/../configs/etl_config.ini')

config = configparser.ConfigParser()
config.read(path_config)

path_docs = config['DOC']['PATH_DOCS']
path_dict = config['DOC']['PATH_DICT']
FORMAT_STORAGE = config['DOC']['FORMAT_STORAGE']
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
logging.warning(f'Start {path_name_file}: {start_time}')


class JobGenerateDict(object):
    def __init__(self, path_files: str):
        self.__spark = SparkSession \
            .builder \
            .getOrCreate()

        self.__spark.sparkContext.setLogLevel("WARN")
        self.__df = self.__spark.read.text(path_files)

    def __del__(self):
        self.__spark.stop()
        end_time = datetime.datetime.now()
        logging.info(f'End   {path_name_file}: {end_time}')
        logging.info(f'Total {path_name_file}: {end_time - start_time}')
        logging.info(79 * '-')

    def clean_data(self, column_words: str) -> pyspark.sql.DataFrame:
        """Pre-processing data
        Processing executed in function:
        - Lower case
        - Words start with letters or whitespace
        - Remove whitespaces into start and final words
        - Remove rows empty
        - Transform each row in list and split row by word

        :Args:
            :param column_words: column's name of Dataframe
        :Returns:
            :return: Dataframe with a word list in each row
        """
        self.__df = self.__df.withColumn(column_words,
                                         lower(col(column_words))) \
            .withColumn(column_words, regexp_replace(str=col(column_words),
                                                     pattern='[^a-z ]',
                                                     replacement='')) \
            .withColumn(column_words, trim(col(column_words))) \
            .filter(self.__df[column_words] != "") \
            .withColumn(column_words, split(column_words, ' '))

        return self

    def generate_df_words(self, column_words: str) -> pyspark.sql.DataFrame:
        """Generate key -> value by word
        This function
        sort by words

        :Args:
            :param column_words: column's name of Dataframe
        :Returns:
            :return: Dataframe processed
        :Samples:
        +-------------+
        |          col|
        +-------------+
        |        those|
        """
        self.__df = self.__df.select(explode(self.__df[column_words])) \
            .drop_duplicates() \
            .orderBy('col', ascending=True)

        return self

    def generate_dict(self,
                      column_key: str,
                      column_words: str) -> pyspark.sql.DataFrame:
        """
        - Create index
        - Rename columns
        - each words is formated how list, the function remove_list_format
        transform list in string

        :Args:
            :param column_key:
            :param column_words:
        :Return:
            dataframe
        """
        remove_list_format = udf(lambda x: ",".join(x))
        self.__df = self.__df.rdd.zipWithIndex()

        self.__df = self.__df.toDF() \
            .withColumnRenamed('_1', column_words) \
            .withColumnRenamed('_2', column_key) \
            .withColumn(column_words, remove_list_format(column_words))

        return self

    def storage_data(self, path_to_storage: str, mode: str) -> None:
        """Persist word dict

        :Args:
            :param path_to_storage: the path
            :param format: the format used to save
            :param mode: operation when data already exists.
        :Returns:
            :return: a word dict storage
        """
        self.__df.write.parquet(path=path_to_storage,
                                mode=mode)
        return self.__df.show(truncate=False)


def main():
    JobGenerateDict(path_files=path_docs) \
        .clean_data(column_words='value') \
        .generate_df_words(column_words='value') \
        .generate_dict(column_key='key', column_words='value') \
        .storage_data(path_to_storage=path_dict, mode='overwrite')


if __name__ == '__main__':
    main()
