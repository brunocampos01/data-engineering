import gzip
import logging
import math
import zipfile
from io import BytesIO
from typing import Optional

import pyspark
import pyspark.sql.functions as pf
from hooks.db.oracle_helper import OracleHelper
from lxml import etree
from pyspark.sql.functions import col
from pyspark.sql.functions import rtrim
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import ByteType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ShortType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from utils.avro_helper import generate_avro_schema
from utils.avro_helper import generate_avro_schema_from_df
from utils.init_spark import init_spark


def decompress_data(data: bytearray) -> str:
    try:
        with gzip.open(BytesIO(data), 'rb') as f:
            return f.read().decode('UTF8')

    except UnicodeDecodeError:
        logging.error(f'OSError: gzip file not encode UTF8, try with ISO-8859-1')
        with gzip.open(BytesIO(data), 'rb') as f:
            return f.read().decode('ISO-8859-1')

    except OSError:
        try:
            logging.error(f'OSError: file is not gzip, try open as zip with ISO-8859-1')
            with zipfile.ZipFile(BytesIO(data)) as zip_file:
                zip_file.printdir()
                with zip_file.open(zip_file.namelist()[0]) as f:
                    return f.read().decode(encoding='ISO-8859-1')

        except Exception as err:
            logging.error(f'File not compress', err)
            with open(data, 'r') as f:
                return f.read()


@udf(returnType=StringType())
def udf_decompress_data(data: bytearray) -> str:
    try:
        with gzip.open(BytesIO(data), 'rb') as f:
            return f.read().decode('UTF8')

    except UnicodeDecodeError:
        logging.error(f'OSError: gzip file not encode UTF8, try with ISO-8859-1')
        with gzip.open(BytesIO(data), 'rb') as f:
            return f.read().decode('ISO-8859-1')

    except OSError:
        try:
            logging.error(f'OSError: file is not gzip, try open as zip with ISO-8859-1')
            with zipfile.ZipFile(BytesIO(data)) as zip_file:
                zip_file.printdir()
                with zip_file.open(zip_file.namelist()[0]) as f:
                    return f.read().decode('ISO-8859-1')

        except Exception as err:
            logging.error(f'File not compress', err)
            with open(data, 'r') as f:
                return f.read()


def _map_type_python_to_spark(list_dict_cols: list) -> pyspark.sql.types.StructType:
    list_types_schema = []

    for col in list_dict_cols:
        if col['type'] == 'string':
            if col['name'].startswith('DT_'):
                list_types_schema.append(StructField(col['name'], TimestampType(), True))
            else:
                list_types_schema.append(StructField(col['name'], StringType(), True))
        if col['type'] == 'boolean':
            list_types_schema.append(StructField(col['name'], BooleanType(), True))
        if col['type'] == 'double':
            list_types_schema.append(StructField(col['name'], DoubleType(), True))
        if col['type'] == 'bigint':
            list_types_schema.append(StructField(col['name'], LongType(), True))
        if col['type'] == 'int':
            list_types_schema.append(StructField(col['name'], IntegerType(), True))
        if col['type'] == 'smallint':
            list_types_schema.append(StructField(col['name'], IntegerType(), True))
        if col['type'] == 'tinyint':
            list_types_schema.append(StructField(col['name'], IntegerType(), True))

    return StructType(list_types_schema)


def convert_type_oracle_to_spark(spark: 'pyspark.sql.session.SparkSession',
                                 records: list,
                                 list_dict_cols: list) -> pyspark.sql.dataframe.DataFrame:
    schema = _map_type_python_to_spark(list_dict_cols)
    return spark.createDataFrame(records, schema)


def map_precision_col_df(df: pyspark.sql.dataframe.DataFrame,
                         col_name: str) -> pyspark.sql.types.DataType:
    if isinstance(df.schema[col_name].dataType, DecimalType) is True:
        data_precision = df.schema[col_name].dataType.precision
        if data_precision <= 2:
            return ByteType()
        elif 2 < data_precision <= 4:
            return ShortType()
        elif 4 < data_precision <= 9:
            return IntegerType()
        else:
            return LongType()

    return StringType()


def preprocess_data_table(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    for col_name in df.columns:
        if isinstance(df.schema[col_name].dataType, StringType) is True:
            df = df \
                .withColumn(col_name, rtrim(col(col_name)))

        if isinstance(df.schema[col_name].dataType, TimestampType) is True:
            df = df \
                .withColumn(col_name, col(col_name).cast('string'))

    return df


def preprocess_data_doc(df: pyspark.sql.dataframe.DataFrame,
                        col_type_pk: str,
                        table_blob_col_pk: str,
                        table_blob_col_blob: str) -> pyspark.sql.dataframe.DataFrame:
    convert_type = LongType() if 'long' in col_type_pk else StringType()
    return df \
        .withColumn(table_blob_col_blob,
                    udf_decompress_data(col(table_blob_col_blob))) \
        .withColumn(table_blob_col_pk,
                    col(table_blob_col_pk).cast(convert_type)) \
        .withColumn(table_blob_col_blob,
                    col(table_blob_col_blob).cast(StringType())) \
        .withColumnRenamed(table_blob_col_pk, 'id') \
        .withColumnRenamed(table_blob_col_blob, 'text')


def join_pyspark_df(df: pyspark.sql.dataframe.DataFrame,
                    df_extra_cols: pyspark.sql.dataframe.DataFrame,
                    id_df: str) -> pyspark.sql.dataframe.DataFrame:
    return df \
        .join(df_extra_cols, on=id_df, how='left') \
        .coalesce(1)


@udf(returnType=ArrayType(StringType()))
def udf_break_into_list(text: str) -> list:
    """
    input:
        text = <retProc NSU=\"000000722413426\" NSUAN=\"000007047235052\"> ...
    output:
        ['<retProc/>\\"000000722413426\\" NSUAN=\\"000007047235052\\"&gt;\n    ', ... ]
    """
    list_tag_lote = ['loteDistFile_name', 'loteConsNSUFalt']

    # Add tag root in everything
    fixed_text = '<root>' + text + '</root>'
    list_root = list(etree.fromstring(fixed_text, parser=etree.XMLParser(recover=True)))

    # If xml has only 1 element, open again
    if list_tag_lote:
        contain_tag = list_root[0].tag in list_tag_lote
        if len(list_root) == 1 & contain_tag:
            list_root = list(list_root[0])

    return [etree.tostring(item, encoding='unicode') for item in list_root]


def pre_process_content(df: pyspark.sql.dataframe.DataFrame,
                        table_blob_col_blob: str,
                        result_col_name: str) -> pyspark.sql.dataframe.DataFrame:
    return df \
        .withColumn(table_blob_col_blob,
                    pf.explode(udf_break_into_list(col(table_blob_col_blob)))) \
        .withColumnRenamed(table_blob_col_blob, result_col_name)


def remove_namespaces(text) -> str:
    """
    input:
        <proc schema="procEventoCTe_v3.00.xsd" ipTransmissor="10.64.111.123">
            <procEventoCTe xmlns="http://www.portalfiscal.inf.br/cte" versao="3.00">
                ...
    output:
        <proc schema="procEventoCTe_v3.00.xsd" ipTransmissor="10.64.111.123">
            <procEventoCTe versao="3.00">
                ...
    """
    return pf.regexp_replace(text, ' xmlns=".*?"', '')


def remove_spaces(text) -> str:
    """
    input:
        <proc  schema="procEventoCTe_v3.00.xsd"       ipTransmissor="10.64.111.123  ">
    output:
        <proc schema="procEventoCTe_v3.00.xsd" ipTransmissor="10.64.111.123">
    """
    return pf.regexp_replace(text, '  ', ' ')


@udf(returnType=StringType())
def udf_remove_signature(text) -> str:
    """
    input:
        <eventoCTe versao="3.00">
                ...
                <Signature>
                    ...
                </Signature>
        </eventoCTe>
    output:
        <eventoCTe versao="3.00">
                ...
        </eventoCTe>
    """
    row = etree.fromstring(text, parser=etree.XMLParser(recover=True))

    for sig in row.findall('.//Signature'):
        sig.getparent().remove(sig)

    return etree.tostring(row, encoding='unicode')


def cleansing_data(df: pyspark.sql.dataframe.DataFrame,
                   result_col_name: str,
                   pre_processed: str) -> pyspark.sql.dataframe.DataFrame:
    return df \
        .withColumn(result_col_name,
                    udf_remove_signature(col(result_col_name))) \
        .withColumn(result_col_name,
                    remove_namespaces(col(pre_processed))) \
        .withColumn(result_col_name,
                    remove_spaces(col(result_col_name)))


def calculate_partitions(total_registry: int, max_registry_by_avro: int) -> int:
    """
    :return: total partitions
    """
    return math.ceil(total_registry / max_registry_by_avro)


def check_schema(check_columns_in_order: bool,
                 left_df: pyspark.sql.DataFrame,
                 right_df: pyspark.sql.DataFrame) -> None:
    if check_columns_in_order:
        assert left_df.dtypes == right_df.dtypes, f"df schema type mismatch" \
                                                  f"\nleft_df:{left_df.printSchema()}" \
                                                  f"\nright_df:{right_df.printSchema()}"
    else:
        assert \
            sorted(left_df.dtypes, key=lambda x: x[0]) \
            == sorted(right_df.dtypes, key=lambda x: x[0]), f"df schema type mismatch" \
                                                            f"\nleft_df:{left_df.printSchema()}" \
                                                            f"\nright_df:{right_df.printSchema()}"


def check_total_rows(left_df: pyspark.sql.DataFrame,
                     right_df: pyspark.sql.DataFrame) -> None:
    left_df_count = left_df.count()
    right_df_count = right_df.count()
    assert left_df_count == right_df_count, \
        f"Number of rows are not same.\n\n" \
        f"Actual Rows (left_df): {left_df_count}\n" \
        f"Expected Rows (right_df): {right_df_count}\n"


def check_df_content(left_df: pyspark.sql.DataFrame,
                     right_df: pyspark.sql.DataFrame) -> None:
    logging.info('Executing: left_df - right_df')
    df_diff_oracle_raw = left_df.subtract(right_df)
    logging.info(df_diff_oracle_raw.count())

    logging.info('Executing: right_df - left_df')
    df_diff_raw_oracle = right_df.subtract(left_df)
    logging.info(df_diff_raw_oracle.count())

    assert left_df.subtract(right_df).count() == right_df.subtract(left_df).count() == 0


def assert_pyspark_df_equal(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    check_data_type: bool,
    check_cols_in_order: bool = True,
    order_by: Optional[str] = None
) -> None:
    """
    left_df: destination layer, ex: raw
    right_df: origin layer, ex: oracle
    """
    # Check data types
    if check_data_type:
        check_schema(check_cols_in_order, left_df, right_df)

    # Check total rows
    check_total_rows(left_df, right_df)

    # Sort df
    if order_by:
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    # Check dataframe content
    check_df_content(left_df, right_df)


def get_pyspark_df_to_process(
    oracle_conn_id: str,
    oracle_conn_blob: str,
    oracle_driver: str,
    spark: pyspark.sql.session.SparkSession,
    n_partitions: int,
    query_blob: str,
    table_blob_col_pk: str,
    table_blob_col_blob: str,
    current_dag_name: str,
    extra_cols: str,
    date: str
) -> pyspark.sql.dataframe.DataFrame:
    df = OracleHelper(oracle_conn_blob) \
        .get_pyspark_df_from_table(oracle_driver=oracle_driver,
                                   spark=spark,
                                   table=f'({query_blob})',
                                   partition_col='COL_PARTITION',
                                   n_partitions=n_partitions * 5) \
        .select(table_blob_col_pk,
                table_blob_col_blob)

    if len(extra_cols) > 0:
        query_extra_col = Variable.get(f'{current_dag_name}_sql_extra_cols_{date}')
        df_extra_cols = OracleHelper(oracle_conn_id) \
            .get_pyspark_df(spark=spark,
                            oracle_driver=oracle_driver,
                            sql=query_extra_col)

        return join_pyspark_df(df=df,
                               df_extra_cols=df_extra_cols,
                               id_df=table_blob_col_pk)

    return df


def prepare_avro_schema(
    layer: str,
    data_name: str,
    template: str,
    path_ojdbc: str,
    path_native_lib: str,
    doc_type: str,
    list_dict_cols: list
) -> dict:
    if doc_type != 'table_test':
        return generate_avro_schema(data_name=data_name,
                                    layer=layer,
                                    list_dict_cols=list_dict_cols)

    "Generate dynamic avro schema from tables"
    spark, sc = init_spark(
        app_name='generate_data_schema',
        step='generate_data_schema',
        dag_name=data_name,
        layer=layer,
        env=env,
        path_ojdbc=path_ojdbc,
        path_native_lib=path_native_lib,
        executor_cores='4',
        executor_memory='4g',
        executor_instances='2',
        driver_memory='1g'
    )

    logging.info(f'\n{template}\nGetting data from Oracle\n{template}')
    df_oracle_table = OracleHelper(context['oracle_conn_table']) \
        .get_pyspark_df(spark=spark,
                        oracle_driver='oracle.jdbc.driver.OracleDriver',
                        sql=f'SELECT * FROM {data_name}')
    df_preprocessed = preprocess_data_table(df_oracle_table)

    return generate_avro_schema_from_df(dag_name=data_name,
                                        layer=layer,
                                        df=df_preprocessed)
