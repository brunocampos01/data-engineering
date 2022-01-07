import numpy as np
import pandas as pd
from airflow.models import Variable

from hooks.db.oracle_helper import OracleHelper
from utils.avro_helper import convert_to_avro
from utils.data_helper import decompress_data


def join_pandas_df(id_df: str,
                   pdf: pd.DataFrame,
                   pdf_extra_cols: pd.DataFrame) -> pd.DataFrame:
    return pdf \
        .merge(pdf_extra_cols, on=id_df, how='left')


# TODO: (Parallelize pandas) - http://blog.adeel.io/2016/11/06/parallelize-pandas-map-or-apply/
def pandas_preprocess_data_doc(pdf: pd.DataFrame,
                               table_blob_col_pk: str,
                               table_blob_col_blob: str,
                               compress_type: str,
                               avro_schema: str) -> memoryview:
    pdf[table_blob_col_blob] = pdf[table_blob_col_blob].map(lambda row: decompress_data(row.read()))
    pdf.rename(columns={
        table_blob_col_pk: 'id',
        table_blob_col_blob: 'text'
    }, inplace=True)
    return convert_to_avro(data=pdf.to_dict(orient='records'),
                           compress_type=compress_type,
                           avro_schema=avro_schema)


def get_pandas_df_to_process(oracle_conn_id: str,
                             oracle_conn_blob: str,
                             query_blob: str,
                             table_blob_col_pk: str,
                             table_blob_col_blob: str,
                             extra_cols: str,
                             current_dag_name: str,
                             date: str) -> pd.DataFrame:
    df = OracleHelper(oracle_conn_blob) \
        .get_pandas_df(query_blob)

    df = df[
        [table_blob_col_pk, table_blob_col_blob]
    ]

    if len(extra_cols) > 0:
        query_extra_col = Variable.get(f'{current_dag_name}_sql_extra_cols_{date}')
        df_extra_cols = OracleHelper(oracle_conn_id) \
            .get_pandas_df(sql=query_extra_col)

        return join_pandas_df(pdf=df,
                              pdf_extra_cols=df_extra_cols,
                              id_df=table_blob_col_pk.upper())

    return df
