from typing import Dict

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    collect_list,
    explode,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog
from data_catalog.src.loader.excel_loader import ExcelLoader
from data_catalog.src.cleaner.data_cleaner import DataCleaner


class MapSourcesTablesLoader(BaseDataCatalog):
    def __init__(
        self,
        spark: SparkSession,
        layer_name: str,
        container_name: str,
        folder: str,
    ):
        super().__init__(spark, layer_name)
        self.spark = spark
        self.layer_name = layer_name
        self.container_name = container_name
        self.folder = folder
        self.excel_loader = ExcelLoader(self.spark, self.layer_name, self.container_name, self.folder)
        self.excel_cleaner = DataCleaner(self.spark, self.layer_name)

    @staticmethod
    def map_excel_uc_sources(
        df: DataFrame, key_col_name: str = 'source', value_col_name: str = 'source_raw'
    ) -> Dict:
        """
        Return:
            e.g.:
                {
                 'bloomberg': ['bloomberg'],
                 'compas': ['compas_shipsure_crew', 'compas'],
                 ...
                }
        """
        df_map_sources = df.select(key_col_name, value_col_name).distinct()
        grouped_df = df_map_sources \
            .groupBy(key_col_name) \
            .agg(collect_list(value_col_name).alias(value_col_name))

        return {row[key_col_name]: row[value_col_name]
                for row in grouped_df.collect()}

    def _agg_sources_and_tables(self, df: DataFrame) -> DataFrame:
        """
        Return:
            e.g.:
            +---------+--------------------+--------------------+
            |   source|          source_raw|         tables_list|
            +---------+--------------------+--------------------+
            |   compas|              compas|[crew_contact, cl...|
            |   compas|compas_shipsure_crew|[integration_seaf...|
            ...
        """
        df = df \
            .select('layer', 'source_raw', 'source', 'table')\
            .filter(df['source_raw'].isNotNull())\
            .filter(df['layer'] == self.layer_name) \
            .distinct()

        df_table_list = df \
            .groupBy('source', 'source_raw') \
            .agg(collect_list('table').alias('tables')) \
            .select(*['source', 'source_raw'], explode('tables').alias('tables')) \
            .groupBy('source', 'source_raw') \
            .agg(collect_list('tables').alias('tables_list'))

        return df_table_list.orderBy('source_raw')

    def map_sources_with_tables(self, df_origin_fields) -> Dict:
        """
        Return:
             e.g.:
             {
                'compas': {'compas': ['crew_contact', ...],
                           'compas_shipsure_crew': ['integration_seafarer',
                                                    'integration_service_record',
                                                    'seafarer',
                                                    'service_record']},
                ...
            }
        """
        df = self._agg_sources_and_tables(df_origin_fields)

        dict_source_tables = {}
        for row in df.collect():
            source = row['source']
            source_raw = row['source_raw']
            tables_list = row['tables_list']

            if source not in dict_source_tables:
                dict_source_tables[source] = {}

            if source_raw in dict_source_tables[source]:
                dict_source_tables[source][source_raw].extend(tables_list)
            else:
                dict_source_tables[source][source_raw] = tables_list

        return dict_source_tables

    def execute(self, file_name_catalog: str) -> Dict:
        """
        NOTE: Tables tab contains all layers, sources and tables.

        Return:
            e.g.:
                {'bloomberg': ['bloomberg'],
                ...
                 'imos': ['imos_datalake_api', 'imos_sftp', 'imos_report_api'],
                 'oracle': ['oracle',
                            'oracle_api_fscm',
                            'oracle_sftp_accounting',
                            'oracle_sftp_hr',
                            'oracle_api_hcm'],
                 }
        """
        df_origin = self.excel_loader.execute(
            file_name=file_name_catalog,
            sheet_tab='Tables',
        )
        df_origin = self.excel_cleaner.execute(df_origin, 'tables')

        return self.map_sources_with_tables(df_origin)
