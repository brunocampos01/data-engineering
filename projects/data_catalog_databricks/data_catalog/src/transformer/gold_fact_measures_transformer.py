from typing import (
    Dict,
    List,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    when,
    lit,
)

from data_catalog.src.transformer.add_dw_cols import AddDWCols
from data_catalog.src.utils import get_tag_names


class GoldFactMeasuresTransformer(AddDWCols):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def __get_list_cols_to_document(self, df_base, fact_name):
        list_not_dim = getattr(self, f"get_list_{fact_name}s_tags_not_dim")()
        list_tags = get_tag_names(df_base)

        if fact_name == 'table':
            list_tags.remove('tag_table_last_data_steward')
            list_not_dim.remove('tag_table_last_data_steward')

        return self.__get_list_possible_cols_to_document(fact_name, list_tags, list_not_dim)

    @staticmethod
    def __get_list_possible_cols_to_document(
            fact_name: str, list_tags_names: List[str], list_not_dim: List[str]
    ) -> List[str]:
        """
        Return a list of columns (places) that is possible to fill in.

        Args:
            list_tags_names (List[str]): a list with tags.
                Example: ['tag_source_active_system',
                          'tag_source_csl_internal_system']
                          or
                          ['tag_table_data_steward',
                           'tag_table_frequency_ingestion',
                           'tag_table_type_ingestion']
            list_not_dim (List[str]): a list with tags that will not became a dim.

        Return:
            (list): names of columns that is possible to document. All tags + description
            example:
                ['tag_source_active_system',
                'tag_source_csl_internal_system',
                'source_description']
        """
        return list(set(list_tags_names + list_not_dim + [f"{fact_name}_description"]))

    def __generate_report(
            self, fact_name: str, dict_list_tags: Dict, df: DataFrame, list_cols_to_document: List
    ) -> None:
        getter_method = getattr(self, f'get_list_{fact_name}s_tags_not_dim', None)
        list_not_dim = getter_method()

        getter_method_all = getattr(self, f'get_list_all_{fact_name}s_tags_not_dim', None)
        list_all_not_dim = getter_method_all()

        list_cols_to_doc_total = []
        for name, list_tags in dict_list_tags.items():
            list_cols_to_doc_total.extend(list_tags)
            list_cols_to_doc_total.append(f'{name}_description')

        list_cols_to_doc_total.extend(list_all_not_dim)
        list_cols_to_doc_total = list(set(list_cols_to_doc_total))
        list_cols_to_document.append(f'{name}_description')

        if fact_name != 'field':
            list_cols_to_doc_total.remove('tag_table_last_data_steward')

        total_to_doc = len(list_cols_to_document)
        total_to_doc_total = len(list_cols_to_doc_total)
        self.logger.info(f"************** Fact {fact_name}s **************")
        self.logger.info(f"List of cols to document in {fact_name}: {list_cols_to_document}")
        self.logger.info(f"Total cols to document in {fact_name}: {total_to_doc}")
        self.logger.info(f"List of tags in {fact_name} that are not dimesions: {list_not_dim}")
        self.logger.info(f"List of cols to document: \n{list_cols_to_doc_total}")
        self.logger.info(f"Total cols to document = {total_to_doc_total}")
        self.logger.info(f'Total fact_{fact_name}s = {df.count()}')
        self.logger.info(f"******************************************")
        df.display()

    def __join_all_fact(self, df: DataFrame, dict_df: Dict) -> DataFrame:
        df.createOrReplaceTempView("fact_sources")
        dict_df.get('fact_tables').createOrReplaceTempView("fact_tables")
        dict_df.get('fact_fields').createOrReplaceTempView("fact_fields")
        return self.spark.sql("""
            SELECT
                s.*,
                t.`sk_table`,
                t.`qty_table_document`,
                t.`qty_table_documented`,
                f.`sk_field`,
                f.`qty_field_document`,
                f.`qty_field_documented`
            FROM 
                fact_fields f LEFT JOIN fact_tables t ON f.`sk_table` = t.`sk_table`
                              LEFT JOIN fact_sources s ON f.`sk_source` = s.`sk_source`
        """)

    def __get_null_sources(self, df: DataFrame, df_fact: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("sources")
        df_fact.createOrReplaceTempView("sources_raw")
        df = self.spark.sql("""
            SELECT 
                a.sk_source AS `sk_source_origin`,
                b.*
            FROM sources_raw a LEFT JOIN sources b ON a.sk_source=b.sk_source
        """)
        df = df \
            .withColumn("sk_source", when(col("sk_source").isNull(), col("sk_source_origin")).otherwise(col("sk_source"))) \
            .withColumn("pk_fact_source", when(col("pk_fact_source").isNull(), col("sk_source_origin")).otherwise(col("pk_fact_source"))) \
            .withColumn("qty_source", when(col("qty_source").isNull(), 1).otherwise(col("qty_source")))
        df = df.drop(*['sk_table', 'qty_table_document', 'qty_table_documented',
                       'sk_field', 'qty_field_document', 'qty_field_documented', 'sk_source_origin'])
        df = df.fillna(0)

        return df

    @staticmethod
    def __fill_null(df: DataFrame) -> DataFrame:
        return df \
            .withColumn("total_cols_by_source_by_data_steward",
                        when(col("total_cols_by_source_by_data_steward").isNull(), 0) \
                        .otherwise(col("total_cols_by_source_by_data_steward"))) \
            .withColumn("total_tables_by_source_by_data_steward",
                        when(col("total_tables_by_source_by_data_steward").isNull(), 0) \
                        .otherwise(col("total_tables_by_source_by_data_steward")))

    def __join_tables_and_fields_fact(self, df: DataFrame, dict_df: Dict) -> DataFrame:
        df.createOrReplaceTempView("fact_tables")
        dict_df.get('fact_fields').createOrReplaceTempView("fact_fields")
        return self.spark.sql("""
            SELECT
                t.*,
                f.`sk_field`,
                f.`qty_field_document`,
                f.`qty_field_documented`
            FROM
                fact_fields f LEFT JOIN fact_tables t ON f.`sk_table` = t.`sk_table`
        """)

    def __filter_current_tables(self, df: DataFrame) -> DataFrame:
        # NOTE: we only have cols from silver and gold layer
        df.createOrReplaceTempView("fact_fields_joined_tables")
        df = self.spark.sql("""
            SELECT
                a.*,
                cast(
                    CASE
                        WHEN b.`total_cols_by_table` IS NULL THEN 0
                        ELSE b.`total_cols_by_table`
                    END AS INT
                ) AS `total_cols_by_table`,
                b.`sum_qty_field_documented_by_table`,
                b.`total_cols_by_source_by_data_steward`
            FROM fact_tables a LEFT JOIN fact_fields_joined_tables b ON a.`sk_table` = b.`sk_table`
        """)
        return df.dropDuplicates(['pk_fact_table'])

    def __process_sources(
            self, df_fact: DataFrame, dict_df: Dict, to_doc_fields: int, to_doc_tables: int,
    ) -> DataFrame:
        df = self.add_col_total_tables_by_source(df_fact, dict_df.get('fact_tables'))
        df = self.add_col_sum_qty_table_documented_by_source(df)
        df = df.dropDuplicates(['pk_fact_source'])
        df = self.__join_all_fact(df, dict_df)
        df = self.add_col_total_cols_by_source(df)
        df = self.add_col_sum_qty_field_documented_by_source(df)
        df = self.add_col_qty_to_document_total(df, 'source', to_doc_fields, to_doc_tables)
        df = self.add_col_qty_documented_total(df, 'source')
        df = df.dropDuplicates(['sk_table'])
        df = df.dropDuplicates(['pk_fact_source'])
        df = self.__get_null_sources(df, df_fact)

        return df

    def __process_tables(self, df: DataFrame, dict_df: Dict, to_doc_fields: int) -> DataFrame:
        # Metric rules:
        # The only 'fact_table' and 'fact_source' need to have the qty_total columns
        # fact_fields will use directly the qty_field_document and qty_field_documented
        df = self.__join_tables_and_fields_fact(df, dict_df)
        df = self.add_col_total_cols_by_table(df)
        df = self.add_col_sum_cols_qty_by_table(df)
        df = self.add_col_total_cols_by_source_by_data_steward(df)
        df = self.__filter_current_tables(df)
        df = self.add_col_total_tables_by_source_by_data_steward(df)
        df = self.add_col_qty_to_document_total(df, 'table', to_doc_fields)
        df = self.add_col_qty_documented_total(df, 'table')
        df = self.__fill_null(df)
        return df

    @staticmethod
    def __process_fields(df_fact: DataFrame) -> DataFrame:
        return df_fact

    def execute(
        self,
        fact_name: str,
        df_fact: DataFrame,
        df_base: DataFrame,
        dict_list_tags: Dict,
        dict_df: Dict = {},
    ) -> DataFrame:
        list_cols_to_document = self.__get_list_cols_to_document(df_base, fact_name)
        list_not_dim = getattr(self, f"get_list_{fact_name}s_tags_not_dim")()
        total_to_doc = len(list_cols_to_document)

        df_fact = self.add_qty_col(df_fact, fact_name)
        df_fact = self.add_qty_to_document_col(df_fact, fact_name, total_to_doc)
        df_fact = self.add_qty_documented_col(
            df_fact=df_fact,
            df_dim=dict_df.get(f'dim_{fact_name}s'),
            fact_name=fact_name,
            list_cols_to_document=list_cols_to_document,
            list_not_dim=list_not_dim,
        )

        if fact_name == "source":
            df_fact = self.__process_sources(df_fact=df_fact, dict_df=dict_df, to_doc_fields=11, to_doc_tables=5)

        elif fact_name == "table":
            df_fact = self.__process_tables(df=df_fact, dict_df=dict_df, to_doc_fields=11)

        elif fact_name == "field":
            df_fact = self.__process_fields(df_fact)

        else:
            raise ValueError(f"Unsupported fact_name: {fact_name}")

        df_fact = self.select_only_fact_cols(df_fact, fact_name)
        self.__generate_report(fact_name, dict_list_tags, df_fact, list_cols_to_document)
        return df_fact
