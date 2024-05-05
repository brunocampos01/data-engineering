from typing import (
    Dict,
    Tuple,
    List,
)

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    LongType,
    DateType,
    StructField,
    StructType,
    ArrayType,
    BooleanType,
    ByteType,
    FloatType,
    BinaryType,
    base64,
    DoubleType,
    ShortType,
)


class SchemaHelper:
    """
    A helper class for managing dataframe schema in QA tests
    """
    @staticmethod
    def set_correct_datatype(df_expected: DataFrame, dict_schema_expected: Dict) -> DataFrame:
        """
        When the sqlsever is float, spark return double. 
        To resolve, the data is cast to dataType defined in dict_schema_expected.

        #### Args:
            df_expected (DataFrame): The expected DataFrame. e.g.: 
                SQLServer: doubleType
                +-------------+
                |amt_cancelled|
                +-------------+
                |          1.1|
                |        39.73|
            df_observed (DataFrame): The observed DataFrame.
                Unity Catalog: floatType
                +------------------+
                |     amt_cancelled|
                +------------------+
                | 1.100000023841858|
                | 39.72999954223633|
        
        ### Returns:
            (DataFrame): dataframe with the the same datatype defined in dict_schema_expected json.
                +------------------+
                |     amt_cancelled|
                +------------------+
                | 1.100000023841858|
                | 39.72999954223633|
        """
        float_cols = [key for key, value in dict_schema_expected.items()
                       if value == 'FloatType' and not key.startswith('etl_')]

        for c in df_expected.columns:
            if c in float_cols:
                df_expected = df_expected.withColumn(c, col(c).cast(FloatType()))

        return df_expected

    @staticmethod
    def lowercase_field_names_in_struct_type(struct_type: StructType) -> StructType:
        """
        ### Converts the field names in a StructType to lowercase.
        
        #### Args:
            * struct_type (pyspark.sql.types.StructType): 
                e.g.: StructType([
                        StructField('SECURITIES', StringType(), True), 
                        StructField('ERROR_CODE', IntegerType(), True)
                    ])

        #### Returns:
            * StructType: e.g.: `StructType([StructField('securities', StringType(), True), StructField('error_code', IntegerType(), True)])`
        """
        new_fields = [
            StructField(field.name.lower(), field.dataType, field.nullable)
            for field in struct_type.fields
        ]
        return StructType(new_fields)

    @staticmethod
    def remove_struct_field_in_struct_type(struct_type: StructType, list_field_name_to_remove: list) -> StructType:
        """
        ### Removes specific fields from a StructType schema. This function creates a new StructType schema with the fields from the input `struct_type`, excluding the ones specified in the `list_field_name_to_remove`.

        #### Args:
            * struct_type (pyspark.sql.types.StructType): The original StructType schema.
                e.g.: StructType([
                        ...     StructField("Name", StringType(), True),
                        ...     StructField("Age", IntegerType(), False),
                        ...     StructField("City", StringType(), True),
                        ... ])
            * list_field_name_to_remove (list): e.g.: `["Age", "City"]`

        #### Returns:
            * StructType: e.g.: StructType(List(StructField(Name,StringType,true)))
        """
        new_fields = []
        for field in struct_type.fields:
            if field.name.lower() not in list_field_name_to_remove:
                new_field_name = field.name.lower()
                new_fields.append(
                    StructField(new_field_name, field.dataType, field.nullable)
                )

        return StructType(new_fields)
    
    @staticmethod
    def get_data_type_mapping() -> Dict[str, any]:
        """
        ### Get a dict mapping data type strings to their corresponding PySpark data types.

        #### Args:
            None
        
        #### Returns:
            * dict: dictionary with corresponding PySpark data types
        """
        return {
            "DecimalType(24,4)": DecimalType(24,4),
            "DecimalType(38,10)": DecimalType(38,10),
            "DecimalType(38,0)": DecimalType(38,0),
            "DecimalType(36,15)": DecimalType(36,15),
            "DecimalType(35,15)": DecimalType(35,15),
            "DecimalType(35,6)": DecimalType(35,6),
            "DecimalType(32,10)": DecimalType(32,10),
            "DecimalType(30,0)": DecimalType(30,0),
            "DecimalType(22,0)": DecimalType(22,0),
            "DecimalType(20,4)": DecimalType(20,4),
            "DecimalType(20,0)": DecimalType(20,0),
            "DecimalType(19,6)": DecimalType(19,6),
            "DecimalType(19,4)": DecimalType(19,4),
            "DecimalType(18,10)": DecimalType(18,10),
            "DecimalType(18,6)": DecimalType(18,6),
            "DecimalType(18,2)": DecimalType(18,2),
            "DecimalType(16,3)": DecimalType(16,3),
            "DecimalType(16,2)": DecimalType(16,2),
            "DecimalType(16,0)": DecimalType(16,0),
            "DecimalType(12,6)": DecimalType(12,6),
            "DecimalType(10,2)": DecimalType(10,2),
            "DecimalType(10,0)": DecimalType(10,0),
            "DecimalType(8,2)": DecimalType(8,2),
            "DecimalType(4,0)": DecimalType(4,0),
            "DecimalType(16,4)": DecimalType(16,4),
            "DecimalType(19,2)": DecimalType(19,2),
            "DecimalType(31,2)": DecimalType(31,2),
            "DecimalType(33,4)": DecimalType(33,4),
            "DecimalType(21,2)": DecimalType(21,2),
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "TimestampType": TimestampType(),
            "LongType": LongType(),
            "DateType": DateType(),
            "ArrayType(StringType())": ArrayType(StringType()),
            "ArrayType(StructType([]))": ArrayType(StructType([])),
            "BooleanType": BooleanType(),
            "BinaryType": BinaryType(),
            "ByteType": ByteType(),
            "FloatType": FloatType(),
            "DoubleType": DoubleType(),
            "ShortType": ShortType(),
            "StructType([StructField('_request_api_endpoint', StringType(), True), StructField('_request_source_name', StringType(), True), StructField('_request_requested_at', StringType(), True), StructField('_request_duration', DoubleType(), True), StructField('_request_attempts', IntegerType(), True), StructField('_request_job_id', StringType(), True), StructField('_request_run_id', StringType(), True)])": StructType(),
        }

    @staticmethod
    def prepare_schema(current_struct: StructType) -> StructType:
        """
        ### Simplify the structure type in arrayType to keep same pattern than JSON files.
        
        #### Args:
            * current_struct (StructType): The struct from JSON file.

        #### Returns:
            * StructType: The corresponding PySpark StructType.
        """
        new_fields = []
        for field in current_struct.fields:
            if not isinstance(field.dataType, ArrayType):
                new_fields.append(field)
            else:
                # found arrayType
                if isinstance(field.dataType.elementType, StructType):
                    new_struct_field = StructField(field.name, ArrayType(StructType([])), True)
                    new_fields.append(new_struct_field)
                elif isinstance(field.dataType, ArrayType):
                    new_fields.append(field)

        return StructType(new_fields)

    @staticmethod
    def convert_dict_to_struct_type(json_data: Dict[str, str], data_type_mapping: Dict) -> StructType:
        """
        ### Convert a dict of a schema into a PySpark StructType.

        #### Args:
            * json_data: The dictionary containing field names and their data type strings. 
            e.g.: `{'SECURITIES': 'StringType', 
                    'etl_created_datetime': 'TimestampType'}`

        #### Returns:
            * StructType: The corresponding PySpark StructType.
            e.g.: StructType([
                    StructField('SECURITIES', StringType(), True),
                    StructField('etl_created_datetime', TimestampType(), True)
                ])
        """
        fields = []
        for field_name, data_type_str in json_data.items():
            data_type_str = data_type_str.replace(' ', '')
            try:
                data_type = data_type_mapping[data_type_str]
            except KeyError:
                raise ValueError(f"Data type: {data_type_str} read in {field_name} not exists in the internal mapping. Check the get_data_type_mapping() function.")
                # TODO: add eval func
            else:
                if field_name == 'etl_none_compliant_pattern':
                    field = StructField(field_name, data_type, nullable=False)
                else:
                    field = StructField(field_name, data_type, nullable=True)
                
                fields.append(field)

        return StructType(fields)
    
    @staticmethod
    def get_and_prepare_schemas(dict_schema_expected: Dict, schema_observerd: StructType) -> Tuple[StructType, StructType]:
        """
        ### Prepare schemas doing removing fields and names converting to lowercase.
        """
        schema_helper = SchemaHelper
        data_type_mapping = schema_helper.get_data_type_mapping()

        schema_observed = schema_helper.prepare_schema(schema_observerd)
        schema_observed = schema_helper.lowercase_field_names_in_struct_type(schema_observed)

        schema_expected = schema_helper.convert_dict_to_struct_type(dict_schema_expected, data_type_mapping)
        schema_expected = schema_helper.lowercase_field_names_in_struct_type(schema_expected)

        return schema_observed, schema_expected
