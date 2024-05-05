"""
### Module - CreateJsonSchema for QA Library

#### Class:
    * CreateJsonSchema: Creates the JSON schema for QA Test
"""
import json
import os
from collections import namedtuple


class CreateJsonSchema:
    """
    ### This class is responsible for creating the JSON schema for QA Test. Please use in local environment

    #### Functions:
        * type_converter: This function convert SQL datatype to Pyspark datatype
        * create_json: This function creates JSON schema for QA validations
    """
    def __init__(self):
        pass

    def type_converter(type_value: str, col_name: str = None, numeric_precision: int = None) -> str:
        """
        ### This function convert SQL datatype to Pyspark datatype

        #### Args:
            * col_name (str): Full name of the column
            * type_value (str): name of the ttype of the column in SQL
            * numeric_precision (int, optional): Numeric precision of the field. Defaults to None.

        #### Returns:
            * str: Spark Data Type Name

        #### Raises:
            * ValueError: Data Type Conversion Not Yet Implemented
        """
        if type_value in ("varchar", "nvarchar", "string", "uniqueidentifier", "image", "char", "nchar"):
            return "StringType"
        elif type_value in ("date"):
            return "DateType"
        elif type_value in ("binary", "varbinary"):
            return "BinaryType"
        elif type_value in ("bigint", "long"):
            if col_name == "_sqlid":
                return "StringType"
            else:
                return "LongType"
        elif type_value in ("numeric"):
            if numeric_precision is None or numeric_precision == 0:
                return "LongType"
            else:
                return "DecimalType"
        elif type_value in ("decimal"):
            return "DecimalType"
        elif type_value in ("int", "integer"):
            return "IntegerType"
        elif type_value in ("timestamp", "datetime", "datetime2"):
            return "TimestampType"
        elif type_value in ("byte", "tinyint"):
            return "ByteType"
        elif type_value in ("boolean", "bit"):
            return "BooleanType"
        elif type_value in ("double"):
            return "DoubleType"
        elif type_value in ("float", "real"):
            return "FloatType"
        elif type_value in ("short", "smallint"):
            return "ShortType"
        elif type_value in ("money"):
            return "DecimalType"
        else:
            raise ValueError(f"{type_value} Data Type Conversion Not yet implemented")

    def create_json(self,
                    input_path: str,
                    output_path: str = "tests/qa",
                    tested_layer: str = "bronze",
                    table_schema: str = "ALL",
                    table_prefix: str = "ALL",
                    replace_name: str = "") -> list:
        """
        ### This function creates JSON schema for QA validations

        #### Args:
            * input_path (str): input path for csv file form SQL Server
            * output_path (str, optional): output path for saving local json schema. Defaults to 'tests/qa'.
            * tested_layer (str, optional): Tested layer: bronze/staging. Defaults to 'bronze'.
            * table_schema (str, optional): Table Schema for generating json files. If All generates for all schemas.Defaults to `ALL`.
            * table_prefix (str, optional): Table prefix for filter. Defaults to `ALL`.
            * replace_name (str, optional): if need to replace table_name string for empty. Defaults to ``.

        #### Returns:
            * list: Returns list with table schemas
        """
        text = open(f"{input_path}", "r").read()
        tables_schema = {}
        table_name = None
        text_list = text.split("\n")
        print(table_prefix)

        for idx, value in enumerate(text_list):
            columns = [x if x != "NULL" else None for x in value.split(",")]

            if idx == 0:
                Row = namedtuple("Row", " ".join(columns))
                continue

            if idx + 1 == len(text_list):
                break

            row = Row(*columns)
            col_name = row.COLUMN_NAME
            if table_name == row.TABLE_NAME:
                new_table = False
            else:
                table_name = row.TABLE_NAME
                new_table = True

            # creating tables_schema
            if (table_prefix == "ALL" and (table_schema == "ALL" or table_schema == row.TABLE_SCHEMA)) or (
                table_prefix in row.TABLE_NAME
            ):
                if new_table:
                    tables_schema[f"{row.TABLE_SCHEMA}.{table_name}"] = {}
                if col_name not in ("CreationDate", "PackageName"):
                    data_type = self.type_converter(col_name, row.DATA_TYPE, row.NUMERIC_PRECISION)
                    # if bronze layer is being tested
                    if tested_layer == "bronze":
                        tables_schema[f"{row.TABLE_SCHEMA}.{table_name}"].update({col_name: data_type})
                    # if staging layer is being tested
                    elif tested_layer == "staging":
                        tables_schema[f"{row.TABLE_SCHEMA}.{table_name}"][col_name] = {
                            "data_type": data_type,
                            "character_maximum_length": row.CHARACTER_MAXIMUM_LENGTH,
                            "numeric_precision": row.NUMERIC_PRECISION,
                            "numeric_scale": row.NUMERIC_SCALE,
                        }
        # saving json file
        for k, v in tables_schema.items():
            table_name = k.lower().split(".")
            path = f"{output_path}/{tested_layer}/{table_name[0]}"
            if not os.path.exists(path):
                os.mkdir(path)
            with open((f"{path}/{table_name[1]}.json").replace(f"{replace_name}", ""), "w") as f:
                f.write(json.dumps(v, indent=4))

        return tables_schema
