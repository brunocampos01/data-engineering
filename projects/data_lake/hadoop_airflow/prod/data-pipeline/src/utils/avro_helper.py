import io

from fastavro import writer
from fastavro.schema import load_schema


def convert_to_avro(data: dict, compress_type: str, avro_schema: str) -> memoryview:
    fo = io.BytesIO()
    parsed_schema = load_schema(avro_schema)
    writer(fo, parsed_schema, data, codec=compress_type)
    fo.seek(0)
    return fo.getbuffer()


def generate_avro_schema(data_name: str, layer: str, list_dict_cols: list) -> dict:
    """
    list_dict_cols:
        [{'name': 'id', 'type': 'long', 'comment': 'oracle: ear_nro_arquivo'},
         {'name': 'text', 'type': 'string', 'comment': 'oracle: ear_arquivo'}]
    """
    avro_schema = {
        "type": "record",
        "name": data_name,
        "comment": layer,
        "fields": []
    }

    for dict_col in list_dict_cols:
        dict_field = {
            "name": dict_col['name'].upper(),
            "type": dict_col['type'],
            "comment": dict_col['comment']
        }
        avro_schema['fields'].append(dict_field)

    return avro_schema


def generate_avro_schema_from_df(dag_name: str, layer: str, df) -> dict:
    avro_schema = {
        "type": "record",
        "name": dag_name,
        "comment": layer,
        "fields": []
    }

    for col_x in df.dtypes:
        name_col = col_x[0]
        type_col = col_x[1]

        if isinstance(df.schema[name_col].dataType, DecimalType) is True:
            if df.schema[name_col].dataType.precision <= 9:
                type_col = ["int", "null"]
            else:
                type_col = ["long", "null"]

        if isinstance(df.schema[name_col].dataType, TimestampType) is True:
            type_col = ["string", "null"]

        dict_field = {
            "name": name_col.upper(),
            "type": type_col,
            "comment": f"oracle: {col_x[0].upper()}"
        }
        avro_schema['fields'].append(dict_field)

    return avro_schema
