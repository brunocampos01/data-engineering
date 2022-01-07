import json
import os


def generate_dag_config(path_file: str, pwd: str) -> None:
    data = {
        "paths": {
            "path_local_avro_schemas": f"{pwd}/avro_schemas",
            "path_libs": "hdfs:///user/bigdata/libs/",
            "path_native_lib": "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1580995/lib/hadoop/lib/native",
            "path_hdfs": {
                "path_avro_schemas": "/user/hive/avro_schemas",
                "path_layer_raw": "/data/prod/raw",
                "path_layer_pre_processed": "/data/prod/pre_processed"
            }
        },
        "libs": {
            "ojdbc": "ojdbc8.jar",
            "spark_redis": "spark-redis/target/spark-redis_2.11-2.6.0-SNAPSHOT-jar-with-dependencies.jar",
            "spark_avro": "spark-avro_2.12-3.0.1.jar",
            "spark_xml": "com.databricks_spark-xml_2.12-0.11.0.jar",
            "avro_tools": "avro-tools-1.10.1.jar"
        }
    }

    with open(path_file, "w") as json_file:
        json.dump(data, json_file, indent=4)


def main():
    pwd = os.environ.get("PWD")
    path_file = f'{pwd}/configs/dag_config.json'
    generate_dag_config(path_file=path_file, pwd=pwd)

    print(f'\nfile: {path_file} generated.')


if __name__ == '__main__':
    main()
