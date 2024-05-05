# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Debug
dbutils.widgets.text("landing_data", "")
dbutils.widgets.text("bronze_data", "bloomberg/ld_currency_daily/delta")
dbutils.widgets.dropdown("format", "csv", ["csv", "parquet", "json", "xml", "csv_bloomberg"])
dbutils.widgets.text("primary_keys", "")
dbutils.widgets.text("datatypes_definition_file", "")
dbutils.widgets.text("options", "{}")
dbutils.widgets.text("source_identifier", "")
dbutils.widgets.text("file_name_origin", "")
#dbutils.widgets.text("response_content", "true")
# dbutils.widgets.text("encrypted_columns","")
dbutils.widgets.text("row_tag", "")

# COMMAND ----------

# DBTITLE 1,Declare Imports
import re
import os
import time
from datetime import datetime
import json

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from great_expectations.core import ExpectationValidationResult
from great_expectations.core.expectation_suite import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from pyspark.sql.functions import *
from pyspark.sql.types import *

from library.logger_provider import LoggerProvider
from library.datalander.defaults import Defaults as LanderDefaults
from library.datalake_storage import DataLakeStorage
from library.dataloader.bronze_data_loader import BronzeDataLoader
from library.dataloader.defaults import Defaults
from library.dataloader.hash_key_generator import HashKeyGenerator
from library.great_expectations.extended_spark_dataset import ExtendedSparkDataset
from library.great_expectations.util import add_custom_result_to_validation
from library.sensitive import (
    decrypt_column,
    decrypt_array_column,
    decrypt_nested_array_column_using_secret
)
from library.qa.utils import find_df_diff

from notebooks.bronze.schema import SchemaDictionary

# COMMAND ----------

# DBTITLE 1,Declare Parameters
landing_data = dbutils.widgets.get("landing_data")
bronze_data = dbutils.widgets.get("bronze_data")
input_format = dbutils.widgets.get("format")
if(input_format=='xml'):
    row_tag = dbutils.widgets.get("row_tag")
else:
    row_tag = ''

file_name_origin = dbutils.widgets.get("file_name_origin")

timestamp_columns_ = []
sensitive_columns_ = None
source_settings_ = None
identify_deleted_rows_ = True

datatypes_definition_file = dbutils.widgets.get("datatypes_definition_file")

# making the list of columns to decrypt
try:
    encrypted_columns = dbutils.widgets.get("encrypted_columns")
    columns_to_decrypt = [column.strip() for column in encrypted_columns.split(",")]
    print("Columns to Decrypt: ", columns_to_decrypt)
    # seting sensitive-landing container
    landing_container = "landing-sensitive"
except Exception:
    columns_to_decrypt = []
    # seting landing container
    landing_container = "landing"
    
# making source primary keys a list
try:
    primary_keys = [x.strip() for x in dbutils.widgets.get("primary_keys").split(",") if x]
except:
    primary_keys = []
    
# options variable
try:
    options = dbutils.widgets.get("options")
    options = json.loads(options)
    print("Options: " +str(options))
except Exception as e:
    options = {}
    print("no options configured! \n" + str(e))

# options variable
try:
    source_identifier = dbutils.widgets.get("source_identifier")
except:
    source_identifier = ''
    print("no source_identifier!")

try:
    if dbutils.widgets.get("response_content") == 'true':
        response_content = True
    else:
        response_content = False
except:
    response_content = False
    
# Try to build the full storage path, so we don't need to rely on the mounting points
DataLakeStorage.add_storage_account()

logger = LoggerProvider.get_logger()

landing_data = DataLakeStorage.get_storage_url(landing_container, landing_data)
bronze_data = DataLakeStorage.get_storage_url("bronze", bronze_data)

print(f"Landing Path: {landing_data}")
print(f"Bronze Path: {bronze_data}")
print("Primary Keys -> " + str(primary_keys))

source_name = landing_data.split("/")[-1] #bringing the repository name for LANDING
bronze_name = bronze_data.split("/")[-2] #bringing the repository name for BRONZE
#bronze_folder = bronze_data.split("/")[-3]
parts = bronze_data.split("/")
container = parts[2].split("@")[-1]
path = "/".join(parts[3:])
bronze_folder = path.split(bronze_name)[0] 
bronze_folder = bronze_folder[:-1]
displayHTML(f'<h2> Testing Object: {source_name} / {bronze_name} </h2>')

# COMMAND ----------

# DBTITLE 1,Load datatypes definition
datatypes = {}

# if a datatypes definition file was informed
if datatypes_definition_file:
    current_path = os.getcwd()
    with open(f"{current_path}{datatypes_definition_file}", "r") as f:
        datatypes = json.load(f)

datatypes    


# COMMAND ----------

def _flatten(df, sep="_"):
    # compute Complex Fields (Arrays, Structs and Maptypes) in Schema
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
            or type(field.dataType) == StructType
            or type(field.dataType) == MapType
        ]
    )

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col(col_name + "." + k).alias(col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)


        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        elif type(complex_fields[col_name]) == MapType:
            keys_df = df.select(explode_outer(map_keys(col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(
                map(
                    lambda f: col(col_name).getItem(f).alias(str(col_name + sep + f)),
                    keys,
                )
            )
            drop_column_list = [col_name]
            df = df.select(
                [
                    col_name
                    for col_name in df.columns
                    if col_name not in drop_column_list
                ]
                + key_cols
            )

            # recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == ArrayType
                or type(field.dataType) == StructType
                or type(field.dataType) == MapType
            ]
        )
    return df


# COMMAND ----------

# DBTITLE 1,Loading landing data
try:
    if (input_format in ("json", "xml", "nested_json", "nested_xml") ) or ( input_format == 'csv' and 'bloomberg' in bronze_data ):
        table_settings = SchemaDictionary.get_table_schema(source_identifier)

        # if xml file format, needs to pass through BronzeDataLoader
        if input_format == "xml":
            landing_df = (
                spark.read.format('com.databricks.spark.xml')
                .schema(table_settings['schema'])
                .option('rowTag', row_tag)
                .options(**options)
                .load(landing_data)
            )
            landing_df = _flatten(landing_df)

        elif input_format == "nested_json":
            landing_df = spark.read.format("json") \
                            .options(**options) 
            if table_settings['schema'] is not None:
                landing_df = landing_df.schema(table_settings['schema'])
            landing_df = landing_df.load(landing_data).drop("etl_src_extract_process_meta").drop(*(LanderDefaults.req_api_endpoint,
                            LanderDefaults.req_source_name,
                            LanderDefaults.req_requested_at,
                            LanderDefaults.req_duration,
                            LanderDefaults.req_attempts,
                            LanderDefaults.req_job_id,
                            LanderDefaults.req_run_id))
            landing_df = _flatten(landing_df)
        else:
            landing_df = spark.read.format(input_format) \
                            .options(**options) 
            if table_settings['schema'] is not None:
                landing_df = landing_df.schema(table_settings['schema'])
            landing_df = landing_df.load(landing_data).distinct()

        if response_content == True:
            landing_df = landing_df.selectExpr("explode(response_content) as data") \
                                   .selectExpr("data.*")
    elif input_format == 'csv' and 'pbcs' in bronze_data:
        landing_df = spark.read.format(input_format).options(**options).load(landing_data).distinct()
    else:
        table_settings = SchemaDictionary.get_table_schema(source_identifier)
        landing_df = spark.read.format(input_format).options(**options).schema(table_settings['schema']).load(landing_data).distinct()
        if options == {}:
            display(landing_df)
    print(f'landing rows amount: {landing_df.count()}')

    # Replace blanks with underscores in the column names
    for col_name in landing_df.columns:
        if col_name.strip() != '':
            new_col_name = col_name.strip().replace(' ', '_')  # Replace spaces with underscores
            if new_col_name != col_name:  # Only change the name if there are changes
                landing_df = landing_df.withColumnRenamed(col_name, new_col_name)

    # Replace special characters in the column names
    for col_name in landing_df.columns:
        if col_name.strip() != '':
            new_col_name = re.sub(r'[^a-zA-Z0-9_#@$]', '', col_name.strip())  # Replace special characters with blanks
            if new_col_name != col_name:  # Only change the name if there are changes
                landing_df = landing_df.withColumnRenamed(col_name, new_col_name)
  
    # applying Default columns to landing dataframe
    if(input_format =="csv_bloomberg"):
        # add tracking columns to the dataframe
        logger.debug("Adding tracking columns")
        for new_column in Defaults.tracking_columns('_metadata' in landing_df.columns):
            landing_df = landing_df.withColumn(new_column.column_name, new_column.column_expression)
        if "_metadata" in df.columns:
            landing_df = landing_df.drop("_metadata")
    else:
        for new_column in Defaults.tracking_columns():
            landing_df = landing_df.withColumn(new_column.column_name, new_column.column_expression)
    
    # drop duplicates
    landing_df = landing_df.na.drop(how="all").drop("etl_src_extract_process_meta").dropDuplicates()
    print(f'landing rows amount after Drop Duplicate: {landing_df.count()}')
    
    # drop row that are empty lines
    landing_df = landing_df.na.drop(how="all")

    tracking_column_names = [column.column_name for column in Defaults.tracking_columns()]
    distinct_columns = [column_name for column_name in landing_df.columns if column_name not in tracking_column_names]

    # distinct row that are equal
    landing_df = landing_df.distinct()

    # check null primary key columns
    if len(primary_keys) > 0:
        joined_pk = ', '.join(primary_keys)
        print('partition by --> ' + joined_pk)

        if 'imos_report_api' in bronze_folder and 'cp_laytimedetails' in source_identifier:
            print(landing_df.columns)
            if 'cargo_id13' in landing_df.columns:
                landing_df = landing_df.withColumnRenamed("cargo_id13", "cargo_id")
            if 'cargo_id15' in landing_df.columns:
                landing_df = landing_df.withColumnRenamed("cargo_id15", "cargo_id1")

            print(landing_df.columns)

        count = landing_df.selectExpr("*", f"cast(regexp_replace({Defaults.file_name_column}, '[^0-9]+', '') as bigint) as file_date") \
                        .selectExpr("*", f"row_number() over(partition by {joined_pk} order by file_date desc) as rn") \
                        .filter('rn = 1') \
                        .drop('file_date', 'rn') \
                        .count()

        print('landing rows amount candidate to deduplicate by pk: {}'.format(count))

except Exception as e:
    print("Error while loading data: {}".format(str(e)))
    landing_df = None


#landing_df.orderBy("last_update_dt", ascending=False).display()

# COMMAND ----------

# DBTITLE 1,Apply trim to the column names
# Apply trim to the column names
for col_name in landing_df.columns:
    print(col_name)
    if(col_name.strip()!=''):
        new_col_name = col_name.strip() # Eliminate blank spaces at the beginning and end of the column names
        if new_col_name != col_name: # Only change the name if there are changes
            landing_df = landing_df.withColumnRenamed(col_name, new_col_name)
    else:
        print(col_name)

# COMMAND ----------

# DBTITLE 1,Loading bronze data
bronze_df = spark.read.format("delta").load(bronze_data).persist()
landing_df = landing_df.selectExpr([f'`{x}` as `{x.replace(" ","_").replace("-", "_").lower()}`' for x in landing_df.columns])

landing_df = landing_df.distinct()

bronze_df = bronze_df.select([x.lower() for x in bronze_df.columns])
# Using equals condition
if file_name_origin != "":
    bronze_df = bronze_df.filter(bronze_df.etl_file_name == file_name_origin)
 
landing_count = landing_df.distinct().count() #Computing number of rows amount for LANDING.

print(f'bronze rows amount: {bronze_df.count()}')
print(f'landing rows amount: {landing_df.count()}')

# COMMAND ----------

# DBTITLE 1,Prepare great expectations context
# decrypting sensitive columns for bronze_df
for column in columns_to_decrypt:
    decrypted_column = f"decrypted_{column}"
    if len(column.split('.')) > 1 and len(column.split('.')) < 2:
        bronze_df = bronze_df.withColumn(decrypted_column, decrypt_array_column(column)) \
                             .drop(column) \
                             .withColumnRenamed(decrypted_column, column)
    elif len(column.split('.')) > 2:
        secret_key_expression = 'ADLS-magellan-encryption-key'
        bronze_df = bronze_df.withColumn(decrypted_column, decrypt_nested_array_column_using_secret(bronze_df, column, secret_key_expression)) \
                             .drop(column) \
                             .withColumnRenamed(decrypted_column, column)
    elif len(column.split('.')) == 1:
        bronze_df = bronze_df.withColumn(decrypted_column, decrypt_column(column)) \
                             .drop(column) \
                             .withColumnRenamed(decrypted_column, column)

# filtering etl_deleted_flag rows
print(f'bronze deleted_flag rows amount: {bronze_df.filter(bronze_df.etl_deleted_flag != "false").count()}')
bronze_df = bronze_df.filter(bronze_df.etl_deleted_flag == "False")

# dropping corrupt columns
if "_rescued_data" in bronze_df.columns:
    bronze_df = bronze_df.drop("_rescued_data")
if "_corrupt_record" in bronze_df.columns:
    bronze_df = bronze_df.drop("_corrupt_record")
if "_corrupt_record" in landing_df.columns:
    landing_df = landing_df.drop("_corrupt_record")

landing_df = landing_df.distinct()

landing_count = landing_df.distinct().count() #Computing number of rows amount for LANDING.
    
# list for dropping some fields for GE dataframe
gdf_drop_list = [Defaults.etl_hash_key_pk, Defaults.deleted_flag_column]
    
# creating GE dataframe pyspark
for column in gdf_drop_list:
    if column in bronze_df.columns:
        ge_df = bronze_df.drop(column)
ge_df = ge_df.transform(HashKeyGenerator.generate_key, pk_columns = primary_keys)

#print(bronze_df.display())
#print(landing_df.display())
# GE dataframe
gdf = ExtendedSparkDataset(ge_df)
print(f'bronze rows amount: {bronze_df.count()}')
print(f'landing rows amount: {landing_df.count()}')

# COMMAND ----------

# DBTITLE 1,prepare dataframes for comparison
# Removing new columns added during loading to bronze
tracking_columns = Defaults.technical_column_names()

if "etl_src_extract_process_meta" in bronze_df.columns:
    tracking_columns+=['etl_src_extract_process_meta']
print('tracking columns --> ' + str(tracking_columns))

landing_df = landing_df.transform(HashKeyGenerator.generate_key, pk_columns = primary_keys)

# Removing columns ilegible from landing
if input_format == 'csv':
    remove_from_landing = [col for col in landing_df.columns if col.startswith('_c')]
else: 
    remove_from_landing = []

if len(remove_from_landing) > 0:
    cleaned_landing_df = landing_df.drop(*remove_from_landing)
else:
    cleaned_landing_df = landing_df
cleaned_bronze_df = bronze_df.drop(*tracking_columns)

if 'imos_report_api' in bronze_folder and 'cp_voyagedetails' in source_identifier:
    if 'port_func' in cleaned_landing_df.columns:
        cleaned_landing_df = cleaned_landing_df.withColumnRenamed("port_func", "port_function")

# check the number of distinct values before splitting columns
count_expressions = [count_distinct(column_name).alias(column_name) for column_name in cleaned_landing_df.columns]
distinct_value_counts_for_landing = cleaned_landing_df.select(*count_expressions).head()

# distinct count of the source key columns
if len(primary_keys) > 0:
    expected_key_count = cleaned_landing_df.select(*primary_keys).distinct().count()

# cast numeric columns to string to be able to compare null values
for column in cleaned_bronze_df.schema:
    type_name = column.dataType.typeName()
    if type_name in ['decimal','float', 'double']:
        cleaned_landing_df = cleaned_landing_df.withColumn(column.name, col(column.name).cast(DecimalType(38,10)))
        cleaned_bronze_df = cleaned_bronze_df.withColumn(column.name, col(column.name).cast(DecimalType(38,10)))

        if 'imos_report_api' == bronze_folder:
            cleaned_landing_df = cleaned_landing_df.withColumn(column.name, col(column.name).cast(DecimalType(16,2)))
            cleaned_bronze_df = cleaned_bronze_df.withColumn(column.name, col(column.name).cast(DecimalType(16,2)))

    elif type_name in ['integer']:
        cleaned_landing_df = cleaned_landing_df.withColumn(column.name, col(column.name).cast(IntegerType()))
        cleaned_bronze_df = cleaned_bronze_df.withColumn(column.name, col(column.name).cast(IntegerType()))
    elif type_name in ['long']:
        cleaned_landing_df = cleaned_landing_df.withColumn(column.name, col(column.name).cast(LongType()))
        cleaned_bronze_df = cleaned_bronze_df.withColumn(column.name, col(column.name).cast(LongType()))

# convert timestamp types
for column in cleaned_bronze_df.schema:
    type_name = column.dataType.typeName()
    if type_name == 'date':
        cleaned_landing_df = cleaned_landing_df.withColumn(column.name, to_date(col(column.name)))
    elif type_name == 'timestamp' and column.name != Defaults.deleted_flag_column:
        cleaned_landing_df = cleaned_landing_df.withColumn(column.name, to_timestamp(col(column.name)))

# don't expect the same hash keys, because we are not working with the same datatypes
cleaned_bronze_df = cleaned_bronze_df.drop(Defaults.etl_hash_key_pk).persist()
cleaned_landing_df = cleaned_landing_df.drop(Defaults.etl_hash_key_pk).persist()

print(f'cleaned bronze rows amount: {cleaned_bronze_df.count()}')
print(f'cleaned landing rows amount: {cleaned_landing_df.count()}')

# COMMAND ----------

# MAGIC %md ### Preparing join dataframes

# COMMAND ----------

# creating join dataframes with fill na
join_bronze_df = cleaned_bronze_df
join_landing_df = cleaned_landing_df

# creating list for join
if len(primary_keys) > 0:
    join_col = primary_keys
else:
    join_col = [x for x in set(cleaned_bronze_df.columns)]
print('join columns --> ' + str(join_col))

# creating join dataframes with fill na
join_bronze_df = cleaned_bronze_df.na.fill('None').na.fill(0)
join_landing_df = cleaned_landing_df.na.fill('None').na.fill(0)

null_bronze_col = [column for column in join_bronze_df.columns if join_bronze_df.filter(col(column).isNull()).count() > 0]
null_landing_col = [column for column in join_landing_df.columns if join_landing_df.filter(col(column).isNull()).count() > 0]

for column in null_bronze_col:
    join_bronze_df = join_bronze_df.withColumn(column, col(column).cast(StringType()))
    join_bronze_df = join_bronze_df.na.fill("1970-01-01 00:00:00", subset=[column])
    if input_format != 'xml':
        join_bronze_df = join_bronze_df.withColumn(column, col(column).cast(TimestampType()))

for column in null_landing_col:
    join_landing_df = join_landing_df.withColumn(column, col(column).cast(StringType()))
    join_landing_df = join_landing_df.na.fill("1970-01-01 00:00:00", subset=[column])
    if input_format != 'xml':
        join_landing_df = join_landing_df.withColumn(column, col(column).cast(TimestampType()))

print('null bronze col --> ' + str(null_bronze_col))
print('null landing col --> ' + str(null_landing_col))

# COMMAND ----------

# MAGIC %md ### QA Test: QA06/QA07/QA08 Check if there are rows from landing not available in bronze

# COMMAND ----------

# missing_bronze_df.merge(join_landing_df, on=['data', 'debito'], how='outer', suffixes=['', '_'], indicator=True)
print(join_bronze_df.count())
# Select distinct rows
distinctDFBronze = join_bronze_df.distinct()
distinctDFBronze.display()

print(join_landing_df.count())
# Select distinct rows
distinctDFLanding = join_landing_df.distinct()
distinctDFLanding.display()

# left_anti_df = (
#     join_bronze_df.join(join_landing_df, on=join_col, how='left_anti')
#     .orderBy('securities'))
# left_anti_df.show(truncate=False)

# COMMAND ----------

print ("Check for missing rows in bronze")
missing_bronze_df = join_landing_df.join(join_bronze_df, on=join_col, how='left_anti')
missing_bronze_count = missing_bronze_df.count()
if missing_bronze_count > 0:
    print(f"VALIDATION ERROR: {missing_bronze_count} missing rows.")
    display(missing_bronze_df.select(*join_col))

# COMMAND ----------

# MAGIC %md ### QA Test: QA06/QA07/QA08 Check if there are rows from unknow source

# COMMAND ----------

print(join_landing_df.distinct().display())
# print(join_landing_df.filter(join_landing_df.last_update_date_eod != "1970-01-01T00:00:00.000+0000").orderBy(join_landing_df.etl_file_name).display())
# join_landing_df.filter(join_landing_df.last_update_date_eod <> '1970-01-01T00:00:00.000+0000').display()
print(join_bronze_df.distinct().display())

# COMMAND ----------

print ("Check for missing rows in landing")
unexpected_landing_df = join_bronze_df.join(join_landing_df, on=join_col, how='left_anti')
unexpected_landing_count = unexpected_landing_df.count() 
if unexpected_landing_count > 0:
    print(f"VALIDATION ERROR: {unexpected_landing_count} unexpected rows found.")
    display(unexpected_landing_df.select(*join_col))

# COMMAND ----------

# MAGIC %md ## Prepare Great Expectations Suite

# COMMAND ----------

custom_expectation_suite = ExpectationSuite(expectation_suite_name=f"landing-to-bronze-{bronze_name}")

# check for the expected rowcount
custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_table_row_count_to_equal",
                                                                 kwargs={'value': landing_count},
                                                                 meta={}))

all_technical_columns = tracking_columns + [Defaults.etl_hash_key_pk]

# check the existence of the columns
landing_columns = [column_name for column_name in cleaned_landing_df.columns]
all_columns = landing_columns + all_technical_columns
### NOT CHECKING IN GE --> AUTOMATION VIA SCHEMA
#custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_table_columns_to_match_set",
#                                                                                    kwargs={'column_set': all_columns}))
                                                                         
# check to be non null (QA01)
if len(primary_keys) == 1:
    mandatory_columns = tracking_columns + primary_keys
else:
    mandatory_columns = tracking_columns

#excluded_column = [Defaults.deleted_flag_column, Defaults.etl_hash_key_pk, 'etl_corrupted_record']
for column in mandatory_columns:
    if (column not in [Defaults.deleted_flag_column]) and (column not in [Defaults.etl_hash_key_pk] and len(primary_keys) == 0) and (column not in ['etl_corrupted_record']):
        custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_not_be_null",
                                                                                            kwargs={'column': column}))

# if a key is defined, the hask key cannot be null. Even if its not a primary key
if len(primary_keys) > 0:
    custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_not_be_null",
                                                                                        kwargs={'column': Defaults.etl_hash_key_pk}))
    custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_unique_value_count_to_be_between",
                                                                                        kwargs={'column': Defaults.etl_hash_key_pk,
                                                                                            'min_value': expected_key_count,
                                                                                            'max_value': expected_key_count}))

# check the number of distinct values expectation
for column in landing_columns:
    if column not in tracking_columns:
        custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_unique_value_count_to_be_between",
                                                                                            kwargs={'column': column,
                                                                                                    'min_value': distinct_value_counts_for_landing[column],
                                                                                                    'max_value': distinct_value_counts_for_landing[column]}))    

### NOT CHECKING IN GE --> AUTOMATION VIA SCHEMA
# check expected datatypes
#for key, value in datatypes.items():
#    custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_be_of_type",
#                                                                        kwargs={'column': key,
#                                                                                'type_': value,
#                                                                                'result_format': 'COMPLETE'}))

# checking invalid or illegal characters
for data in datatypes:
    if datatypes[data] in ["StringType"] and data not in ["_corrupt_record", "_rescued_data"]:
        regex='.|\W\s|^$'
        custom_expectation_suite.add_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_match_regex",
                                                                        kwargs={'column': data,
                                                                                'regex': regex,
                                                                                'result_format': 'COMPLETE'}))
# Run great expectations validations
run_id =  f'{time.time()}'
validation_result = gdf.validate(custom_expectation_suite, run_id=run_id)

# COMMAND ----------

# MAGIC %md ### Add custom expectations

# COMMAND ----------

# build a expectation result to include in the validation results    
all_bronze_rows_in_landing_result = ExpectationValidationResult(success=missing_bronze_count == 0 and unexpected_landing_count == 0,
    result={ "observed_value": {"landing_count": cleaned_landing_df.count(),
                                "bronze_count": cleaned_bronze_df.count(),
                                "missing_count": missing_bronze_count,
                                "unexpected_count": unexpected_landing_count }},
    expectation_config=ExpectationConfiguration(expectation_type="expect_all_rows_match_between_landing_and_bronze", kwargs={}),
    meta={})    
    
custom_validation = add_custom_result_to_validation(all_bronze_rows_in_landing_result, validation_result)

# COMMAND ----------

# MAGIC %md ### Show HTML

# COMMAND ----------

# visualizing the validation result page
validation_result_document_content = ValidationResultsPageRenderer().render(custom_validation)
validation_result_HTML = DefaultJinjaPageView().render(validation_result_document_content)

displayHTML(validation_result_HTML)     

# COMMAND ----------

# MAGIC %md ### Save to Azure blob storage ?

# COMMAND ----------

current_datetime = datetime.now()
current_datetime = current_datetime.strftime("%Y%m%d%H%M%S")

scope = os.environ['KVScope']
storage_account = dbutils.secrets.get(scope, 'ADLS-storage-account')
account_url = f"https://{storage_account}.blob.core.windows.net"

token_credential = ClientSecretCredential(
    dbutils.secrets.get(scope, 'tenantid'),
    dbutils.secrets.get(scope, 'ADLS-magellanadls-app-id'),
    dbutils.secrets.get(scope=scope, key="ADLS-magellanadls-app")
)

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=token_credential)

container_name = "quality-assurance"
output_path = f"landing_to_bronze/{bronze_folder}/{bronze_name}/{bronze_name}_{current_datetime}.html"
container_client = blob_service_client.get_container_client(container_name)
blob_client = blob_service_client.get_blob_client(container_name, output_path)
blob_client.upload_blob(validation_result_HTML)

# COMMAND ----------

padding = 60
ge_results = [f"{x['expectation_config']['expectation_type'].ljust(padding)} {x['success']}" for x in custom_validation['results']]
output='GREAT EXPECTATIONS SUCCEEDED!'.center(padding+5,'#')+'\n' if custom_validation['statistics'].get('unsuccessful_expectations', 0) == 0 else 'GREAT EXPECTATIONS FAILED!'.center(padding+5,'#')+'\n'
output+='\n'.join([x for x in list(set(ge_results))])+'\n'+'#'*(padding+5)+f"\nTest saved into\n{storage_account}/{container_name}/{bronze_folder}/{bronze_name}/{bronze_name}_{current_datetime}.html"
dbutils.notebook.exit(output)
