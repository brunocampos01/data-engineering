# Databricks notebook source
# MAGIC %md ## This notebook is responsible for updating the Unity Catalog

# COMMAND ----------

# MAGIC %run "./setup"

# COMMAND ----------

# declare parameters
dbutils.widgets.text("target", "*")
dbutils.widgets.text("max_parallelism", "48")

# COMMAND ----------

# define variables
import os

release_target = dbutils.widgets.get("target")
current_notebook_path = get_current_notebook_path()
current_notebook_folder = os.path.dirname(current_notebook_path)
current_notebook_name = os.path.basename(current_notebook_path)
max_parallelism = int(dbutils.widgets.get("max_parallelism"))

print(f"Current notebook folder {current_notebook_folder}")
print(f"Release target {release_target}")

# COMMAND ----------

# Finding all notebooks

import glob
from concurrent.futures import ThreadPoolExecutor


class Notebook:

    def __init__(self, path: str):
        self.path = path
        self.timeout = 6000
        self.retry = 1
        self.notebook_layer = self.find_layer_from_path(path)
        self.data_catalog = self.catalog_name_for_layer(self.notebook_layer)
        self.data_owner = self.find_data_owner()
        self.schema_name = find_schema_name(path)
        self.relative_path = self.relative_path_for_notebook()
        self.target_object_type = self.find_target_object_type()
        self.parameters = {
            "data_catalog": self.data_catalog,
            "data_owner": self.data_owner,
            "database_name": self.schema_name,
            "storage_account": find_storage_account()
        }

    def relative_path_for_notebook(self):
        """"
        Remove the folder from the path
        Returns:
            str
        """
        str_path_len = 1 + len(os.getcwd())
        path = self.path[str_path_len:]
        return path

    def find_target_object_type(self) -> str:
        """
        Determines which type of object is created from the script
        Returns:
            str: (TABLE, FUNCTION, VIEW)
        """
        if self.relative_path.lower().endswith('view'):
            return 'VIEW'
        elif '_schema_functions_/' in self.relative_path.lower():
            return 'FUNCTION'
        return 'TABLE'

    @classmethod
    def submit_notebook(cls, notebook):
        """
        Triggers the notebook execution
        """
        #print("Running notebook %s" % notebook.relative_path)
        try:
            if notebook.parameters:
                create_result = dbutils.notebook.run(notebook.relative_path, notebook.timeout, notebook.parameters)
            else:
                create_result = dbutils.notebook.run(notebook.relative_path, notebook.timeout)

            # add custom properties for the created table
            if notebook.target_object_type == 'TABLE':
                notebook.write_custom_table_properties()

            return create_result
        except Exception:
            if notebook.retry < 1:
                print(f"{notebook.relative_path} FAILED.")
                raise
        #print("Retrying notebook %s" % notebook.relative_path)
        notebook.retry = notebook.retry - 1
        cls.submit_notebook(notebook)

    def write_custom_table_properties(self):
        # add custom properties for the created table
        set_workflow_name_property_for_sql_file(self.relative_path, self.data_catalog)

    @classmethod
    def find_layer_from_path(cls, notebook_path):
        """
        Returns:
            str
        """
        layer_name = os.path.basename(os.path.dirname(notebook_path))
        if layer_name not in ['bronze', 'silver', 'silver_entities', 'silver_interfaces', 'gold', 'sensitive', 'monitoring', 'data_catalog']:
            raise Exception(f"Invalid layer name {layer_name} for notebook {notebook_path}")
        return layer_name

    @classmethod
    def catalog_name_for_layer(cls, layer_name):
        """
        Returns:
            str
        """
        environment = os.getenv("Environment").lower()
        if environment == 'prod':
            return layer_name
        return f"{environment}_{layer_name}"

    @classmethod
    def find_data_owner(cls):
        """
        Returns:
            str
        """
        environment = os.getenv("Environment").lower()
        if environment == 'dev':
            return "dev_uc_catalogs_owners"
        elif environment == 'test':
            return "test_uc_catalogs_owners"
        return "prod_uc_catalogs_owners"


def find_notebooks_in_folder(folder: str):
    print(f"Searching notebooks in {folder}")
    ignored_files = ['setup', current_notebook_name]
    return [path
            for path in glob.iglob(f'{folder}/**', recursive=True)
            if os.path.isfile(path) and not path.startswith("__")
            and os.path.basename(path) not in ignored_files]


def find_notebooks_for_deployment():
    unity_catalog_folder = os.getcwd()
    root_dir = unity_catalog_folder
    if bool(release_target) and release_target != '*':
        job_names = release_target.split(',')
        discovered_notebooks = []
        for job_name in job_names:
            for job_folder in glob.iglob(f'{root_dir}/*/{job_name}', recursive=True):
                job_related_scripts = find_notebooks_in_folder(job_folder)
                discovered_notebooks = discovered_notebooks + job_related_scripts
    else:
        discovered_notebooks = find_notebooks_in_folder(root_dir)

    return sorted(set(discovered_notebooks))


# Array of instances of Notebook Class
all_notebooks = find_notebooks_for_deployment()
notebooks_data = [Notebook(path) for path in all_notebooks]

print(f"{len(notebooks_data)} notebooks found.")


# COMMAND ----------

def parallel_notebooks(notebooks, num_in_parallel):
    # If you create too many notebooks in parallel the driver may crash when you submit all the jobs at once.
    # This code limits the number of parallel notebooks.
    with ThreadPoolExecutor(max_workers=num_in_parallel) as ec:
        return [ec.submit(Notebook.submit_notebook, notebook) for notebook in notebooks]

# default timeout for parallel notebooks
default_timeout = 6000

# COMMAND ----------

# create all schemas before creating the tables
all_schemas = set([(n.data_catalog, n.schema_name, n.data_owner) for n in notebooks_data])
print("===========================")
print(f"Creating databases ")
print("===========================")
for iter_catalog, iter_schema, iter_data_owner in all_schemas:
    full_database_name = f"{iter_catalog}.{iter_schema}"
    create_stmt = f"CREATE DATABASE IF NOT EXISTS {full_database_name}"
    print(create_stmt)
    spark.sql(create_stmt)
    spark.sql(f"ALTER DATABASE {full_database_name} OWNER TO {iter_data_owner}")

# COMMAND ----------

# create all schema functions
all_function_notebooks = [nbd for nbd in notebooks_data if nbd.target_object_type == "FUNCTION"]
print("===========================")
print(f"Running FUNCTION notebooks")
print("===========================")
res = parallel_notebooks(all_function_notebooks, max_parallelism)
function_results = [i.result(timeout=default_timeout) for i in res]  # This is a blocking call.

# COMMAND ----------

# create all sensitive tables
layer_notebooks = [nbd for nbd in notebooks_data
                   if nbd.notebook_layer == "sensitive"
                   and nbd.target_object_type == "TABLE"]
if len(layer_notebooks) > 0:
    print("==================================")
    print(f"Running sensitive TABLE notebooks")
    print("==================================")
    res = parallel_notebooks(layer_notebooks, max_parallelism)
    table_results = [i.result(timeout=default_timeout) for i in res]  # This is a blocking call.

# COMMAND ----------

# create all sensitive views
layer_notebooks = [nbd for nbd in notebooks_data
                   if nbd.notebook_layer == "sensitive"
                   and nbd.target_object_type == "VIEW"]
if len(layer_notebooks) > 0:
    print("==================================")
    print(f"Running sensitive VIEW notebooks")
    print("==================================")
    res = parallel_notebooks(layer_notebooks, max_parallelism)
    view_results = [i.result(timeout=default_timeout) for i in res]  # This is a blocking call.

# COMMAND ----------

# create all medallion tables

for layer in ['bronze', 'silver', 'silver_entities', 'silver_interfaces', 'gold', 'data_catalog']:
    layer_notebooks = [nbd for nbd in notebooks_data
                       if nbd.notebook_layer == layer
                       and nbd.target_object_type == "TABLE"]
    if len(layer_notebooks) > 0:
        print("================================")
        print(f"Running {layer} TABLE notebooks")
        print("================================")
        res = parallel_notebooks(layer_notebooks, max_parallelism)
        table_results = [i.result(timeout=default_timeout) for i in res]  # This is a blocking call.

# COMMAND ----------

# create all medallion views
for layer in ['bronze', 'silver', 'silver_entities', 'silver_interfaces', 'gold']:
    layer_notebooks = [nbd for nbd in notebooks_data
                       if nbd.notebook_layer == layer
                       and nbd.target_object_type == "VIEW"]
    if len(layer_notebooks) > 0:
        print("===============================")
        print(f"Running {layer} VIEW notebooks")
        print("===============================")
        res = parallel_notebooks(layer_notebooks, max_parallelism)
        view_results = [i.result(timeout=default_timeout) for i in res]  # This is a blocking call.
