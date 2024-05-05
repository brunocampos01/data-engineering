# QA Testing Dim Tables: Lift & Shift

## How to Create an Orchestrator
1. Create orchestrator notebook for each domain, e.g. `orchestrator_<domain>_fact`
2. Create sql query file at `tests/qa/liftshift/sql/<domain>/dim/<di_table_name>.sql`
3. Open the notebook that you created and prepare the parameters.

### Example Parameters Orchestrator for a Dim
```python
execution_parameters = {
    "query_path": "sql/dimensions/bi_financial/dim_ledger.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_ledger",
    "table_type": "dim",
    "list_deny_cols": "['MAINT_JOB_CANCEL_DATE']",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])
```

### list_deny_cols
List columns to remove from business columns. It will afect expect_same_content_rows test.

NOTES:
- `query_path`: this file must contains a valid query without semicollon and with breakline in the last row.
- `table_type`: dim


## Data Tests
Look at [README.md](https://adb-6443813330208089.9.azuredatabricks.net/?o=6443813330208089#files/555931528533789) to see the descriptions.

---
