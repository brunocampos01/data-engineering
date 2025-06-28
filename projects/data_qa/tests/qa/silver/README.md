# QA Testing Bronze-Silver

## Step by Step
1. Create orchestrator notebook for each database, e.g. `orchestrator_<schema_name>`
2. Create data schemas at `resources/schemas/<database_name>/<table_name>.json`.
3. Open the notebook that you created and prepare the parameters.

## Mandatory Orchestrator Parameters
- schema_bronze_name
- table_bronze_name
- schema_silver_name
- table_silver_name
- list_pk_cols
- datatypes_definition_file

### Example Parameters Orchestrator for a Table
```json
execution_parameters = {
    "schema_bronze_name": "bloomberg",
    "table_bronze_name": "ld_currency",
    "schema_silver_name": "bloomberg",
    "table_silver_name": "ld_currency",
    "list_pk_cols": "SECURITIES",
    "list_order_by_cols": "LAST_UPDATE_DT, px_last",
    "datatypes_definition_file": "/resources/schemas/bloomberg/ld_currency.json"
}
```

## Great Expectations Tests
| **QA Identifier** | **QA test description**                                                                                                                                                                                          | **Silver ADLS Layer** | **Silver UC** |
|:-----------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------:|:-------------:|
| QA01              | Non-null primary key column, except when it occur for some column(s) of composite primary key (where  then those NULL value(s) column(S) are substituted)                                                        | N / A                 | &#9745;       |
| QA02              | CDC behavior - proper identification of data change capture : proper behavior against determined primary key column(s)                                                                                           | N / A                 | N / A         |
| QA03              | Completeness - Right number of newly added and updated records loaded at subsequent layer/stage, from previous stage                                                                                             | N / A                 | &#9745;       |
| QA04              | Avoid duplication - Uniqueness of records for business fields composing an ingested content ( aka. appart of added technical columns )                                                                           | N / A                 | &#9745;       |
| QA05              | Data schema integrity for none-transforming data process ( business columns, even for added technical columns )                                                                                                  | N / A                 | &#9745;       |
| QA06              |    "Expected data values, types and formats integrity, according to the applied transformation logic (if it applies) in the package - Including Special character being kept when no transformation is involved" | N / A                 | &#9745;       |
| QA07              | No value / Null value integrity except when transformed as part of a transformation package ( expect for the defined primary key column(s))                                                                      | N / A                 | &#9745;       |
| QA08              | Data precision preservation of double/float data type                                                                                                                                                            | N / A                 | &#9745;       |
| QA09              | Trunking/Staging incremental loads properly behaving                                                                                                                                                             | N / A                 | N / A         |

---
