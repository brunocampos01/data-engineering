# Data Catalog Project
[![Python 3](https://img.shields.io/badge/Python-3-blue.svg)](https://www.python.org/downloads/release/python-381/)

## Project's Scope
The scope of this project consists of building processes to populate the metadata mapped in the data documentation project into the data lake (descriptions, tags, properties and characteristics of the fields). 
Based on the metadata ingested to the data lake, generate visual representations of the data lake that can be explored by management, business and IT teams though Power BI dashboards. [1]

## Project's Goal
Populate all data gathered in the data documentation project to the Unity Catalog. Generate a Dashboard to give visibility of the data to ACME company. [1]


## Requirements
| Requisite           | Version                                            |
|---------------------|--------------------------------------------------- |
| Python              | 3.10.12                                            |
| Pip                 | 22.2.2                                             |
| Databricks runtime  | 13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12) |

- A user with permissions to read and write the magellanadlsdev containers (Azure Storage Account)

<!-- ## Install
```sh
pip install --require-hashes -r requirements.txt
``` 
## How to Run
TODO

## Features
TODO
-->


## Dimensions and Facts Tables
Star schema for fact_sources:
![Ilustration](docs/images/fact_sources_diagram.png)

Star schema for fact_tables:
![Ilustration](docs/images/fact_tables_diagram.png)

Star schema for fact_fields:
![Ilustration](docs/images/fact_fields_diagram.png)


<!-- ## Workflow
TODO

### Job: `update_data_catalog`
In this notebook TODO

### Job: `generate_bronze_catalog`
In this notebook TODO

### Job: `generate_silver_catalog`
In this notebook TODO

### Job: `generate_gold_catalog`
In this notebook it is generated the dimensions and fact tables. -->

### References:
- [1] [Kickoff Presentation - Unit Catalog and Dashboards](https://docs.google.com/presentation/d/1Uldv4dU0pn60xcX-XoTyXj9g9-BQTBALF9D_SpU_RFY/edit?usp=sharing)

---
