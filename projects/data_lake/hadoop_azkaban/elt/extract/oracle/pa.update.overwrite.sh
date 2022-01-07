#!/bin/bash
sqoop job --delete oracle-pa-project

mkdir /opt/sqoop-1.4.7/gen-lib/pa.project/
mkdir /opt/sqoop-1.4.7/gen-src/pa.project/

sqoop job --create oracle-pa-project \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_pa_project --hive-overwrite --hive-drop-import-delims \
    --table PA.PA_PROJECTS_ALL --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '\020' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/pa.poject \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/pa.poject/ \
    --outdir /opt/sqoop-1.4.7/gen-src/pa.poject/
