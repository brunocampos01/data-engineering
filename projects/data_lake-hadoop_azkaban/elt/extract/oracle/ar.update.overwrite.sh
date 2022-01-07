#!/bin/bash
sqoop job --delete oracle-ar-update-capa
sqoop job --delete oracle-ar-update-lancamentos
sqoop job --delete oracle-ar-update-recebimentos

mkdir /opt/sqoop-1.4.7/gen-lib/ar.capa/
mkdir /opt/sqoop-1.4.7/gen-lib/ar.lancamentos/
mkdir /opt/sqoop-1.4.7/gen-lib/ar.recebimentos/
mkdir /opt/sqoop-1.4.7/gen-src/ar.capa/
mkdir /opt/sqoop-1.4.7/gen-src/ar.lancamentos/
mkdir /opt/sqoop-1.4.7/gen-src/ar.recebimentos/

sqoop job --create oracle-ar-update-capa \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim004.xpto.com.br:1521/XE \
    --username xxslave \
    --password-file hdfs:///user/sqoop/oracle_xxslave-corp.password \
    --hive-import --hive-table raw.oracle_ar_capa --hive-overwrite \
    --table XXSLAVE.XSOF_AR_CAPA --split-by 'CUSTOMER_TRX_ID' --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/ar.capa \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ar.capa/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ar.capa/

sqoop job --create oracle-ar-update-lancamentos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim004.xpto.com.br:1521/XE \
    --username xxslave \
    --password-file hdfs:///user/sqoop/oracle_xxslave-corp.password \
    --hive-import --hive-table raw.oracle_ar_lancamentos --hive-overwrite \
    --table XXSLAVE.XSOF_AR_LINHA --split-by 'CUSTOMER_TRX_ID' --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/ar.lancamentos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ar.lancamentos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ar.lancamentos/

sqoop job --create oracle-ar-update-recebimentos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim004.xpto.com.br:1521/XE \
    --username xxslave \
    --password-file hdfs:///user/sqoop/oracle_xxslave-corp.password \
    --hive-import --hive-table raw.oracle_ar_recebimentos --hive-overwrite \
    --table XXSLAVE.XSOF_AR_RECEBIM --split-by 'CUSTOMER_TRX_ID' --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/ar.recebimentos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ar.recebimentos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ar.recebimentos/
