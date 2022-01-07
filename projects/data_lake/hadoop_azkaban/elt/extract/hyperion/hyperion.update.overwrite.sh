#!/bin/bash
sqoop job --delete hyperion-update-financas
sqoop job --delete hyperion-update-rh
sqoop job --delete hyperion-update-metadados

mkdir /opt/sqoop-1.4.7/gen-lib/smartview.financas/
mkdir /opt/sqoop-1.4.7/gen-src/smartview.financas/
mkdir /opt/sqoop-1.4.7/gen-lib/smartview.rh/
mkdir /opt/sqoop-1.4.7/gen-src/smartview.rh/
mkdir /opt/sqoop-1.4.7/gen-lib/smartview.metadados/
mkdir /opt/sqoop-1.4.7/gen-src/smartview.metadados/

sqoop job --create hyperion-update-financas \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username xpto \
    --password-file hdfs:///user/sqoop/hyperion.password \
    --hive-import --hive-table raw.hyperion_financas --hive-overwrite \
    --table xpto.HP_EXPORT_FIN_VALUE --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/hyperion/smartview.financas \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/smartview.financas/ \
    --outdir /opt/sqoop-1.4.7/gen-src/smartview.financas/

sqoop job --create hyperion-update-rh \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username xpto \
    --password-file hdfs:///user/sqoop/hyperion.password \
    --hive-import --hive-table raw.hyperion_rh --hive-overwrite \
    --table xpto.HP_EXPORT_RH_VALUE --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/hyperion/smartview.rh \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/smartview.rh/ \
    --outdir /opt/sqoop-1.4.7/gen-src/smartview.rh/

sqoop job --create hyperion-update-metadados \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username xpto \
    --password-file hdfs:///user/sqoop/hyperion.password \
    --hive-import --hive-table raw.hyperion_metadados --hive-overwrite \
    --table xpto.VHYPMETADADOS --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/hyperion/smartview.metadados \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/smartview.metadados/ \
    --outdir /opt/sqoop-1.4.7/gen-src/smartview.metadados/
