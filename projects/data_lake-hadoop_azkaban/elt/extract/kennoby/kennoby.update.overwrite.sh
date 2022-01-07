#!/bin/bash
# Exclusao dos jobs criados
sqoop job --delete senior-update-tcolaboradores

# Criacao das pastas necessarias para os jobs do sqoop
mkdir /opt/sqoop-1.4.7/gen-lib/cargos/
mkdir /opt/sqoop-1.4.7/gen-src/cargos/

# Criacao dos jobs em sqoop
sqoop job --create senior-update-tcargos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim018.xpto.com.br:1521/seniord.xpto.com.br \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_tcargos --hive-overwrite \
    --table VETORHPRO.R024CAR --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/cargos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/cargos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/cargos/
