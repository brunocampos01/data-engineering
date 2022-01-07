#!/bin/bash
# Exclusao dos jobs criados
sqoop job --delete senior-update-colaboradores
sqoop job --delete senior-update-cargos
sqoop job --delete senior-update-desligamentos
sqoop job --delete senior-update-causasdesligamento
sqoop job --delete senior-update-localtrabalho
sqoop job --delete senior-update-centrosdecusto
sqoop job --delete senior-update-motivosdesligamento
sqoop job --delete senior-update-equipetrabalho
sqoop job --delete senior-update-hierarquiaequipe
sqoop job --delete senior-update-situacaocolaborador
sqoop job --delete senior-update-empresas
sqoop job --delete senior-update-escalatrabalho
sqoop job --delete senior-update-instrucaocolaborador
sqoop job --delete senior-update-historicocargos
sqoop job --delete senior-update-historicocentrosdecusto
sqoop job --delete senior-update-historicoequipetrabalho
sqoop job --delete senior-update-infocolaboradores

# Criacao das pastas necessarias para os jobs do sqoop
mkdir /opt/sqoop-1.4.7/gen-lib/cargos/
mkdir /opt/sqoop-1.4.7/gen-src/cargos/
mkdir /opt/sqoop-1.4.7/gen-lib/colaboradores/
mkdir /opt/sqoop-1.4.7/gen-src/colaboradores/
mkdir /opt/sqoop-1.4.7/gen-lib/desligamentos/
mkdir /opt/sqoop-1.4.7/gen-src/desligamentos/
mkdir /opt/sqoop-1.4.7/gen-lib/localtrabalho/
mkdir /opt/sqoop-1.4.7/gen-src/localtrabalho/
mkdir /opt/sqoop-1.4.7/gen-lib/causasdesligamento/
mkdir /opt/sqoop-1.4.7/gen-src/causasdesligamento/
mkdir /opt/sqoop-1.4.7/gen-lib/centrosdecusto/
mkdir /opt/sqoop-1.4.7/gen-src/centrosdecusto/
mkdir /opt/sqoop-1.4.7/gen-lib/motivosdesligamento/
mkdir /opt/sqoop-1.4.7/gen-src/motivosdesligamento/
mkdir /opt/sqoop-1.4.7/gen-lib/equipetrabalho/
mkdir /opt/sqoop-1.4.7/gen-src/equipetrabalho/
mkdir /opt/sqoop-1.4.7/gen-lib/hierarquiaequipe/
mkdir /opt/sqoop-1.4.7/gen-src/hierarquiaequipe/
mkdir /opt/sqoop-1.4.7/gen-lib/situacaocolaborador/
mkdir /opt/sqoop-1.4.7/gen-src/situacaocolaborador/
mkdir /opt/sqoop-1.4.7/gen-lib/empresas/
mkdir /opt/sqoop-1.4.7/gen-src/empresas/
mkdir /opt/sqoop-1.4.7/gen-lib/escalatrabalho/
mkdir /opt/sqoop-1.4.7/gen-src/escalatrabalho/
mkdir /opt/sqoop-1.4.7/gen-lib/instrucaocolaborador/
mkdir /opt/sqoop-1.4.7/gen-src/instrucaocolaborador/
mkdir /opt/sqoop-1.4.7/gen-lib/historicocargos/
mkdir /opt/sqoop-1.4.7/gen-src/historicocargos/
mkdir /opt/sqoop-1.4.7/gen-lib/historicocentrosdecusto/
mkdir /opt/sqoop-1.4.7/gen-src/historicocentrosdecusto/
mkdir /opt/sqoop-1.4.7/gen-lib/historicoequipetrabalho/
mkdir /opt/sqoop-1.4.7/gen-src/historicoequipetrabalho/
mkdir /opt/sqoop-1.4.7/gen-lib/infocolaboradores/
mkdir /opt/sqoop-1.4.7/gen-src/infocolaboradores/

#--- Criacao dos jobs em sqoop

# Ingestao de uma view (necessario usar query e split-by)
sqoop job --create senior-update-colaboradores \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_colaboradores --hive-overwrite \
    --query "SELECT * FROM VETORHPRO.vbiCorpSen WHERE \$CONDITIONS" \
    --split-by NUMCAD \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/colaboradores \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/colaboradores/ \
    --outdir /opt/sqoop-1.4.7/gen-src/colaboradores/

# Ingestao de uma view (necessario usar query e split-by)
sqoop job --create senior-update-desligamentos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_desligamentos --hive-overwrite \
    --query "SELECT * FROM VETORHPRO.vbiCorpSenCm WHERE \$CONDITIONS" \
    --split-by NUMCAD \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/desligamentos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/desligamentos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/desligamentos/

sqoop job --create senior-update-localtrabalho \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_localtrabalho --hive-overwrite \
    --table VETORHPRO.USU_TPOSTRA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/localtrabalho \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/localtrabalho/ \
    --outdir /opt/sqoop-1.4.7/gen-src/localtrabalho/

sqoop job --create senior-update-cargos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_cargos --hive-overwrite \
    --table VETORHPRO.R024CAR --m 1 \
    --columns "ESTCAR, CODCAR, TITRED, TITCAR, CARALF, SISCAR, DATCRI, DATEXT, USU_SUBTIT, USU_NIVCAR" \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '"' \
    --optionally-enclosed-by '"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/cargos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/cargos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/cargos/

sqoop job --create senior-update-causasdesligamento \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_causasdesligamento --hive-overwrite \
    --table VETORHPRO.R042CAU --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/causasdesligamento \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/causasdesligamento/ \
    --outdir /opt/sqoop-1.4.7/gen-src/causasdesligamento/

sqoop job --create senior-update-centrosdecusto \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_centrosdecusto --hive-overwrite \
    --table VETORHPRO.R018CCU --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/centrosdecusto \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/centrosdecusto/ \
    --outdir /opt/sqoop-1.4.7/gen-src/centrosdecusto/

sqoop job --create senior-update-motivosdesligamento \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_motivosdesligamento --hive-overwrite \
    --table VETORHPRO.R114RMD --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/motivosdesligamento \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/motivosdesligamento/ \
    --outdir /opt/sqoop-1.4.7/gen-src/motivosdesligamento/

sqoop job --create senior-update-equipetrabalho \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_equipetrabalho --hive-overwrite \
    --table VETORHPRO.R017POS --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/equipetrabalho \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/equipetrabalho/ \
    --outdir /opt/sqoop-1.4.7/gen-src/equipetrabalho/

sqoop job --create senior-update-hierarquiaequipe \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_hierarquiaequipe --hive-overwrite \
    --table VETORHPRO.R017HIE --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/hierarquiaequipe \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/hierarquiaequipe/ \
    --outdir /opt/sqoop-1.4.7/gen-src/hierarquiaequipe/

sqoop job --create senior-update-situacaocolaborador \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_situacaocolaborador --hive-overwrite \
    --table VETORHPRO.R010SIT --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/situacaocolaborador \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/situacaocolaborador/ \
    --outdir /opt/sqoop-1.4.7/gen-src/situacaocolaborador/

sqoop job --create senior-update-empresas \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_empresas --hive-overwrite \
    --table VETORHPRO.R030EMP --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/empresas \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/empresas/ \
    --outdir /opt/sqoop-1.4.7/gen-src/empresas/

sqoop job --create senior-update-escalatrabalho \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_escalatrabalho --hive-overwrite \
    --table VETORHPRO.R006ESC --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/escalatrabalho \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/escalatrabalho/ \
    --outdir /opt/sqoop-1.4.7/gen-src/escalatrabalho/

sqoop job --create senior-update-instrucaocolaborador \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_instrucaocolaborador --hive-overwrite \
    --table VETORHPRO.R022GRA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/instrucaocolaborador \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/instrucaocolaborador/ \
    --outdir /opt/sqoop-1.4.7/gen-src/instrucaocolaborador/

sqoop job --create senior-update-historicocargos \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_historicocargos --hive-overwrite \
    --table VETORHPRO.R038HCA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/historicocargos \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/historicocargos/ \
    --outdir /opt/sqoop-1.4.7/gen-src/historicocargos/

sqoop job --create senior-update-historicocentrosdecusto \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_historicocentrosdecusto --hive-overwrite \
    --table VETORHPRO.R038HCC --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/historicocentrosdecusto \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/historicocentrosdecusto/ \
    --outdir /opt/sqoop-1.4.7/gen-src/historicocentrosdecusto/

sqoop job --create senior-update-historicoequipetrabalho \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_historicoequipetrabalho --hive-overwrite \
    --table VETORHPRO.R038HPO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/historicoequipetrabalho \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/historicoequipetrabalho/ \
    --outdir /opt/sqoop-1.4.7/gen-src/historicoequipetrabalho/

sqoop job --create senior-update-infocolaboradores \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvsim101.xpto.com.br:1521/SENPRO \
    --username USR_BICORP \
    --password-file hdfs:///user/sqoop/senior.password \
    --hive-import --hive-table raw.senior_infocolaboradores --hive-overwrite \
    --table VETORHPRO.R034CPL --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/senior/infocolaboradores \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/infocolaboradores/ \
    --outdir /opt/sqoop-1.4.7/gen-src/infocolaboradores/
