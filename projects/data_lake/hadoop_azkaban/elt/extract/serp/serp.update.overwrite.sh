#!/bin/bash
mkdir /opt/sqoop-1.4.7/gen-lib/ehrs.quadrohorario
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.quadrohorario
mkdir /opt/sqoop-1.4.7/gen-lib/ehrs.quadrohorariofluxo
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.quadrohorariofluxo
mkdir /opt/sqoop-1.4.7/gen-lib/ehrs.situacaofluxo
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.situacaofluxo
mkdir /opt/sqoop-1.4.7/gen-lib/ehrs.vinculortc
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.vinculortc
mkdir /opt/sqoop-1.4.7/gen-lib/ehrs.vinculosccd
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.vinculosccd
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.colaborador
mkdir /opt/sqoop-1.4.7/gen-src/ehrs.colaborador

sqoop job --delete serp-update-vinculortc
sqoop job --delete serp-update-vinculosccd
sqoop job --delete serp-update-quadrohorario
sqoop job --delete serp-update-quadrohorarioitem
sqoop job --delete serp-update-quadrohorariofluxo
sqoop job --delete serp-update-situacaofluxo
sqoop job --delete serp-update-colaboradores

sqoop job --create serp-update-quadrohorario \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_quadrohorario --hive-overwrite \
    --table ehrsquadrohorario \
    --split-by idquadrohorario --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/quadrohorario \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.quadrohorario/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.quadrohorario/

sqoop job --create serp-update-quadrohorariofluxo \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_quadrohorariofluxo --hive-overwrite \
    --table ehrsquadrohorariofluxo \
    --split-by idfluxo --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/quadrohorariofluxo \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.quadrohorariofluxo/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.quadrohorariofluxo/

sqoop job --create serp-update-quadrohorarioitem \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_quadrohorarioitem --hive-overwrite \
    --table ehrsquadrohorarioitem \
    --split-by idquadrohorarioitem --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/quadrohorarioitem \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.quadrohorarioitem/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.quadrohorarioitem/

sqoop job --create serp-update-colaboradores \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_colaboradores --hive-overwrite \
    --table ehrscolaborador \
    --split-by idcolaborador --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/colaboradores \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.colaborador/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.colaborador/

sqoop job --create serp-update-situacaofluxo \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_situacaofluxo --hive-overwrite \
    --table ehrssituacaofluxo --m 1 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/situacaofluxo \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.situacaofluxo/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.situacaofluxo/

sqoop job --create serp-update-vinculortc \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_vinculortc --hive-overwrite \
    --table ehrsvinculortc \
    --split-by idvinculortc --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/vinculortc \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.vinculortc/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.vinculortc/

sqoop job --create serp-update-vinculosccd \
    -- import \
    --connect jdbc:postgresql://flnsrvcorp3194.xpto.com.br:5432/quadro_de_horario \
    --username datadriven \
    --password-file hdfs:///user/sqoop/serp.password \
    --hive-import --hive-table raw.serp_vinculosccd --hive-overwrite \
    --table ehrsvinculosccd \
    --split-by idvinculosccd --m 10 \
    --compress \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/serp/vinculosccd \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ehrs.vinculosccd/ \
    --outdir /opt/sqoop-1.4.7/gen-src/ehrs.vinculosccd/
