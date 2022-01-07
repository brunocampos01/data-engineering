#!/bin/bash
sqoop job --delete oracle-gl-drilldown
sqoop job --delete oracle-gl-livro
sqoop job --delete oracle-gl-movimento
sqoop job --delete oracle-gl-periodo
sqoop job --delete oracle-gl-pl-contas
sqoop job --delete oracle-gl-saldo

mkdir /opt/sqoop-1.4.7/gen-lib/gl.drilldown/
mkdir /opt/sqoop-1.4.7/gen-src/gl.drilldown/
mkdir /opt/sqoop-1.4.7/gen-lib/gl.livro/
mkdir /opt/sqoop-1.4.7/gen-src/gl.livro/
mkdir /opt/sqoop-1.4.7/gen-lib/gl.movimento/
mkdir /opt/sqoop-1.4.7/gen-src/gl.movimento/
mkdir /opt/sqoop-1.4.7/gen-lib/gl.periodo/
mkdir /opt/sqoop-1.4.7/gen-src/gl.periodo/
mkdir /opt/sqoop-1.4.7/gen-lib/gl.pl_contas/
mkdir /opt/sqoop-1.4.7/gen-src/gl.pl_contas/
mkdir /opt/sqoop-1.4.7/gen-lib/gl.saldo/
mkdir /opt/sqoop-1.4.7/gen-src/gl.saldo/

sqoop job --create oracle-gl-drilldown \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_drilldown --hive-overwrite --hive-drop-import-delims \
    --table BI.XBI_GL_DRILLDOWN --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '\020' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.drilldown \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.drilldown/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.drilldown/

sqoop job --create oracle-gl-livro \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_livro --hive-overwrite \
    --table BI.XBI_GL_LIVRO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.livro \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.livro/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.livro/

sqoop job --create oracle-gl-movimento \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_movimento --hive-overwrite \
    --table BI.XBI_GL_MOVIMENTO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '\020' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.movimento \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.movimento/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.movimento/

sqoop job --create oracle-gl-periodo \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_periodo --hive-overwrite \
    --table BI.XBI_GL_PERIODO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.periodo \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.periodo/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.periodo/

sqoop job --create oracle-gl-pl-contas \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_pl_contas --hive-overwrite \
    --table BI.XBI_GL_PL_CONTAS --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.pl_contas \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.pl_contas/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.pl_contas/

sqoop job --create oracle-gl-saldo \
    -- import \
    --connect jdbc:oracle:thin://@flnsrvunj3020.xpto.com.br:1521/XE \
    --username BI \
    --password-file hdfs:///user/sqoop/oracle_bi.password \
    --hive-import --hive-table raw.oracle_gl_saldo --hive-overwrite \
    --table BI.XBI_GL_SALDO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/oracle/gl.saldo \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/gl.saldo/ \
    --outdir /opt/sqoop-1.4.7/gen-src/gl.saldo/
