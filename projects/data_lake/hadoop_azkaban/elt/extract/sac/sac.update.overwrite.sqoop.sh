#!/bin/bash
sqoop job --delete sac-update-propostahoraextra
sqoop job --delete sac-update-itemhoraextra
sqoop job --delete sac-update-estadoaprovacao
sqoop job --delete sac-update-tipohoraextra
sqoop job --delete sac-update-usuario
sqoop job --delete sac-update-colaborador
sqoop job --delete sac-update-projeto
sqoop job --delete sac-update-sistema
sqoop job --delete sac-update-tipoatividade
sqoop job --delete sac-update-motivo

mkdir /opt/sqoop-1.4.7/gen-lib/ecbh.propostahoraextra
mkdir /opt/sqoop-1.4.7/gen-src/ecbh.propostahoraextra

mkdir /opt/sqoop-1.4.7/gen-lib/ecbh.itemhoraextra
mkdir /opt/sqoop-1.4.7/gen-src/ecbh.itemhoraextra

mkdir /opt/sqoop-1.4.7/gen-lib/ecbh.estadoaprovacao
mkdir /opt/sqoop-1.4.7/gen-src/ecbh.estadoaprovacao

mkdir /opt/sqoop-1.4.7/gen-lib/ecbh.tipohoraextra
mkdir /opt/sqoop-1.4.7/gen-src/ecbh.tipohoraextra

mkdir /opt/sqoop-1.4.7/gen-lib/eseg.usuario
mkdir /opt/sqoop-1.4.7/gen-src/eseg.usuario

mkdir /opt/sqoop-1.4.7/gen-lib/esac.colaborador
mkdir /opt/sqoop-1.4.7/gen-src/esac.colaborador

mkdir /opt/sqoop-1.4.7/gen-lib/esac.projeto
mkdir /opt/sqoop-1.4.7/gen-src/esac.projeto

mkdir /opt/sqoop-1.4.7/gen-lib/esac.sistema
mkdir /opt/sqoop-1.4.7/gen-src/esac.sistema

mkdir /opt/sqoop-1.4.7/gen-lib/esac.tipoatividade
mkdir /opt/sqoop-1.4.7/gen-src/esac.tipoatividade

mkdir /opt/sqoop-1.4.7/gen-lib/esac.motivo
mkdir /opt/sqoop-1.4.7/gen-src/esac.motivo

#Tabelas relacionamento: ECBHESTADOAPROV v, ECBHTIPOHORAEXTRA v, ESACCOLABORADOR v
sqoop job --create sac-update-propostahoraextra \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_propostahoraextra --hive-overwrite \
    --table sac.ECBHPROPHORAEXTRA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/ecbh.propostahoraextra \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ecbh.propostahoraextra \
    --outdir /opt/sqoop-1.4.7/gen-src/ecbh.propostahoraextra

#Tabelas relacionamento: ECBHPROPHORAEXTRA v, ECBHMOTIVO I
sqoop job --create sac-update-itemhoraextra \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_itemhoraextra --hive-overwrite \
    --query 'select h.CDPROPOSTA, h.CDPROJETO, h.CDUSUARIO, h.NUITEM, h.DTREALIZACAO, h.HRPREVINICIO, h.HRPREVFINAL, h.QTHORASPREV, h.HRREALINICIO, h.HRREALFINAL, h.QTHORASREAL, h.QTSALDOPONTO, h.QTSALDOBANCO, h.QTSALDOVIAGEM, h.DTCALCSALDO, h.DTULTPONTOFECHADO, h.QTSALDOMES, h.QTSALDOANO, h.QTHORAAPOIO, h.QTHORAPAGAS, h.QTHORAAPAGAR, h.FLCANCELAMENTO, h.CDMOTIVO from sac.ECBHITEMHORAEXTRA h where $CONDITIONS' \
    --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/ecbh.itemhoraextra \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ecbh.itemhoraextra \
    --outdir /opt/sqoop-1.4.7/gen-src/ecbh.itemhoraextra

#Não possui nenhum relacionamento com outras tabelas
sqoop job --create sac-update-estadoaprovacao \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_estadoaprovacao --hive-overwrite \
    --table sac.ECBHESTADOAPROV --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/ecbh.estadoaprovacao \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ecbh.estadoaprovacao \
    --outdir /opt/sqoop-1.4.7/gen-src/ecbh.estadoaprovacao

#Não possui nenhum relacionamento com outras tabelas
sqoop job --create sac-update-tipohoraextra \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_tipohoraextra --hive-overwrite \
    --table sac.ECBHTIPOHORAEXTRA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/ecbh.tipohoraextra \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ecbh.tipohoraextra \
    --outdir /opt/sqoop-1.4.7/gen-src/ecbh.tipohoraextra

#Não possui nenhum relacionamento com outras tabelas
sqoop job --create sac-update-usuario \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_usuario --hive-overwrite \
    --table sac.ESEGUSUARIO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/eseg.usuario \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/eseg.usuario \
    --outdir /opt/sqoop-1.4.7/gen-src/eseg.usuario

#Tabelas relacionamento: ESACSISTEMA Iv, ESACFUNCAO, ESACPROJETO Iv, ESACTIPOATIVIDADE I, ESACUSUARIO, ESEGUSUARIO v
sqoop job --create sac-update-colaborador \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_colaborador --hive-overwrite \
    --table sac.ESACCOLABORADOR --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/esac.colaborador \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/esac.colaborador \
    --outdir /opt/sqoop-1.4.7/gen-src/esac.colaborador

sqoop job --create sac-update-projeto \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_projeto --hive-overwrite \
    --table sac.ESACPROJETO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/esac.projeto \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/esac.projeto \
    --outdir /opt/sqoop-1.4.7/gen-src/esac.projeto

sqoop job --create sac-update-sistema \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_sistema --hive-overwrite \
    --table sac.ESACSISTEMA --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/esac.sistema \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/esac.sistema \
    --outdir /opt/sqoop-1.4.7/gen-src/esac.sistema

sqoop job --create sac-update-tipoatividade \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_tipoatividade --hive-overwrite \
    --table sac.ESACTIPOATIVIDADE --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/esac.tipoatividade \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/esac.tipoatividade \
    --outdir /opt/sqoop-1.4.7/gen-src/esac.tipoatividade

sqoop job --create sac-update-motivo \
    -- import \
    --connect jdbc:db2://sacbanco.xpto.com.br:50100/sac \
    --username sac \
    --password-file hdfs:///user/sqoop/sac.password \
    --hive-import --hive-table raw.sac_motivo --hive-overwrite \
    --table sac.ECBHMOTIVO --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sac/esac.motivo \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/esac.motivo \
    --outdir /opt/sqoop-1.4.7/gen-src/esac.motivo
