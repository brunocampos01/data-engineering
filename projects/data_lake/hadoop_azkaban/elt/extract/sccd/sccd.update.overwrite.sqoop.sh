#!/bin/bash
sqoop job -delete sccd-ungp-update-customers
sqoop job -delete sccd-ungp-update-srs
sqoop job -delete sccd-ungp-update-cycles
sqoop job -delete sccd-ungp-update-history
sqoop job -delete sccd-ungp-update-related-record
sqoop job -delete sccd-ungp-update-sla-records
sqoop job -delete sccd-ungp-update-calendar
sqoop job -delete sccd-ungp-update-workperiod
sqoop job -delete sccd-ungp-update-worklog
sqoop job -delete sccd-ungp-update-ticket
sqoop job -delete sccd-ungp-update-ci
sqoop job -delete sccd-ungp-update-synonymdomain
sqoop job -delete sccd-ungp-update-lsynonymdomain
sqoop job -delete sccd-ungp-update-contracts
sqoop job -delete sccd-ungp-update-longdescription

mkdir /opt/sqoop-1.4.7/gen-lib/maximo.customers
mkdir /opt/sqoop-1.4.7/gen-src/maximo.customers
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.calendar
mkdir /opt/sqoop-1.4.7/gen-src/maximo.calendar
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.workperiod
mkdir /opt/sqoop-1.4.7/gen-src/maximo.workperiod
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.relatedrecord
mkdir /opt/sqoop-1.4.7/gen-src/maximo.relatedrecord
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.worklog
mkdir /opt/sqoop-1.4.7/gen-src/maximo.worklog
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.synonymdomain
mkdir /opt/sqoop-1.4.7/gen-src/maximo.synonymdomain
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.lsynonymdomain
mkdir /opt/sqoop-1.4.7/gen-src/maximo.lsynonymdomain
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.ci
mkdir /opt/sqoop-1.4.7/gen-src/maximo.ci
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.ticket
mkdir /opt/sqoop-1.4.7/gen-src/maximo.ticket
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.slarecords
mkdir /opt/sqoop-1.4.7/gen-src/maximo.slarecords
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.tkstatus
mkdir /opt/sqoop-1.4.7/gen-src/maximo.tkstatus
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.sr
mkdir /opt/sqoop-1.4.7/gen-src/maximo.sr
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.contracts
mkdir /opt/sqoop-1.4.7/gen-src/maximo.contracts
mkdir /opt/sqoop-1.4.7/gen-lib/maximo.longdescription
mkdir /opt/sqoop-1.4.7/gen-src/maximo.longdescription

sqoop job --create sccd-ungp-update-customers \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_customers --hive-overwrite \
    --query "select * from maximo.pluspcustomer where sof_siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.customers \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.customers \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.customers

sqoop job --create sccd-ungp-update-calendar \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_calendar --hive-overwrite \
    --query "SELECT c.* from maximo.calendar c where c.sof_siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 10 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.calendar \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.calendar \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.calendar

sqoop job --create sccd-ungp-update-workperiod \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_workperiod --hive-overwrite \
    --table maximo.workperiod --split-by 'ROWSTAMP' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.workperiod \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.workperiod \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.workperiod

sqoop job --create sccd-ungp-update-srs \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_srs --hive-overwrite \
    --query "SELECT * from maximo.sr where siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --fields-terminated-by '\001' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.sr \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.sr \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.sr

sqoop job --create sccd-ungp-update-cycles \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_cycles --hive-overwrite \
    --query "SELECT * from maximo.incident where siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.incident \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.incident \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.incident

sqoop job --create sccd-ungp-update-history \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_ticket_history --hive-overwrite \
    --query "SELECT tk.* from maximo.tkstatus tk where tk.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.tkstatus \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.tkstatus \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.tkstatus

sqoop job --create sccd-ungp-update-related-record \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_related_record --hive-overwrite \
    --query "SELECT rr.* from maximo.relatedrecord rr where rr.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.relatedrecord \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.relatedrecord \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.relatedrecord

sqoop job --create sccd-ungp-update-sla-records \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_sla_records --hive-overwrite \
    --table maximo.slarecords \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.slarecords \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.slarecords \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.slarecords

sqoop job --create sccd-ungp-update-worklog \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_worklog --hive-overwrite \
    --query "SELECT wl.* from maximo.worklog wl where wl.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'WORKLOGID' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.worklog \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.worklog \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.worklog

sqoop job --create sccd-ungp-update-ticket \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_ticket --hive-overwrite \
    --query "select * from maximo.ticket where siteid = 'UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.ticket \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.ticket \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.ticket

sqoop job --create sccd-ungp-update-ci \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_ci --hive-overwrite \
    --query "select * from maximo.ci where sof_siteid = 'UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.ci \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.ci \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.ci

sqoop job --create sccd-ungp-update-classstructure \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_classstructure --hive-overwrite \
    --query "select * from maximo.classstructure where sof_siteid = 'UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.classstructure \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.classstructure \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.classstructure

sqoop job --create sccd-ungp-update-classification \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_classification --hive-overwrite \
    --table maximo.CLASSIFICATION \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.classification \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.classification \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.classification

sqoop job --create sccd-ungp-update-cirelation \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_cirelation --hive-overwrite \
    --table maximo.cirelation \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.cirelation \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.cirelation \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.cirelation

sqoop job --create sccd-ungp-update-contract-collection \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_contract_collection --hive-overwrite \
    --table maximo.sof_contrato_colection --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.contractcollection \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.contractcollection \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.contractcollection

sqoop job --create sccd-ungp-update-synonymdomain \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_synonymdomain --hive-overwrite \
    --table maximo.synonymdomain \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.synonymdomain \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.synonymdomain \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.synonymdomain

sqoop job --create sccd-ungp-update-lsynonymdomain \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_lsynonymdomain --hive-overwrite \
    --table maximo.l_synonymdomain \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.lsynonymdomain \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.lsynonymdomain \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.lsynonymdomain

sqoop job --create sccd-ungp-update-contracts \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_contracts --hive-overwrite \
    --table maximo.contract \
    --split-by 'ROWSTAMP' --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.contracts \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.contracts \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.contracts

sqoop job --create sccd-ungp-update-longdescription \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --hive-import --hive-table raw.sccd_ungp_longdescription --hive-overwrite \
    --table MAXIMO.LONGDESCRIPTION \
    --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.longdescription \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/maximo.longdescription \
    --outdir /opt/sqoop-1.4.7/gen-src/maximo.longdescription
