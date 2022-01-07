#!/bin/bash
sqoop job --create sccd-ungp-create-customers \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "select * from maximo.pluspcustomer where sof_siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 5 \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_customers \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.customers \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-srs \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT * from maximo.sr where siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_srs \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.sr \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-cycles \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT * from maximo.incident where siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_cycles \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.incident \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-history \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT tk.* from maximo.tkstatus tk where tk.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_ticket_history \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.tkstatus \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-related-record \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT rr.* from maximo.relatedrecord rr where rr.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_related_record \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.relatedrecord \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-sla-records \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --table maximo.slarecords \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_sla_records \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.slarecords \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-calendar \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT c.* from maximo.calendar c where c.sof_siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 10 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_calendar \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.calendar \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-workperiod \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --table maximo.workperiod --split-by 'ROWSTAMP' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_workperiod \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.workperiod \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-worklog \
    -- import \
    --connect jdbc:db2://flnssrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "SELECT wl.* from maximo.worklog wl where wl.siteid='UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_worklog \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.worklog \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create sccd-ungp-create-ticket \
    -- import \
    --connect jdbc:db2://flnsrvungp0063.xpto.com.br:50005/maxdb75 \
    --username mssoft \
    --password-file hdfs:///user/sqoop/sccd-ungp.password \
    --query "select * from maximo.ticket where siteid = 'UNGP' and \$CONDITIONS" \
    --split-by 'ROWSTAMP' --m 30 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --hive-import --create-hive-table --hive-table raw.sccd_ungp_ticket \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/sccd/ungp/maximo.ticket \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib \
    --outdir /opt/sqoop/gen-src
