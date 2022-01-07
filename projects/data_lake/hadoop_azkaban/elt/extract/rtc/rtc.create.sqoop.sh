#!/bin/bash

sqoop job --create rtc-create-releases \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_release --split-by 'PROJECT_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_releases \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.releases \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-links \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_request_relational_link --split-by 'REQUEST_RELATION_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_links \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.links \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-types \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_request_type --split-by 'PROJECT_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitem_types \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.types \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-iterations \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_iteration --split-by 'PROJECT_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_iterations \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.iterations \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-projects \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_project --split-by 'PROJECT_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_projects \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.projects \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-workitems \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_request --split-by 'REQUEST_ID' --m 50 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.requests \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-int \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_int_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_int \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.int \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-long \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_long_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_long \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.long \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-decimal \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_decimal_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_decimal \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.decimal \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-enumeration \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_enumeration --split-by 'REQUEST_ENUMERATION_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_enumeration \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.enumeration \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-timestamp \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_timestamp_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_timestamp \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.timestamp \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-boolean \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_bool_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_boolean \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.boolean \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-string \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_string_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_string \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-string_medium \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_string_m_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_string_medium \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string_medium \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-string_large \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ricalm.vw_rqst_string_l_ext --split-by 'REQUEST_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_workitems_attributes_string_large \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string_large \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-timelines \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_timeline --split-by 'TIMELINE_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_timelines \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timelines \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-timecodes \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_timecode --m 1 \
    --hive-import --create-hive-table --hive-table raw.rtc_timecodes \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timecodes \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-timesheets \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_timesheet --split-by 'TIMESHEET_ID' --m 25 \
    --hive-import --create-hive-table --hive-table raw.rtc_timesheets \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timesheets \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-activities \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_activity --split-by 'ACTIVITY_ID' --m 50 \
    --hive-import --create-hive-table --hive-table raw.rtc_activities \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.activities \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/

sqoop job --create rtc-create-resources \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --table ridw.vw_resource --split-by 'RESOURCE_ID' --m 10 \
    --hive-import --create-hive-table --hive-table raw.rtc_resources \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.resources \
    --delete-target-dir \
    --bindir /opt/sqoop/gen-lib/ \
    --outdir /opt/sqoop/gen-src/
