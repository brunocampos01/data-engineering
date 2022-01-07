#!/bin/bash
sqoop job -delete rtc-ungp-update-releases
sqoop job -delete rtc-ungp-update-links
sqoop job -delete rtc-ungp-update-types
sqoop job -delete rtc-ungp-update-iterations
sqoop job -delete rtc-ungp-update-projects
sqoop job -delete rtc-ungp-update-workitems
sqoop job -delete rtc-ungp-update-int
sqoop job -delete rtc-ungp-update-long
sqoop job -delete rtc-ungp-update-decimal
sqoop job -delete rtc-ungp-update-enumeration
sqoop job -delete rtc-ungp-update-timestamp
sqoop job -delete rtc-ungp-update-boolean
sqoop job -delete rtc-ungp-update-string
sqoop job -delete rtc-ungp-update-string_medium
sqoop job -delete rtc-ungp-update-string_large
sqoop job -delete rtc-ungp-update-timelines
sqoop job -delete rtc-ungp-update-timecodes
sqoop job -delete rtc-ungp-update-timesheets
sqoop job -delete rtc-ungp-update-activities
sqoop job -delete rtc-ungp-update-resources

mkdir /opt/sqoop-1.4.7/gen-lib/ridw.releases
mkdir /opt/sqoop-1.4.7/gen-src/ridw.releases
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.links
mkdir /opt/sqoop-1.4.7/gen-src/ridw.links
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.iterations
mkdir /opt/sqoop-1.4.7/gen-src/ridw.iterations
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.projects
mkdir /opt/sqoop-1.4.7/gen-src/ridw.projects
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.requests
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.requests
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.int
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.int
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.long
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.long
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.decimal
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.decimal
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.enumeration
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.enumeration
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.timestamp
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.timestamp
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.boolean
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.boolean
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.string
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.string
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.string_medium
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.string_medium
mkdir /opt/sqoop-1.4.7/gen-lib/ricalm.string_large
mkdir /opt/sqoop-1.4.7/gen-src/ricalm.string_large
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.timelines
mkdir /opt/sqoop-1.4.7/gen-src/ridw.timelines
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.timecodes
mkdir /opt/sqoop-1.4.7/gen-src/ridw.timecodes
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.timesheets
mkdir /opt/sqoop-1.4.7/gen-src/ridw.timesheets
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.activities
mkdir /opt/sqoop-1.4.7/gen-src/ridw.activities
mkdir /opt/sqoop-1.4.7/gen-lib/ridw.resources
mkdir /opt/sqoop-1.4.7/gen-src/ridw.resources

sqoop job --create rtc-ungp-update-releases \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_releases --hive-overwrite \
    --table ridw.vw_release --split-by 'PROJECT_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ungp/ridw.releases \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.releases \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.releases

sqoop job --create rtc-ungp-update-links \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_links --hive-overwrite \
    --table ridw.vw_request_relational_link --split-by 'REQUEST_RELATION_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.links \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.links \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.links

sqoop job --create rtc-ungp-update-types \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_types --hive-overwrite \
    --table ridw.vw_request_type --split-by 'PROJECT_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.types \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.types \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.types

sqoop job --create rtc-ungp-update-iterations \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_iterations --hive-overwrite \
    --table ridw.vw_iteration --split-by 'PROJECT_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.iterations \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.iterations \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.iterations

sqoop job --create rtc-ungp-update-projects \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_projects --hive-overwrite \
    --table ridw.vw_project --split-by 'PROJECT_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.projects \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.projects \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.projects

sqoop job --create rtc-ungp-update-workitems \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitems --hive-overwrite \
    --table ridw.vw_request --split-by 'REQUEST_ID' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.requests \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.requests \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.requests

sqoop job --create rtc-ungp-update-int \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_int --hive-overwrite \
    --table ricalm.vw_rqst_int_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.int \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.int \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.int

sqoop job --create rtc-ungp-update-long \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_int --hive-overwrite \
    --table ricalm.vw_rqst_long_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.long \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.long \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.long

sqoop job --create rtc-ungp-update-decimal \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_decimal --hive-overwrite \
    --table ricalm.vw_rqst_decimal_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.decimal \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.decimal \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.decimal

sqoop job --create rtc-ungp-update-enumeration \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_enumeration --hive-overwrite \
    --table ricalm.vw_rqst_enumeration --split-by 'REQUEST_ENUMERATION_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.enumeration \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.enumeration \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.enumeration

sqoop job --create rtc-ungp-update-timestamp \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_timestamp --hive-overwrite \
    --table ricalm.vw_rqst_timestamp_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.timestamp \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.timestamp \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.timestamp

sqoop job --create rtc-ungp-update-boolean \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_boolean --hive-overwrite \
    --table ricalm.vw_rqst_bool_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.boolean \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.boolean \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.boolean

sqoop job --create rtc-ungp-update-string \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_string --hive-overwrite \
    --table ricalm.vw_rqst_string_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.string \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.string

sqoop job --create rtc-ungp-update-string_medium \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_medium --hive-overwrite \
    --table ricalm.vw_rqst_string_m_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string_medium \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.string_medium \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.string_medium

sqoop job --create rtc-ungp-update-string_large \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_workitem_attributes_string_large --hive-overwrite \
    --table ricalm.vw_rqst_string_l_ext --split-by 'REQUEST_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ricalm.string_large \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ricalm.string_large \
    --outdir /opt/sqoop-1.4.7/gen-src/ricalm.string_large

sqoop job --create rtc-ungp-update-timelines \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_timelines --hive-overwrite \
    --table ridw.vw_timeline --split-by 'TIMELINE_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timelines \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.timelines \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.timelines

sqoop job --create rtc-ungp-update-timecodes \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_timecodes --hive-overwrite \
    --table ridw.vw_timecode --m 1 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timecodes \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.timecodes \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.timecodes

sqoop job --create rtc-ungp-update-timesheets \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_timesheets --hive-overwrite \
    --table ridw.vw_timesheet --split-by 'TIMESHEET_ID' --m 25 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.timesheets \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.timesheets \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.timesheets

sqoop job --create rtc-ungp-update-activities \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_activities --hive-overwrite \
    --table ridw.vw_activity --split-by 'ACTIVITY_ID' --m 50 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.activities \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.activities \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.activities

sqoop job --create rtc-ungp-update-resources \
    -- import \
    --connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
    --username sidrcons \
    --password-file hdfs:///user/sqoop/rtc.password \
    --hive-import --hive-table raw.rtc_ungp_resources --hive-overwrite \
    --table ridw.vw_resource --split-by 'RESOURCE_ID' --m 10 \
    --compress \
    --input-enclosed-by '"' \
    --input-escaped-by '\"' \
    --optionally-enclosed-by '\"' \
    --fields-terminated-by '|' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --target-dir /user/hadoop/xpto/rtc/ridw.resources \
    --delete-target-dir \
    --bindir /opt/sqoop-1.4.7/gen-lib/ridw.resources \
    --outdir /opt/sqoop-1.4.7/gen-src/ridw.resources

#sqoop job --create rtc-ungp-update-categories \
#-- import \
#--connect jdbc:db2://flnsrvungp4003.xpto.com.br:50000/dw \
#--username sidrcons \
#--password-file hdfs:///user/sqoop/rtc.password \
#--table ricalm.vw_rqst_category_ext --m 1 \
#--hive-import --hive-table staging.rtc_categories \
#--compress \
#--input-enclosed-by '"' \
#--input-escaped-by '\"' \
#--optionally-enclosed-by '\"' \
#--fields-terminated-by '|' \
#--null-string '\N' \
#--null-non-string '\N' \
#--target-dir rtc/ricalm.categories \
#--delete-target-dir \
#--bindir /opt/sqoop-1.4.7/gen-lib/ \
#--outdir /opt/sqoop-1.4.7/gen-src/ \
