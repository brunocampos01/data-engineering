CREATE FUNCTION row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'
USING JAR 'hdfs:///user/hive/auxjars/hive-contrib-3.1.1.jar';
