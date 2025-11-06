hdfs dfs -mkdir /archivable_dir

hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/1.txt
hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/2.txt
hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/3.txt

hadoop fs -ls  /archivable_dir
hadoop fs -cat  /archivable_dir/1.txt

sleep 10

hadoop archive -archiveName 3_small_files.har -p /archivable_dir -r 1 . small_files


hadoop fs -ls  /user/campos/small_files/3_small_files.har
hadoop fs -cat  /user/campos/small_files/3_small_files.har/part-0
