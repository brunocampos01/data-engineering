## HDFS Main Features
- Java-based 
- master/worker
- batch oriented 
- HDFS is based on disk I/O operations
- blocks abstraction - files are stored in blocks. Each block has a default size of 128MB that can be overridden at the moment of creating file. These blocks are stored on DataNodes. But the blocks of one file won't be necessarily stored in the same DataNode.
- fault tolerance - HDFS offers a reliability for stored files.
- TCP/IP communication
- commodity hardware
- Principle: Write Once, Read Many
- created files were write-once
- HDFS splits large data files into chunks of 256 megabytes (MB)
- Default block = 128 MB
- node max 48 TB

### Filesystem
- Ext3 é recomendado pelo Yahoo
- Ext4 é recomendado pela [Cloudera](https://docs.cloudera.com/documentation/enterprise/5-7-x/topics/install_cdh_file_system.html)

## Architecture
- the NameNode since HDFS requires that all meta-data operations such as file creation, deletion or file length extensions round-trip through the NameNode

<br/>

![image](https://user-images.githubusercontent.com/12896018/161322509-73d93beb-a323-4557-b78a-9b1f84d1f9d6.png)

### Read File
![image](https://user-images.githubusercontent.com/12896018/161320802-c2485811-8aec-4c57-bcae-4b154c0f0a34.png)


### Write File
![image](https://user-images.githubusercontent.com/12896018/161321496-45dbdd6d-0f68-4e87-b0b4-7cafcdd2cc01.png)

### Storage Data
Files abstraction in HDFS is based on blocks. Each file is equal to a sequence of blocks. These blocks are not only the components of a file but also the unit of replication. They also influence the frequency of files writing (described in next section).

By default, a block has 128MB (64MB in Hadoop 1)  but the size can be overridden at the moment of file creation. The same rule applies to the number of file replicas. This parameter also can be defined specificaly for a file and it's called replication factor.


#### What happens if the file has smaller size than the block size ? 
Files are physically stored on underlying file system. This file system considers HDFS files as any other files and stores them as increments on its own block size. So even if a file is smaller than HDFS block size, the file won't take the whole block size physically.

However HDFS is not well suited to handle files smaller than blocks. It's because of memory and disk seeks. NameNode holds information about files locations in memory with objects having around 150 bytes. Thus, less files are created, less memory is used. The same logic applies to physical files. More small files are stored, more disk seeks must be done to read the data.

Every HDFS block is also accompanied by a meta file containing CRC32 checksum of this block. This metadata file is used to detect block as corrupted .


### Data locality Principle
In Hadoop, Data locality is the process of moving the computation close to where the actual data resides on the node, instead of moving large data to computation. 

This minimizes network congestion and increases the overall throughput of the system.

<br/>

![image](https://user-images.githubusercontent.com/12896018/161332420-daf05329-5c35-4d1f-8eca-45d1e65a54d7.png)



#### Example 1
- BLock = 128 MB
- File  = 149 MB


```sh
hdfs fsck path/file_01 -files -blocks


# path/file_01 156246254 bytes, replicated: replication=3, 2 block(s):  OK

# 0. BP-109690795-172.18.0.98-1599605041004:blk_1073838597_97776 len=134217728 Live_repl=3
# 1. BP-109690795-172.18.0.98-1599605041004:blk_1073838599_97778 len=22028526 Live_repl=3


Status: HEALTHY
 Number of data-nodes:	4
 Number of racks:		1
 Total dirs:			0
 Total symlinks:		0

Replicated Blocks:
 Total size:	156246254 B
 Total files:	1
 Total blocks (validated):	2 (avg. block size 78123127 B)
 Minimally replicated blocks:	2 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	3
 Average block replication:	3.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
 Blocks queued for replication:	0
```

#### Example 2
- BLock = 128 MB
- File  = 46.7 MB
 
```sh
hdfs fsck path/file_02 -files -blocks


# path/file_02 48937069 bytes, replicated: replication=3, 1 block(s):  OK

# 0. BP-109690795-172.18.098 - 1599605041004:blk_1073838600_97779 len=48937069 Live_repl=3

Status: HEALTHY
 Number of data-nodes:	4
 Number of racks:		1
 Total dirs:			0
 Total symlinks:		0

Replicated Blocks:
 Total size:	48937069 B
 Total files:	1
 Total blocks (validated):	1 (avg. block size 48937069 B)
 Minimally replicated blocks:	1 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	3
 Average block replication:	3.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
 Blocks queued for replication:	0
```

## Small Files Issue
![image](https://user-images.githubusercontent.com/12896018/161390912-84f874aa-183c-474a-8185-c62937f040cd.png)

![image](https://user-images.githubusercontent.com/12896018/161390892-0bf476b3-0bb3-4121-9f0f-905e86c290c9.png)

<br/>

We can see that we require more than 100x memory on the Namenode heap to store the multiple small files as opposed to one big 192MB file

---

<br/>
<br/>
 
## HDFS Main Commands
- Read Json
```bash
hdfs dfs -text /data/raw/ccg/2018/11/04/* | jq
```

- Disk usage
```bash
hdfs dfs -du -h /data/raw/cte/*/*/*/* 
```

- Copy
```bash
hdfs dfs -cp /data/raw/acc* /tmp
```

- Word count
```bash
hdfs dfs -text /data/raw/acc/2014/01/01/* | wc -l
```

- Move
```bash
hdfs dfs -mv /data/raw/acc/2021/01/06/* /data/raw/acc/2021/01/06/acc_original_$(hadoop jar /ssddisk/SAT_BIG_DATA/bda-batch-data-pipeline/libs/avro-tools-1.10.1.jar count /data/raw/acc/2021/01/06).avro.snappy
```

- ls
```bash
hdfs dfs -ls  /tmp/acc
```

- Check block corrupt
```bash
hdfs fsck -list-corruptfileblocks
```

- Remove files
```
hdfs dfs -rm <path_file_hdfs>
```

- Test Spark + YARN + HDFS
```
spark-submit \
           --class org.apache.spark.examples.SparkPi \
           --master yarn \
           --deploy-mode cluster \
           /opt/cloudera/parcels/CDH/jars/spark-examples*.jar 10 /data/raw/nfe/2020/01/01/nfe_original_3125.avro.snappy
```

- Change own
```
sudo -u hdfs hdfs dfs -chown -R <hdfs_folder>
```


#### Using Hadoop Command
- Open file
```
hadoop fs -text /user/platform/etl/conf/enabledStores.json
```

- Remove file
```
hadoop fs -rm /user/platform/etl/conf/enabledStores.json*
```

- Put file
```
hadoop fs -put enabledStores.json /user/platform/etl/conf/
```

- Execute jar: parquet-tools
```
hadoop jar /ssddisk/SAT_BIG_DATA/data-pipeline/batch/libs/parquet-tools-1.10.99.7.2.8.0-228.jar
```

- Execute jar: avro-tools
```
hadoop jar /ssddisk/SAT_BIG_DATA/bda-batch-data-pipeline/libs/avro-tools-1.10.1.jar
```

- Execute hadoop-streaming: Python code
```
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
         -mapper "python mapper.py" \
         -reducer "python reducer.py" 
         -input 201808 BF Amostra.csv 
         -output Amostra.csv
```

## Environment Variables
- HDFS Home
```
$HADOOP PREFIX
```



---

#### References:
- [1] https://eng.uber.com/hdfs-file-format-apache-spark/
- [2] https://www.waitingforcode.com/hdfs
- [3] [Big Data Analytics with Hadoop](https://www.slideshare.net/PhilippeJulio/hadoop-architecture/2-WHO_AM_I_Big_Data)
- [4] https://glouppe.github.io/info8002-large-scale-data-systems/?p=lecture9.md#63
- [5] https://glouppe.github.io/info8002-large-scale-data-systems/?p=lecture6.md#1
- [6] [Best reference](https://data-flair.training/blogs/hadoop-tutorial/)
- [7] [The Small Files Problem](https://blog.cloudera.com/the-small-files-problem/)
- [8] https://cloudera.ericlin.me/
- [9] https://blog.cloudera.com/small-files-big-foils-addressing-the-associated-metadata-and-application-challenges/

