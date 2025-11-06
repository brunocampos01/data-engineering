
# Data Types

## RC
TODO

## HAR (Hadoop Archive)
- This structure are **mappers** of files
- Reduce the NameNode memory usage
- Immutable
- Useful only backup
- A Hadoop archive directory contains metadata and data (part-*) 
- The `_index` and `_masterindex` file contains the **name of the files** that are `part-` of the archive and the location within the part files.

![image](https://user-images.githubusercontent.com/12896018/161391269-52a92691-2f99-4c0f-b450-617c23275638.png)

#### Disadvantages
- Reading HAR files is slow, because it requires to access two index files and then finally the data file.
- One way to access HAR files is use hadoop command

#### Example

```bash
hdfs dfs -mkdir /archivable_dir

hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/1.txt
hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/2.txt
hadoop fs -copyFromLocal ~/tested_file.txt /archivable_dir/3.txt

hadoop fs -ls  /archivable_dir
Found 3 items
-rw-r--r--   1 campos supergroup          7 2020-12-16 13:03 /archivable_dir/1.txt
-rw-r--r--   1 campos supergroup          7 2020-12-16 13:03 /archivable_dir/2.txt
-rw-r--r--   1 campos supergroup          7 2020-12-16 13:03 /archivable_dir/3.txt

hadoop fs -cat  /archivable_dir/1.txt
Test 1
```

```bash
 3_small_files.har - the name of HAR
# -p /archivable_dir parent directory of small files to archive
# . - archives all files in /archivable_dir
# small_files - the directory of created HAR
hadoop archive -archiveName 3_small_files.har -p /archivable_dir -r 1 . small_files

hadoop fs -ls  /user/campos/small_files/3_small_files.har
Found 4 items
-rw-r--r--   1 campos supergroup          0 2020-12-16 13:11 /user/campos/small_files/3_small_files.har/_SUCCESS
-rw-r--r--   5 campos supergroup        266 2020-12-16 13:11 /user/campos/small_files/3_small_files.har/_index
-rw-r--r--   5 campos supergroup         23 2020-12-16 13:11 /user/campos/small_files/3_small_files.har/_masterindex
-rw-r--r--   1 campos supergroup         21 2020-12-16 13:11 /user/campos/small_files/3_small_files.har/part-0

hadoop fs -cat  /user/campos/small_files/3_small_files.har/part-0
Test 1
Test 1
Test 1

hadoop fs -cat  /user/campos/small_files/3_small_files.har/_index
%2F dir 1481717019765+493+campos+supergroup 0 0 1.txt 2.txt 3.txt 
%2F1.txt file part-0 0 7 1481716999330+420+campos+supergroup 
%2F2.txt file part-0 7 7 1481717016200+420+campos+supergroup 
%2F3.txt file part-0 14 7 1481717019748+420+campos+supergroup

hadoop fs -cat  /user/campos/small_files/3_small_files.har/_masterindex
3 
0 1394155366 0 266 
```
- [How HAR ( Hadoop Archive ) works](https://community.cloudera.com/t5/Community-Articles/How-HAR-Hadoop-Archive-works/ta-p/249141)

<br/>

## SequenceFile
- "Container for huge number of small files"
- It is splittable, so is suitable for MapReduce
- It is compression supported
- Binary
- Key-value
- Good for ingested

![image](https://user-images.githubusercontent.com/12896018/161391776-74c8c37d-864e-4e71-bc09-669021c5a074.png)

#### Disadvantages
- Not good for Hive
- Python is not support

<br/>

## Avro 
- Row-based
- Hadoop native
- Good choice for Kafka
- Ideal for:
  - write-heavy
  - **large binary data**
  - read records in their entirety 
- Typed schema
- Data schemas in JSON
- Support block compression
- Splittable
- Avro files are generally used to store intermediate data when:
  - The data is written once and read once.
  - The reader does not filter the data.
  - The reader reads the data sequentially.

<br/>

![image](https://user-images.githubusercontent.com/12896018/161400240-6220a89c-ffc1-4726-bdab-48bea3de9bad.png)

<br/>

## Parquet
- Spark native
- Used in Airbnb
- Column-oriented
- Ideal for:
  - read-heavy analytical (Write Once Read Many)
- Only the required columns will be retrieved/read, this reduces disk I/O
- Immutable
- Typed schema (Can be read and write using Avro API and Avro Schema)
- Support block compression (up to 75% with Snappy compression)
- Supports efficient queries

<br/>

![image](https://user-images.githubusercontent.com/12896018/161400876-0b46a565-5a4c-4e1a-a14c-bf99969a0270.png)

<br/>

## ORC
- ACID Support
- Built-in Indexes
- Best compression rate of all
- Used in Facebook

![image](https://user-images.githubusercontent.com/12896018/161401616-6fa09f71-a3fb-4e01-bcd2-20d2437a270d.png)


<br/>

## Iceberg
TODO

<br/>

## Thrift
![image](https://user-images.githubusercontent.com/12896018/161335025-f079d270-a44a-41c8-a637-50b20d4eb055.png)


## Lessons learned from performance tests
👉 JSON is the standard for communication on the Internet. APIs and websites communicate constantly with JSON thanks to its convenient features such as clearly defined schemes, the ability to store complex data.

👉 Whatever you do, never use the JSON format. In almost all tests it proved to be the worst format to use.

👉 Parquet and Avro are definitely more optimized for the needs of Big Data — splitability, compression support, excellent support for complex data structures, but readability and writing speed are quite poor.

👉 The unit of paralleling data access in the case of Parquet and Avro is an HDFS file block — this makes it very easy to distribute the processing evenly across all resources available on the Hadoop cluster.

👉 When choosing a data storage format in Hadoop, you need to consider many factors, such as integration with third-party applications, the evolution of the schema, support for specific data types ... But if you put a performance in the first place, the above tests prove convincingly that Parquet is your best choice.

👉 Notably, the algorithms compression have played a significant role not only in reducing the amount of data but also in improving performance when swallowing and accessing data. But this part of the picture has not been tested;

👉 Apache Avro proved to be a fast universal encoder for structured data. With very efficient serialization and deserialization, this format can guarantee very good performance when all the attributes of a record are accessed at the same time.

👉 CSV should typically be the fastest to write, JSON the easiest to understand for humans, and Parquet the fastest to read a subset of columns, while Avro is the fastest to read all columns at once.

👉 According to test data, columnar formats like Apache Parquet delivered very good flexibility between fast data ingestion, fast random data retrieval, and scalable data analytics. In many cases, this provides the additional advantage of simple storage systems, as only one type of technology is needed to store data and maintain different use cases (random access to data and analytics).

🧐 Tip. Columnar formats are typically used where several columns need to be requested rather than all columns as their column-oriented storage design is well suited for this purpose. On the other hand, row-based formats are used where all fields in a row need to be accessed.

This is why Avro is usually used to store the raw data because all fields are usually required during ingestion. And Parquet is used after preprocessing for further analytics because usually all fields are no longer required there.


#### References:
- [1] https://eng.uber.com/hdfs-file-format-apache-spark/
- [2] https://www.oreilly.com/library/view/operationalizing-the-data/9781492049517/ch04.html
- [3] https://oswinrh.medium.com/parquet-avro-or-orc-47b4802b4bcb
- [4] https://luminousmen.com/post/big-data-file-formats
- [5] [orc](https://www.learntospark.com/2020/02/how-to-read-and-write-orc-file-in-apache-spark.html)

---
