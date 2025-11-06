
No HDFS pode acontecer o problema de blocos pequenos e o outro problema é de blocos muito grandes.
Por exmeplo, arquivo de string, txt ou até mesmo um campo do avro que seja muito grande pode gerar o problema de de blocos muito grande.
Por exmplo, aqui tenho um arquivo avro com o seguinte schema avro:
```
{
  "type": "record",
  "name": "xxx",
  "fields": [
    {
      "name": "ID_FILE",
      "type": "string"
    },
    {
      "name": "BL_CONTENT",
      "type": "string"
    }
  ]
}
```

Ao importar dados em avro para o HDFS, obtive o seguite erro:

```
└─▪ hdfs dfs -text /data/raw/xxx/2021/01/27/* | wc -l
-text: Fatal internal error
org.apache.avro.AvroRuntimeException: java.io.IOException: Block size invalid or too large for this implementation: 6076182580
	at org.apache.avro.file.DataFileStream.hasNextBlock(DataFileStream.java:278)
	at org.apache.avro.file.DataFileStream.hasNext(DataFileStream.java:197)
	at org.apache.hadoop.fs.shell.Display$AvroFileInputStream.read(Display.java:287)
	at java.io.InputStream.read(InputStream.java:170)
	at java.io.InputStream.read(InputStream.java:101)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:92)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:66)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:127)
	at org.apache.hadoop.fs.shell.Display$Cat.printToStdout(Display.java:101)
	at org.apache.hadoop.fs.shell.Display$Cat.processPath(Display.java:96)
	at org.apache.hadoop.fs.shell.Command.processPathInternal(Command.java:367)
	at org.apache.hadoop.fs.shell.Command.processPaths(Command.java:331)
	at org.apache.hadoop.fs.shell.Command.processPathArgument(Command.java:304)
	at org.apache.hadoop.fs.shell.Command.processArgument(Command.java:286)
	at org.apache.hadoop.fs.shell.Command.processArguments(Command.java:270)
	at org.apache.hadoop.fs.shell.FsCommand.processRawArguments(FsCommand.java:119)
	at org.apache.hadoop.fs.shell.Command.run(Command.java:177)
	at org.apache.hadoop.fs.FsShell.run(FsShell.java:326)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:90)
	at org.apache.hadoop.fs.FsShell.main(FsShell.java:389)
Caused by: java.io.IOException: Block size invalid or too large for this implementation: 6076182580
	at org.apache.avro.file.DataFileStream.hasNextBlock(DataFileStream.java:269)
	... 20 more
0
```

O campo BL_CONTENT é uma string e o seu maior tamanho pode chegar a passar de 4GB !!!
A solução foi parser essa string em várias outras linhas de avro. O schema avro mantive com o memso formato, contudo o resultado foi:

```
{
  "ID_FILE": "33333333-f6c4-4025-896b-660472acf4b0",
  "BL_CONTENT": "555555555555555              ABCS - STATE CITY                       SE00000000002020120120201231241"
}

...

{
  "ID_FILE": "33333333-f6c4-4025-896b-660472acf4b0",
  "BL_CONTENT": "666666666666666              ABCS - STATE CITY                       SE00000000000000000000000000000"
}
```

A quantidade de blocos diminuiu com essa alteração. Ficando um consumo mais eficiente.
```
└─▪ hdfs fsck /data/raw/xxx/2021/27/01/part-0-mapred_xxx_000000000000000_27-01-2021.avro -files -blocksdfsadmin
Connecting to namenode via ...
part-0-mapred_xxx_000000000000000_27-01-2021.avro at Sat May 08 23:44:51 BRT 2021
/data/raw/xxx/2021/01/27/part-0-mapred_xxx_000000000000000_27-01-2021.avro 1266322123 bytes, replicated: replication=3, 5 block(s):  OK
0. BP-1249934417-100.107.0.150-1590184165334:blk_1089263231_15537708 len=268435456 Live_repl=3
1. BP-1249934417-100.107.0.150-1590184165334:blk_1089263232_15537709 len=268435456 Live_repl=3
2. BP-1249934417-100.107.0.150-1590184165334:blk_1089263233_15537710 len=268435456 Live_repl=3
3. BP-1249934417-100.107.0.150-1590184165334:blk_1089263234_15537711 len=268435456 Live_repl=3
4. BP-1249934417-100.107.0.150-1590184165334:blk_1089263235_15537712 len=192580299 Live_repl=3
Status: HEALTHY
 Number of data-nodes:	7
 Number of racks:		1
 Total dirs:			0
 Total symlinks:		0

Replicated Blocks:
 Total size:	1266322123 B
 Total files:	1
 Total blocks (validated):	5 (avg. block size 253264424 B)
```
