# Migração de plataforma
- É quando por exemplo, um cliente muda seu site que ele mesmo mantinha e passa a usar VTEX. Nisso pode ocorrer a mudança de ID, mas os cliente continuam os mesmos.


#### Resolution
- Know if ID PRODUCTS changes
- Know if: existam usuários com um ID que existia na antiga, estes serão sobrescritos. Por exemplo, se na nova apiKey existe um usuário com ID 753 e na antiga também, mesmo que seja outro usuário, o da nova apiKey será sobrescrito pelo da antiga,
- Know which goal or result of migration



### [lojasrede-v7][PLATFORM] Migração de plataforma lojasrede para lojasrede-v7**
_Precisamos migrar a base de eventos da apikey "lojasrede" para a nova apiKey "lojasrede-v7". Atenção aos detalhes:

A plataforma ainda não virou.

- "lojasrede" está em produção atualmente no rede.natura.net (https://www.lojasrede.com.br/)
- "Lojasrede-v7" está apenas em ambiente de homologação https://beta.lojasrede.com.br/).
- Quando forem

Existe um DE/PARA dos IDS eles mudaram na troca de plataforma. 

Eles estão em Anexo, coloquei uma versão em csv e outra em xlsx_


---
## Script

1. Acess machine yarn: `ssh yarn...`

2. In yarn, open repository witn script: `/opt/platform-migrate-spark/`

3. In new terminal, copy file which contens **ID old** mapping **ID news** to machine yarn.

4. Execute script migrate.sh with parameters: `./migrate.sh <JOB> <APIKEY> <NEW_APIKEY> bucket-s3 --begin Y-M-D --end Y-M-D -s`

Ex:<br/>
`./migrate.sh click lojasrede lojasrede-v7 platform-dumps-virginia -b 2018-06-* -e 2018-12-16 -s V2`
<br/>
Ex:`./migrate.sh view comprafoodservice comprafoodservice-v6 platform-dumps-virginia -b 2018-09-01 -e 2018-09-30 -s V2`

5. Consult bucket S3: `hadoop fs -ls s3a://platform-dumps-virginia/cartviews/2018-12-*/lojasrede*`<br/>
if cartView, use for consult shoppingCarts

Ex:</br>
```
hadoop fs -ls s3a://platform-dumps-virginia/views/2018-09-*/comprafoodservice*
19/03/15 13:46:40 INFO Configuration.deprecation: fs.s3a.server-side-encryption-key is deprecated. Instead, use fs.s3a.server-side-encryption.key
-rw-rw-rw-   1 campos campos        608 2019-03-15 13:40 s3a://platform-dumps-virginia/views/2018-09-02/comprafoodservice-v6.gz
-rw-rw-rw-   1 campos campos        614 2018-09-03 00:19 s3a://platform-dumps-virginia/views/2018-09-02/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       4992 2019-03-15 13:40 s3a://platform-dumps-virginia/views/2018-09-03/comprafoodservice-v6.gz
-rw-rw-rw-   1 campos campos       4997 2018-09-04 00:22 s3a://platform-dumps-virginia/views/2018-09-03/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       7607 2019-03-15 13:40 s3a://platform-dumps-virginia/views/2018-09-04/comprafoodservice-v6.gz
-rw-rw-rw-   1 campos campos       7625 2018-09-05 00:21 s3a://platform-dumps-virginia/views/2018-09-04/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       4439 2019-03-15 13:40 s3a://platform-dumps-virginia/views/2018-09-05/comprafoodservice-v6.gz
-rw-rw-rw-   1 campos campos       4443 2018-09-06 00:21 s3a://platform-dumps-virginia/views/2018-09-05/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       2072 2019-03-15 13:40 s3a://platform-dumps-virginia/views/2018-09-06/comprafoodservice-v6.gz
-rw-rw-rw-   1 campos campos       2075 2018-09-07 00:21 s3a://platform-dumps-virginia/views/2018-09-06/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       1176 2018-09-10 00:22 s3a://platform-dumps-virginia/views/2018-09-09/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       2792 2018-09-11 00:40 s3a://platform-dumps-virginia/views/2018-09-10/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       4635 2018-09-12 00:23 s3a://platform-dumps-virginia/views/2018-09-11/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       7450 2018-09-13 00:24 s3a://platform-dumps-virginia/views/2018-09-12/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       3718 2018-09-14 00:26 s3a://platform-dumps-virginia/views/2018-09-13/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       4094 2018-09-15 00:23 s3a://platform-dumps-virginia/views/2018-09-14/comprafoodservice.gz
-rw-rw-rw-   1 campos campos        588 2018-09-16 00:22 s3a://platform-dumps-virginia/views/2018-09-15/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      16912 2018-09-18 00:25 s3a://platform-dumps-virginia/views/2018-09-17/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       9507 2018-09-19 00:24 s3a://platform-dumps-virginia/views/2018-09-18/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       8068 2018-09-20 00:26 s3a://platform-dumps-virginia/views/2018-09-19/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       4705 2018-09-21 00:27 s3a://platform-dumps-virginia/views/2018-09-20/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      11927 2018-09-22 00:26 s3a://platform-dumps-virginia/views/2018-09-21/comprafoodservice.gz
-rw-rw-rw-   1 campos campos       1414 2018-09-28 14:39 s3a://platform-dumps-virginia/views/2018-09-22/comprafoodservice.gz
-rw-rw-rw-   1 campos campos        801 2018-09-24 00:24 s3a://platform-dumps-virginia/views/2018-09-23/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      12007 2018-09-25 00:25 s3a://platform-dumps-virginia/views/2018-09-24/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      44563 2018-09-26 00:25 s3a://platform-dumps-virginia/views/2018-09-25/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      16874 2018-09-27 00:26 s3a://platform-dumps-virginia/views/2018-09-26/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      20522 2018-09-28 00:24 s3a://platform-dumps-virginia/views/2018-09-27/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      18995 2018-09-29 03:27 s3a://platform-dumps-virginia/views/2018-09-28/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      18060 2018-09-30 01:23 s3a://platform-dumps-virginia/views/2018-09-29/comprafoodservice.gz
-rw-rw-rw-   1 campos campos      19201 2018-10-01 00:23 s3a://platform-dumps-virginia/views/2018-09-30/comprafoodservice.gz
```
Havia executado somente : `./migrate.sh view comprafoodservice comprafoodservice-v6 platform-dumps-virginia -b 2018-09-02 -e 2018-09-06 -s V2`

### Problems

```
19/03/15 16:38:13 INFO Client: Application report for application_1551718840772_36222 (state: RUNNING)
19/03/15 16:38:14 INFO Client: Application report for application_1551718840772_36222 (state: RUNNING)
19/03/15 16:38:15 INFO Client: Application report for application_1551718840772_36222 (state: RUNNING)
19/03/15 16:38:16 INFO Client: Application report for application_1551718840772_36222 (state: FINISHED)
19/03/15 16:38:16 INFO Client: 
	 client token: N/A
	 diagnostics: User class threw exception: org.apache.hadoop.mapred.InvalidInputException: Input path does not exist: s3n://platform-dumps-virginia/views/2018-09-01/comprafoodservice.gz
	 ApplicationMaster host: 172.28.7.251
	 ApplicationMaster RPC port: 0
	 queue: root.spark
	 start time: 1552667751814
	 final status: FAILED
	 tracking URL: http://yarn.platform.linximpulse.net:8088/proxy/application_1551718840772_36222/
	 user: brunorozza
Exception in thread "main" org.apache.spark.SparkException: Application application_1551718840772_36222 finished with failed status
	at org.apache.spark.deploy.yarn.Client.run(Client.scala:925)
	at org.apache.spark.deploy.yarn.Client$.main(Client.scala:971)
	at org.apache.spark.deploy.yarn.Client.main(Client.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:674)
	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:180)
	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:205)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:120)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
19/03/15 16:38:16 INFO ShutdownHookManager: Shutdown hook called
19/03/15 16:38:16 INFO ShutdownHookManager: Deleting directory /tmp/spark-84a218a6-6676-427f-b400-59b3eeedde68
brunorozza@platform-jenkins-02:/opt/platform-migration-spark$ 
```

If problem without data, so ignored this date




---

# Migration Client

- O cliente antes tinha a apikey 'drogariavenancio' e mudou de plataforma ficando com a apikey 'drogariavenancio-v7'
- Precisamos exportar todos os usuários que tinhamos na apikey antiga e importar na nova.
- O objetivo é melhorar a performance do Mail
