## [deliverysupermuffato] reprocessar dados da Busca


Pessoal, dia 02/04 tivemos alguns problemas com cookies na deliverysupermuffato e para diminuir a criação de cookies, removemos o grava (extraction da busca).
O problema é que eu não executei o job para mudar o metaState para production e ficamos sem métricas no dashboard desde o dia que fizemos a task.
Precisamos que seja reprocessado os dados da busca para temos informações no dashboard novamente.

## Steps

1. Check grafana this store: http://grafana.bigdata.chaordicsystems.com:3000/
- view store and which empty days

2. Check which steps failure in SEARCH flow<br/>
```
cd $HOME/pp-scripts/hbaseQuery
python hbaseToCsv.py get deliverysupermuffato 2019-04-01 2019-04-30 site@site,page@busca generaldata
```


3. Rexecution in machine Luigi the steps with not process<br/>
ex: process traking<br/>
`hadoop jar /mnt/pp-mapreduces/target/pp-mapreduces-41-jar-with-dependencies.jar com.neemu.mr.tracking.TrackerMRConfig -output /tmp/deliverysupermuffato_2019-04-02-00-00 -startDate 2019-04-02 -endDate 2019-04-03 -userName platform -reduces 80 -reprocess -hbase`


Reprocessar dados AGREGA
`2019-05-06 04:07:55,643 INFO: Running command: hadoop jar /mnt/pp-mapreduces/target/pp-mapreduces-41-jar-with-dependencies.jar com.neemu.searchlog.mr.agregaterequests.AgregateRequestsMRConfig -input s3a://bigdata-dumps/raw/searchlog/2019-05-06,s3a://bigdata-dumps/raw/searchlog/2019-05-05 -output /tmp/multiloja_2019-05-06-04-07 -startDate 2019-05-05 -endDate 2019-05-05 -userName platform -reduces 20`

---

## Reprocessar o fluxo da Busca a partir do `traking -hbase`
- O kafka joga dados no S3, exemplo: [chaordic-dumps/daily/pageViews/2019/05/19/ultrafarma](https://s3.console.aws.amazon.com/s3/buckets/chaordic-dumps/daily/pageViews/2019/05/19/ultrafarma/?region=us-east-1&tab=overview)
- `join`
- Agrega
- Traking -hbase

---

- Objetivo é jogar dados de uma loja para um loja sample

1. Dump dos dados 
`hadoop fs -text s3a://bigdata-dumps/sessions-transformed/casaevideo/190101/* > /tmp/casa_e_video`

2. Replace dos nomes das lojas
`sed -i 's/origin/new/g' /tmp/casa_e_video`

3. Reprocessar o agrega 
- `-input` são os path de onde deve pegar os dados para executar o agrega.
- `-output` é para onde os dados processados vão ser enviados.
```bash
hadoop jar pp-mapreduces-41-jar-with-dependencies.jar com.neemu.mr.agrega.AgregaMRConfig \
       -output s3n://bigdata-dumps/sessions/teste-transformed/coty \
       -input s3a://bigdata-dumps/raw/realtime-transformed/2019-05-16/coty/*, \
           s3a://bigdata-dumps/raw/realtime-transformed/2019-05-21/coty/*, \
           s3a://bigdata-dumps/raw/realtime-transformed/2019-05-22/coty/*, \
           s3a://chaordic-dumps/daily/busca-searchapirequest-join/2019/05/21/coty/*, \
           s3a://chaordic-dumps/daily/busca-searchapirequest-join/2019/05/22/coty \
       -startDate 2019-05-16 \
       -endDate 2019-05-22 \
       -userName platform \
       -reduces 80 \
       -store coty
```

4. Reprocessar o traking
```
hadoop jar pp-mapreduces-41-jar-with-dependencies.jar com.neemu.mr.tracking.TrackerMRConfig \
       -output /tmp/coty_2019-01-01-31 \
       -startDate 2019-01-01 \
       -endDate 2019-01-31 \
       -userName platform \
       -reduces 80 \
       -reprocess \
       -hbase \
       -store casaevideo
```

5. Reprocessar o `import`
```
./runImportForAllStores.sh -sd 2019-05-16 -ed 2019-05-22 -store coty -ddl dropAndCreate
```

6. Reprocessar o `neemu-etl`
```
 bash scripts/run-neemu-etl.sh -e prod -f 2019-05-16 -l 2019-05-22 -s coty
 ```
 
 ---
 
# Dados da Busca no Dashborad
 
Para ter dados no dashboard da busca é necessário ter dados no HBASE do `provider@neemusearch`
- Ex:<br/>
```bash
python hbaseToCsv.py get agrotama 2019-05-27 2019-05-27 site@site,page@busca,provider@neemusearch generaldata
  
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	purchase	27
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	search	1009
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	access	1400
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	revenue	19573.70
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	session	568
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	sessionwithpurchase	23
# agrotama	agrotama,2019-05-27,site@site,page@busca,provider@neemusearch	click	632
```
  
Pode haver dados de busca que sejam do próprio cliente. 
-Ex:<br/>
```bash
python hbaseToCsv.py get bobo 2019-05-27 2019-05-27 site@site,page@busca,provider@bobo generaldata

# bobo	bobo,2019-05-27,site@site,page@busca,provider@bobo	click	5
# bobo	bobo,2019-05-27,site@site,page@busca,provider@bobo	search	57
# bobo	bobo,2019-05-27,site@site,page@busca,provider@bobo	session	38
# bobo	bobo,2019-05-27,site@site,page@busca,provider@bobo	access	60
```

---

# Checar os Acessos de um Cliente

http://grafana.bigdata.chaordicsystems.com:3000/dashboard/db/access-monitor-multitransform?var-loja=bobo&var-pagetype=All&var-aggregate=1m&from=now%2Fw&to=now%2Fw
