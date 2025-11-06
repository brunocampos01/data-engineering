## Install Hadoop
```
sudo su; \
apt-get update; \
apt-get install -qy wget gpg unzip zip curl; \
wget -qO - https://public-repo-1.hortonworks.com/HDP/centos6/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins | apt-key add -; \
wget http://public-repo-1.hortonworks.com/HDP/ubuntu16/2.x/updates/2.6.5.0/hdp.list -O /etc/apt/sources.list.d/hdp.list; \
wget http://public-repo-1.hortonworks.com/HDP-GPL/ubuntu16/2.x/updates/2.6.5.0/hdp.gpl.list -O /etc/apt/sources.list.d/hdp.gpl.list; \
apt-get update; \
apt-get install -qy hadoop hadoop-client hadoop-mapreduce hadoop-yarn hadooplzo hadoop-yarn-nodemanager spark2; \
apt-get clean; \
rm -rf /var/lib/apt/lists; \
curl -s "https://get.sdkman.io" | bash
```

<br/>

# Hadoop Ecosystem 
O Hadoop foi desenhado assumindo que hardware falha. Por isso um dos pilares dele é **reliability** (confiabilidade)

### Apache Framework Basic Modules
- Hadoop Common
- HDFS
- YARN
- MapRedure


### MapReduce
_Otimize seu código. Ele será repetido MUITAS vezes._

Google: creation of the Google File System (GFS), MapReduce (MR), and Bigtable

MR introduced a new parallel programming paradigm, based on functional programming.
In essence, your MR applications interact with the MapReduce system that sends
computation code (map and reduce functions) to where the data resides, favoring
data locality and cluster rack affinity rather than bringing data to your application.

The computational challenges and solutions expressed in Google’s GFS paper pro‐
vided a blueprint for the Hadoop File System (HDFS)


Learning Spark



- As tarefas de mapeamento geralmente processam um input block por vez (usando o FileInputFormat padrão).
- Quanto maior o número de arquivos, maior o número de tarefas de Mapa necessárias e o tempo do trabalho pode ser muito mais lento.

- [1] https://stackoverflow.com/questions/34243134/what-is-sequence-file-in-hadoop

### Hive
Em pipelines Hive puros, há configurações fornecidas para coletar resultados automaticamente em arquivos: `hive.merge.smallfiles`

### YARN
Kill app
```
yarn application -kill application_1620300645822_0084
```

### Hive
- Impala: https://data-flair.training/blogs/impala-tutorial/
- Hive: https://data-flair.training/blogs/hive-tutorial/


---
