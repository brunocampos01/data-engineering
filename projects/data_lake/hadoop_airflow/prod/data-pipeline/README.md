# Batch: Data Pipeline
In this documentation there are procedures for managing the data pipeline.

## Requirements
| Requisite      | Version  |
| -------------- | -------- |
| Python         | 3.8.10   |
| Virtualenv     | 20.0.17  |
| Docker         | 19.03.12 |
| Docker-compose | 1.26.2   |
| Snappy         | 1.1.8    |

## **Prepare Environment**
### Prepare Python
```bash
cd /ssddisk/data-pipeline
virtualenv -p python3 venv
source venv/bin/activate

pip3 install --require-hashes -r requirements.txt
```

### Make Environment Variables Available
```bash
set -a # export all variables
source configs/.env
```

### Generate Settings Files
```bash
python3  $AIRFLOW_HOME/src/utils/generate_conn_config.py
python3  $AIRFLOW_HOME/src/utils/generate_dag_config.py
```

### Set up Broker, Cache and Database (Metadata)
```bash
cd $AIRFLOW_HOME/../../cache/
docker-compose up -d

cd $AIRFLOW_HOME
docker-compose up -d
```

### Start Metadata Database
```bash
airflow db init
```

### Create User for Webserver
```bash
airflow users create \
    --username $USER\
    --firstname $USER\
    --lastname $USER\
    --role Admin\
    --email $USER@xpto.company
```

### Create the Connections, Variables in Airflow, Databases, Tables and Partitions at Hive
```bash
python3 $AIRFLOW_HOME/src/utils/prepare_env.py
```

### Start Services
- Webserver
```bash
airflow webserver \
    --stdout $AIRFLOW_HOME/logs/airflow-webserver.out \
    --stderr $AIRFLOW_HOME/logs/airflow-webserver.err \
    --log-file $AIRFLOW_HOME/logs/airflow-webserver.log \
    --pid $AIRFLOW_HOME/logs/airflow-webserver.pid \
    --port 8080 \
    --daemon
```

- Celery Webserver (queue tasks)
```bash
airflow celery flower \
    --stdout $AIRFLOW_HOME/logs/airflow-flower.out \
    --stderr $AIRFLOW_HOME/logs/airflow-flower.err \
    --log-file $AIRFLOW_HOME/logs/airflow-flower.log \
    --pid $AIRFLOW_HOME/logs/airflow-flower.pid \
    --daemon
```

- Scheduler
```bash
airflow scheduler \
    --stdout $AIRFLOW_HOME/logs/airflow-scheduler.out \
    --stderr $AIRFLOW_HOME/logs/airflow-scheduler.err \
    --log-file $AIRFLOW_HOME/logs/airflow-scheduler.log \
    --pid $AIRFLOW_HOME/logs/airflow-scheduler.pid \
    --daemon
```

- Worker
```bash
airflow celery worker \
    --stdout $AIRFLOW_HOME/logs/airflow-worker.out \
    --stderr $AIRFLOW_HOME/logs/airflow-worker.err \
    --log-file $AIRFLOW_HOME/logs/airflow-worker.log \
    --pid $AIRFLOW_HOME/logs/airflow-worker.pid \
    --daemon
```

---

## Run Tests

### Access the Environment
```bash
cd /ssddisk/data-pipeline
source venv/bin/activate
```

### Make Environment Variables Available
```bash
set -a # export all variables
source configs/.env
```

### Run the Tests
```bash
pytest tests -vv
```

---

## Delete Projct
```
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
docker volume prune --force
docker network prune --force

sudo kill -9 $(ps aux | grep 'airflow' | awk '{print $2}')
sudo kill -9 $(ps aux | grep 'celery' | awk '{print $2}')
sudo kill -9 $(ps aux | grep 'flower' | awk '{print $2}')

sudo rm -rf /ssddisk/data-pipeline/logs/*.err
sudo rm -rf /ssddisk/data-pipeline/logs/*.out
sudo rm -rf /ssddisk/data-pipeline/logs/*.pid
sudo rm -rf /ssddisk/data-pipeline/logs/*.log
sudo rm -rf /ssddisk/data-pipeline/logs/scheduler*
```

---

### Design Pattern
- The data pipeline design follows the pattern of **template** where in the folder `plugins/operators`, `plugins/validators` and `plugins/tranfers` keep the abstract classes with a default method (execute).
<br/>
This method contains the logic of the algorithm. It is composed only of calls.
- In `plugins/hooks`, `plugins/security` and `plugins/utils` has class and concret functions that implementam everything that is done in the abstract classes. Many of them extend airflow classes.
- Motivation to chose this pattern: the **template** is based on inheritance and as the data pipeline tool already delivers a lot of ready-made functionality, it facilitated the extension and reuse of code.

---

<p  align="left">
<br/>
<a href="mailto:brunocampos01@gmail.com" target="_blank"><img src="https://github.com/brunocampos01/devops/blob/master/images/email.png" alt="Gmail" width="30">
</a>
<a href="https://stackoverflow.com/users/8329698/bruno-campos" target="_blank"><img src="https://github.com/brunocampos01/devops/blob/master/images/stackoverflow.png" alt="Stackoverflow" width="30">
</a>
<a href="https://www.linkedin.com/in/brunocampos01" target="_blank"><img src="https://github.com/brunocampos01/devops/blob/master/images/linkedin.png" alt="LinkedIn" width="30"></a>
<a href="https://github.com/brunocampos01" target="_blank"><img src="https://github.com/brunocampos01/devops/blob/master/images/github.png" alt="GitHub" width="30"></a>
<a href="https://medium.com/@brunocampos01" target="_blank"><img src="https://github.com/brunocampos01/devops/blob/master/images/medium.png" alt="Medium" width="30">
</a>
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png",  align="right" /></a><br/>
</p>
