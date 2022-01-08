#!/bin/bash
# Autor: brunocampos01	
# Script: ./Elasticsearch_install.sh
# https://www.linode.com/docs/databases/elasticsearch/visualize-apache-web-server-logs-using-elastic-stack-on-debian-8/
# ----------------------------------- #
# Elasticsearch in linux mint and ubuntu
# input: N/A
# output: running Elasticsearch
# ----------------------------------- #

echo -e "Update packages \n"
sudo apt-get update && apt-get upgrade
echo
echo -e "======================================== \n"

echo -e "Install the KEYs official Elastic  \n"
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo
echo -e "======================================== \n"

echo -e "Install the apt-transport-https package \n"
sudo apt-get install apt-transport-https
echo
echo -e "======================================== \n"

echo -e "Add the APT repository information to your server’s list of sources: \n"
echo "deb https://artifacts.elastic.co/packages/6.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic.list
echo
echo -e "======================================== \n"

echo -e "Update packages \n"
sudo apt-get update
echo
echo -e "======================================== \n"

echo -e "Install Elasticsearch \n"
sudo apt-get install -y elasticsearch
echo
echo -e "======================================== \n"

echo -e "Install the elasticsearch package: \n"
sudo apt-get update
echo
echo -e "======================================== \n"

echo -e "Enable and start the elasticsearch service: \n"
sudo systemctl enable elasticsearch
sudo systemctl start elasticsearch | sleep 5
echo
echo -e "======================================== \n"

echo -e "Test: \n"
curl localhost:9200
echo
