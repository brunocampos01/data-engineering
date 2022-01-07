#!/bin/bash
curl --user publicuser:0AmgWMej https://serp.xpto.com.br/xxxxx >>~/datasets/contributors.xml
hadoop fs -rm -r ~/datasets/contributors.xml /user/hadoop/serp
hadoop fs -put ~/datasets/contributors.xml /user/hadoop/serp
pig -x ~/pig-scripts/contributors.pig
