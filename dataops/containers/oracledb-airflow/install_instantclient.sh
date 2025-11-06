#!/usr/bin/env bash

cd /opt/oracle/
mkdir instantclient

ORACLE_VERSION=12.2.0.1.0
OCI_HOME=/opt/oracle/instantclient
INSTALLERS=oracle-instantclient

BASIC_CLIENT_ZIP=$INSTALLERS/instantclient-basic-linux.x64-${ORACLE_VERSION}.zip
JDBC_ZIP=$INSTALLERS/instantclient-jdbc-linux.x64-${ORACLE_VERSION}.zip
SQLPLUS_ZIP=$INSTALLERS/instantclient-sqlplus-linux.x64-${ORACLE_VERSION}.zip
SDK_ZIP=$INSTALLERS/instantclient-sdk-linux.x64-${ORACLE_VERSION}.zip
TOOLS_ZIP=$INSTALLERS/instantclient-tools-linux.x64-${ORACLE_VERSION}.zip

unzip $BASIC_CLIENT_ZIP -d $OCI_HOME
unzip $JDBC_ZIP         -d $OCI_HOME
unzip $SQLPLUS_ZIP      -d $OCI_HOME
unzip $SDK_ZIP          -d $OCI_HOME
unzip $TOOLS_ZIP        -d $OCI_HOME

# simbolik link
ln -s /opt/oracle/instantclient/libclntsh.so.12.2 /opt/oracle/instantclient/libclntsh.so
ln -s /opt/oracle/instantclient/libocci.so.12.2 /opt/oracle/instantclient/libocci.so
ldconfig
