FROM python:3

# Set environment variables
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
# default value
ARG ORACLE_VERSION=12.2.0.1.0 

# COPY, best pratices: https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
RUN mkdir /opt/oracle/
COPY ./ /opt/oracle/

# Installs
RUN apt update && apt-get -yq dist-upgrade
RUN apt install -y unzip \
                   vim \
                   wget \
                   libaio1 \
                   libaio-dev \
    && pip install --no-cache-dir -r /opt/oracle/requirements.txt

RUN chmod 755 /opt/oracle/install_instantclient.sh && ./opt/oracle/install_instantclient.sh

#Set env
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

ENV OCI_HOME=$ORACLE_HOME
ENV OCI_LIB_DIR=$ORACLE_HOME
ENV OCI_INCLUDE_DIR=$ORACLE_HOME/sdk/include

# Clean
RUN apt autoclean  \
    && rm -rf /var/lib/apt/lists/*
