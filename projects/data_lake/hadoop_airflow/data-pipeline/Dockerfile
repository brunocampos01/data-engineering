FROM python:3.8.6-slim

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Set environment variables
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Configure Airflow: connect to backend
WORKDIR /root/airflow/
RUN mkdir /data_pipeline
COPY ./ ./data_pipeline
RUN cd /data_pipeline && ls -la

# Update and upgrade
RUN apt-get update \
    && apt-get -yq dist-upgrade 

# Install
RUN apt-get install -y --no-install-recommends \
    vim \
    curl \
    sudo \
    tini \
    gosu \
    rsync \
    netcat \
    locales \
    freetds-bin \
    build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    python3-dev \
    libsnappy-dev \
    libkrb5-3 \
    libkrb5-dev \
    libmariadb3 \
    libffi6 \
    libpq5 \
    sasl2-bin \
    libsasl2-dev \
    libsasl2-2 \
    libsasl2-modules \
    libssl1.1 \
    libffi-dev \
    libpq-dev \
    libssl-dev

RUN pip install --require-hashes --no-cache-dir -r requirements.txt

# clean
RUN apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base
