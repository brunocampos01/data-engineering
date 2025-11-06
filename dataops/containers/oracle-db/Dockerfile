# oracle linux 7
FROM store/oracle/database-enterprise:12.2.0.1

# Set environment variables
ENV LANG=C.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_COLLATE=C
ENV LC_CTYPE=en_US.UTF-8
ENV ORACLE_SID=${1:-ORCLCDB}
ENV ORACLE_PDB=${2:-ORCLPDB1}

USER root

# COPY, best pratices: https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
RUN mkdir /opt/oracle/
COPY ./ /opt/oracle/

# Installs
RUN yum -y update
RUN yum install -y yum-utils
