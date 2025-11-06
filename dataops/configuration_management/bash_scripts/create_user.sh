#!/bin/bash
# Description: Structure environment
# User: root
# Local: node01 (prod) or node08 (dev)
# Author: brunocampos01
# ----------------------------------- #
USER_LOGIN=''
USER_PASSWD=''
PRINCIPAL_GROUP=''

usage() {
    cat <<EOF
Create user in BDA. Execute this script as root user and node where the kerberos service is running.
Usage:
    ${0} [-l <login>] [-p "<password>"] [-g "<principal_group>"]

Example:
    ${0} -l brunocampos01 -p welcome1 -g user_group

Args:
    -l "<login>"             Ex: brunocampos01.
    -p "<password>"          default is welcome1.
    -g "<principal_group>"   options: admin_group, user_group, job_group and dev_group.
EOF
}

while getopts l:p:g: OPT; do
    case "${OPT}" in
    l)
        USER_LOGIN="${OPTARG}"
        ;;
    p)
        USER_PASSWD="${OPTARG}"
        ;;
    g)
        PRINCIPAL_GROUP="${OPTARG}"
        ;;
    ?)
        usage
        exit 1
        ;;
    esac
done

if [ -z "$USER_LOGIN" ] || [ -z "$USER_PASSWD" ] || [ -z "$PRINCIPAL_GROUP" ]; then
    echo "Missing login=${USER_LOGIN} or password=${USER_PASSWD} or group=${PRINCIPAL_GROUP}" >&2
    usage
    exit 1
fi

echo "----------------------------------------"
echo "----------- create user linux ----------"
echo "----------------------------------------"
# shellcheck disable=SC2128
dcli -C useradd -G $PRINCIPAL_GROUP -m $USER_LOGIN
dcli -C id $USER_LOGIN
dcli -C passwd -S $USER_LOGIN
hash=$(echo $USER_PASSWD | openssl passwd -1 -stdin)
dcli -C "usermod --pass='$hash' $USER_LOGIN"
dcli -C passwd -S $USER_LOGIN
dcli -C chmod 755 /home/$USER_LOGIN
dcli -C ls -ld /home/$USER_LOGIN

echo "----------------------------------------"
echo "----------- create user hdfs -----------"
echo "----------------------------------------"
passwd='welcome1'
echo $passwd | sudo -u hdfs -i
echo $passwd | kinit hdfs
hdfs dfs -mkdir /user/$USER_LOGIN
hdfs dfs -chown $USER_LOGIN:$USER_LOGIN /user/$USER_LOGIN
hdfs dfs -ls -d /user/$USER_LOGIN

echo "----------------------------------------"
echo "--------- create user kerberos ---------"
echo "----------------------------------------"
# shellcheck disable=SC2216
echo $USER_PASSWD | echo $USER_PASSWD | kadmin.local add_principal $USER_LOGIN
kadmin.local -q "addprinc -pw $USER_PASSWD $USER_LOGIN"

echo "----------------------------------------"
echo "------------ create user hue -----------"
echo "----------------------------------------"
GET_HUE_PROCESS="ls -alrt /var/run/cloudera-scm-agent/process | grep HUE | tail -1"
eval $GET_HUE_PROCESS
HUE_PROCESS=$(eval $GET_HUE_PROCESS | awk '{print $9}')
export HUE_CONF_DIR=/var/run/cloudera-scm-agent/process/$HUE_PROCESS
echo $HUE_CONF_DIR

# shellcheck disable=SC2164
cd /opt/cloudera/parcels/CDH/lib/hue/
build/env/bin/hue useradmin_sync_with_unix --cm-managed
