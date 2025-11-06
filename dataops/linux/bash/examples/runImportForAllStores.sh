#!/bin/bash
#description    :Drop, create, import HBASE to MYSQL (RDS)
#date           :2019-02-06
#===============================================================================

# current directory
cd $(dirname $(readlink -f ${0}))

HDFS_PATH="hdfs://hdfs.platform.linximpulse.net/user/platform/etl/conf/storeListCockpit"
STORE_TO_PROCESS=""
START_DATE=$(date +"%Y-%m-%d" -d "-1 day")
END_DATE=$(date +"%Y-%m-%d")
DDL=""
ITER=1
PARAMETERS_SIZE=${#}

usage() {
cat << EOF
    Usage: [-sd <start-date>] [-ed <end-date>] [OPTIONAL -store <store>] [OPTIONAL -ddl <ddl>]
    Example: ./runImportForAllStores.sh -sd 2019-02-02 -ed 2019-02-03 -store araujo -ddl drop
    -sd <start date>                YYYY-MM-DD
    -ed <end date>                  YYYY-MM-DD
    -store <store>                  <OPTIONAL> If no store is given, so, by default, all stores will run.
    -ddl <Data Definition Language> Its possible four arguments: <drop>, <create>, <dropAndCreate> and <importer>
                                    If no ddl is given, so, return argument error.
                                    By default, run <dropAndCreate> without store to run all stores.

    The script do:
    - Drop schema in mysql. Its possible drop only DB with parameter [OPTIONAL -store <store>] [-ddl <drop>]
    - Create schema in mysql. Its possible create only DB with parameter [OPTIONAL -store <store>] [-ddl <create>]
    - Drop and create all schemas <default>. Its possible drop and create only DB with parameter [OPTIONAL -store <store>] [OPTIONAL -ddl <dropAndCreate>]
    - Run only importer. If parameter [-ddl <importer>]
EOF
exit 1
}

verify_parameter(){
	local PARAM_TYPE=${1}
	local VALUE=${2}
	if [[ -z ${VALUE} ]] || [[ ${VALUE} == -* ]]; then
		echo "${PARAM_TYPE} does not have a valid value"
		exit 1
	fi
}

# check number of arguments passed
if (( ${#} < 6 || ${#} > 8 )); then
    echo "ERROR: You entered $# parameters"
    echo -e "You must enter minimum 6 command line arguments.\n"
    usage
fi

while [ ${ITER} -lt $(expr $PARAMETERS_SIZE + 1) ]; do
	let PARAM=ITER+1
	case ${!ITER} in
		-sd) PARAM_TYPE="-sd(start-date)"
			verify_parameter $PARAM_TYPE ${!PARAM}
			START_DATE="${!PARAM}";;
		-ed) PARAM_TYPE="-ed(end-date)"
			verify_parameter $PARAM_TYPE ${!PARAM}
			END_DATE="${!PARAM}";;
		-store) PARAM_TYPE="-store"
			verify_parameter $PARAM_TYPE ${!PARAM}
			STORE_TO_PROCESS="${!PARAM}";;
		-ddl) PARAM_TYPE="-ddl"
			verify_parameter $PARAM_TYPE ${!PARAM}
			DDL="${!PARAM}";;
		\?)
            usage ;;
	esac
	let ITER=ITER+1

done

runImport(){
    local STORE=$1
    local START_DATE=$2
    local END_DATE=$3
    local DDL=$4

    local JAR="pp-importhbasetomysql-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
    local MAIN_CLASS="com.neemu.Import.main.Import"

    search_provider="neemusearch"
    [[ ${STORE} = "wine" ]] && search_provider="wine"

    exec java -cp ${JAR} ${MAIN_CLASS} \
            -startDate ${START_DATE} \
            -endDate ${END_DATE} \
            -store ${STORE} \
            -ddl ${DDL} \
            -typeOutput mysql \
            -log acquisition \
            -searchProvider ${search_provider} \
            -username bigdata \
            -cockpit prod
}

export -f runImport

storeList=$STORE_TO_PROCESS
[[ -z "$STORE_TO_PROCESS" ]] && storeList=$(hadoop fs -cat ${HDFS_PATH})

echo $storeList | sed -e 's/ /\n/g' | xargs -P 6 -i bash -c "runImport {} ${START_DATE} ${END_DATE} ${DDL}"

