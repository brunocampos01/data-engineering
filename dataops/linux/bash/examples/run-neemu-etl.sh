#!/bin/bash
#description    :Run neemu-etl
#author         :Diego Quintana (diegoquintana@neemu.com)
#date           :2016-03-31
#===============================================================================

PROJECT_ROOT_DIR="$(dirname `readlink -f $0`)/../"
ENV="dev"
SERVICE="search"
KPIS=""
GIVEN_START_END_DATES=1
YESTERDAY=$(date -d '1 day ago' +'%Y-%m-%d')
START_DATE=${YESTERDAY}
END_DATE=${YESTERDAY}
DATABASES_FILE="${PROJECT_ROOT_DIR}/config/databases.json"
SCRIPT_CREATE_STORE="${PROJECT_ROOT_DIR}/scripts/create_store_list.py"

usage() {
cat << EOF
    Usage: ${0} [-e <env>] [-s "<store1,store2>"] [-p <product] [-k "kpi1,kpi2"]
            [-f <start date> -l <end date>]
    Example: ${0} -e dev -s "mobly,oppa" -p search -k "searches" -f 2016-01-30 -l 2016-01-31

For a given store and product (service), retrieve all KPIs data from Cockpit
Log in MySQL, and fill Dashboard API. Check 'lib/api/service-kpis.json' for all
registered KPIs of a product and 'config/current-environment.json' for all
registered stores.

By default, 'dev' environment is set, all stores from config/dev.json are used,
'search' is the product and start and end dates are equal to yesterday.

  -e <environment>     The environment that neemu-etl will run: 'dev',
                       'staging', 'prod'. Default is 'dev'.
  -s "<store1,store2>" The store that neemu-etl should run. If no store is
                       given, so, by default, all stores will be retrieved from
                       the current environment configuration file in 'config'
                       directory.
  -p <product>         The product (service) whose data will be gotten. Default
                       is 'search'.
  -k "<kpi1,kpi2>"     KPIs separated by ',' that must be used. Default is all
                       available on 'lib/service-kpis.json'.
  -f <start date>      YYYY-MM-DD. Default is yesterday. Depends on -l.
  -l <end date>        YYYY-MM-DD. Default is yesterday. Depends on -f.
EOF
}

end_script() {
    if [ ! -z "${1}" ]; then
        printf "\nError: ${1}\n"
    fi
    printf "Exiting...\n"
    exit 1
}

while getopts e:s:p:k:f:l: OPT
do
    case "${OPT}" in
        e)
            ENV="${OPTARG}"
            ;;
        s)
            STORES_STR=("${OPTARG}")
            ;;
        p)
            SERVICE="${OPTARG}"
            ;;
        k)
            KPIS="${OPTARG}"
            ;;
        f)
            START_DATE="${OPTARG}"
            GIVEN_START_END_DATES=$(( ! ${GIVEN_START_END_DATES} ))
            ;;
        l)
            END_DATE="${OPTARG}"
            GIVEN_START_END_DATES=$(( ! ${GIVEN_START_END_DATES} ))
            ;;
        ?)
            usage
            exit 0
            ;;
   esac
done

# both dates must be used together if one is provided
if [ ${GIVEN_START_END_DATES} -ne 1 ]; then
    usage
    end_script "-f and -l must be used together"
fi

if [ -z ${STORES_STR} ]; then
    STORES_STR="all"
fi

echo "Creating store list to: $STORES_STR"

# script to remove stores that executes on new ETL
if [ -f ${DATABASES_FILE} ]; then
    STORES_STR=$(cat ${DATABASES_FILE} | python ${SCRIPT_CREATE_STORE} -s ${STORES_STR})

    if [ $? -gt 0 ]; then
        echo -e "stores list could not be retrieved from: ${DATABASES_FILE}\n"
        usage
        end_script
    fi
else
    CONFIG_FILE="${PROJECT_ROOT_DIR}/config/${ENV}.json"
    STORES_STR=$(cat ${CONFIG_FILE} | python ${SCRIPT_CREATE_STORE}\
        -i config\
        -s ${STORES_STR})

    if [ $? -gt 0 ]; then
        end_script "stores list could not be retrieved from: ${CONFIG_FILE}"
    fi
fi

if [ -z ${STORES_STR} ]; then
    end_script "Invalid store. Try new ETL."
fi

# 'store2,store1,...' -> 'store2\nstore1\n...' -> 'store1\nstore2\n' ->
# 'store1,store2,...'
STORES_STR=$(printf "${STORES_STR}" | sed -r 's/,/\n/g')
STORES_STR=$(printf "%s\n" "${STORES_STR}" | sort)
STORES_STR=$(printf "${STORES_STR}" | tr '\n' ',')

echo "-s ${STORES_STR}"

if [ "${ENV}" == "dev" ]; then
    NODE_ENV=${ENV} node ${PROJECT_ROOT_DIR}/main.js/
        --stores ${STORES_STR}\
        --service ${SERVICE}\
        --kpis "${KPIS}"\
        -s ${START_DATE}\
        -e ${END_DATE} | ${PROJECT_ROOT_DIR}/node_modules/bunyan/bin/bunyan
else
    NODE_ENV=${ENV} node ${PROJECT_ROOT_DIR}/main.js\
        --stores ${STORES_STR}\
        --service ${SERVICE}\
        --kpis "${KPIS}"\
        -s ${START_DATE}\
        -e ${END_DATE}
fi
