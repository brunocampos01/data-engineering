usage() {
    cat <<EOF
Create user in environment. Execute this script as root user and node where the kerberos service is running.
Usage:
    ${0} [-l <login>] [-p "<password>"] [-g "<principal_group>"]

Example:
    ${0} -l brunocampos01 -p welcome1 -g user_group

Args:
    -l "<login>"             Ex: brunocampos01, brunorozza01.
    -p "<password>"          default is welcome1.
    -g "<principal_group>"   options: admin_group, user_group, job_group and dev_group.
EOF
}

# Choose arguments
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

# Check arguments
if [ -z "$USER_LOGIN" ] || [ -z "$USER_PASSWD" ] || [ -z "$PRINCIPAL_GROUP" ]; then
    echo "Missing login=${USER_LOGIN} or password=${USER_PASSWD} or group=${PRINCIPAL_GROUP}" >&2
    usage
    exit 1
fi
