# **Cookbook: Bash Script**

## Check Root
```bash
check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo "This script must be run as root"
        exit 1
    fi
}

check_root
```
## Log
```bash
function log {
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "$timestamp: $1"
}

log "Converting airflow.properties to airflow.cfg ..."
sleep 1
log "Converted"

# 2022-01-04 23:12:19: Converting airflow.properties to airflow.cfg ...
# 2022-01-04 23:12:20: Converted
```

## Date
```bash
# yesterday
YESTERDAY=$(date +"%Y-%m-%d" -d "-1 day")
echo $YESTERDAY

# today
DAY=$(date +"%Y-%m-%d")
echo $DAY

# tomorrow
TOMORROW=$(date +"%Y-%m-%d" -d "day")
echo $TOMORROW
```

## Comments
- Single line
```bash
# comment
```

- Mutiple lines
```bash
<<HEREDOC
    usage function
    write comments
    this not show
HEREDOC
```

## Debug
- `set -x` : print everything
- `set -e` : print only error
- `set -u` : print


## Variables
- Every invoke variables witn `${}`
- `#` : arguments number
- `@` : arguments
- `?` : Exit status of last task
- `$` : PID

## Conditions
- `[[ NUM -eq NUM ]]`:	Equal
- `[[ NUM -ne NUM ]]`:	Not equal
- `[[ NUM -lt NUM ]]`:	Less than
- `[[ NUM -gt NUM ]]`:	Greater than
- `[[ ! EXPR ]]`:	Not
- Ex:
`while [ ${ITER} -lt $(expr $PARAMETERS_SIZE + 1) ]; do ...`

## String variables
- `[[-z ${String}]]`: zero value
- `[[-n ${String}]]`: not empty value
- Ex:
  ```bash
  # String
  if [[ -z "$string" ]]; then
    echo "String is empty"
  elif [[ -n "$string" ]]; then
    echo "String is not empty"
  fi
  ```

## Conditional
```bash
git commit && git push
git commit || echo "Commit failed"
```

## Path
```bash
PROJECT_DIR="$(dirname `readlink -f $0`)"
```

### Check Errors When Script End
```bash
check_errors() {
    if [ $? != 0 ];
    then
      echo "Error running import cockpit for $STORE_TO_PROCESS" >> $LOG_FILE
      echo "Error $? running import cockpit for $STORE_TO_PROCESS"
      exit 1
    fi
}

# OR
end_script() {
    if [ ! -z "${1}" ]; then
        printf "\nError: ${1}\n"
    fi
    printf "Exiting...\n"
    exit 1
}
```

## Test If File Exists
```bash
if [ ! -f "$@" ]; then
    echo "File "$@" doesn't exist"
    exit
fi
```

## Histograms
```bash
$ < data/tips.csv Rio -ge 'g+geom_histogram(aes(bill))' | display

# or
data/tips.csv csvcut -c bill | feedgnuplot --terminal 'dumb 80,25' \
--histogram 0 --with boxes --ymin 0 --binwidth 1.5 --unset grid --exit
```

## Print Lines
```bash
echo $1
while read line; do
	echo "$line"
done
```

---

## References
- [howtoubuntu](https://howtoubuntu.org/how-to-execute-a-run-or-bin-file-in-ubuntu)
- [devhints](https://devhints.io/bash)

