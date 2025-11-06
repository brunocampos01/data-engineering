### Create backup
```shell
cluster-network.json.BAK_`date +%d%b%Y%H%M%S`
```


## Add Superuser Privilegies

```bash
export EDITOR=/usr/bin/vim
sudo EDITOR=/usr/bin/vim visudo
```


---

entender melhor a parte de rede do linux:

- dnsmasq 

---

## Wiping the entire disk
```
dd if=/dev/zero of=/dev/sdX bs=1M #replace X with the target drive letter.
```

Wiping the Master boot record (MBR)Edit
```
dd if=/dev/zero of=/dev/hdX bs=446 count=1 #replace X with the target drive letter.
```


## Processing Files and Data

- For JSON, use [`jq`](http://stedolan.github.io/jq/). For interactive use, also see [`jid`](https://github.com/simeji/jid) and [`jiq`](https://github.com/fiatjaf/jiq).

- For YAML, use [`shyaml`](https://github.com/0k/shyaml).

- For Excel or CSV files, [csvkit](https://github.com/onyxfish/csvkit) provides `in2csv`, `csvcut`, `csvjoin`, `csvgrep`, etc.

- For Amazon S3, [`s3cmd`](https://github.com/s3tools/s3cmd) is convenient and [`s4cmd`](https://github.com/bloomreach/s4cmd) is faster. Amazon's [`aws`](https://github.com/aws/aws-cli) and the improved [`saws`](https://github.com/donnemartin/saws) are essential for other AWS-related tasks.

- Know about `sort` and `uniq`, including uniq's `-u` and `-d` options -- see one-liners below. See also `comm`.

- Know about `cut`, `paste`, and `join` to manipulate text files. Many people use `cut` but forget about `join`.

- Know about `wc` to count newlines (`-l`), characters (`-m`), words (`-w`) and bytes (`-c`).

- Know about `tee` to copy from stdin to a file and also to stdout, as in `ls -al | tee file.txt`.

- For more complex calculations, including grouping, reversing fields, and statistical calculations, consider [`datamash`](https://www.gnu.org/software/datamash/).

## Network

### Check DNS
```sh
host localhost    # return IP

# localhost has address 127.0.0.1
# localhost has IPv6 address ::1
```

### Check PORT
```bash
telnet 192.168.0.1 3306
```

### Copy file localhost to remote-machine
##### download: remote -> local
```sh
scp user@remote_host:remote_file local_file
```

#### upload: local -> remote
```sh
scp local_file user@remote_host:remote_file
```

##### Example

```sh
scp lojas_xxx.csv brunorozza@yarn.systems.com:/home/brunorozza
lojas_xxx.csv
```

#### Check acess
- DNS lookup utility
```bash
dig my-addr-aws.net

# return SUCCESS
#
# ; <<>> DiG 9.11.5-P1-1ubuntu2.4-Ubuntu <<>> my-addr-aws.net
# ;; global options: +cmd
# ;; Got answer:
# ;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 10448
# ;; flags: qr rd ra; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 1

# ;; OPT PSEUDOSECTION:
# ; EDNS: version: 0, flags:; udp: 65494
# ;; QUESTION SECTION:
# ;my-addr-aws.net.			IN	A

# ;; ANSWER SECTION:
# my-addr-aws.net.		43200	IN	A	150.162.2.10
...
```

---

## System information

### Check Linux Version
- Kernel
```bash
uname --all

# Linux avell 5.4.0-40-generic #44-Ubuntu SMP Tue Jun 23 00:01:04 UTC 2020 x86_64 x86_64 x86_64 GNU/Linux
```

- Operational System
```bash
cat /etc/*release

# DISTRIB_ID=Ubuntu
# DISTRIB_RELEASE=20.04
# DISTRIB_CODENAME=focal
# DISTRIB_DESCRIPTION="Ubuntu 20.04 LTS"
# NAME="Ubuntu"
# VERSION="20.04 LTS (Focal Fossa)"
# ID=ubuntu
# ID_LIKE=debian
# PRETTY_NAME="Ubuntu 20.04 LTS"
# VERSION_ID="20.04"
# HOME_URL="https://www.ubuntu.com/"
# SUPPORT_URL="https://help.ubuntu.com/"
# BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
# PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
# VERSION_CODENAME=focal
# UBUNTU_CODENAME=focal
```

### Generate Key
```bash
`< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-10};`
```
## Search comands:
`apropose <PARAMETRO>`

## view manual
`man <BINARIE>`<br/>

www.explainshell.com<br/>
explain commands, example:

explainshell.png

## Find
`find /etc -name <FILE>`

## dpkg problems
```
sudo dpkg --configure -a
sudo apt-get -f install
sudo apt-get -f remove

sudo apt-get autoclean
sudo apt-get autoremove
sudo apt-get update


sudo apt-get upgrade
```

## Glossary

https://linuxconfig.org/linux-commands

`pwd`  print the current working directory path


### File and directory
- `cat -n ` add line numbers to all lines
- `mkdir dir1` Creating a directory
- `touch file1` Creating files and reading the file content
- `cat -n todo-list.txt` display a content of any text file eith numenbers line.
- `file todo-list.txt` Obtain the file type
- `rm -r my_files` remove repository

#### grep and find
grep search in file<bvr/>
find search in folder and files

