# PostgreSQL 

## Installing
```bash
sudo apt update
sudo apt install postgresql \
                 postgresql-contrib
```

## Check Status Services
```
service postgresql status
```

## Creating user
By default, PostgreSQL creates a special user postgres that has all rights. So, connect in this user
```bash
sudo su postgres

# postgres@
```

It is recommended that you create another use
```bash
sudo -u postgres createuser <username>  WITH PASSWORD '<my_password>';
ALTER USER <my_user> WITH SUPERUSER;
ALTER USER <my_user> WITH CREATEDB;
ALTER USER <my_user> WITH REPLICATION;
```

## Connect
```
psql -U my_user
```

## List databases
```
\l

#                                  List of databases
#   Name    |  Owner   | Encoding |   Collate   |    Ctype    |   Access privileges   
#-----------+----------+----------+-------------+-------------+-----------------------
# postgres  | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 | 
# template0 | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 | =c/postgres          +
#           |          |          |             |             | postgres=CTc/postgres
# template1 | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 | =c/postgres          +
#           |          |          |             |             | postgres=CTc/postgres
# (3 rows)

```

## List Users
```
\du

---------------+------------------------------------------------------------+-----------
 brunocampos01 |                                                            | {}
 postgres      | Superuser, Create role, Create DB, Replication, Bypass RLS | {}
```
