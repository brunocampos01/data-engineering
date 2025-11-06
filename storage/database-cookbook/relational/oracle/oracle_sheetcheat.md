## Start
First, connect DB with user
```sql
connect sys/Oradoc_db1 as sysdba
```
or

```sql
CONNECT sys AS sysdba;

# pwd: Oradoc_db1
```

Oracle mapped schema:user (equal `USE database;`)
```sql
ALTER SESSION SET current_schema = <other_user>;
```

### Show
- database
```sql
SELECT * FROM v$database;
```

- current database
```sql
SELECT * FROM global_name;
```

- users
```sql
SELECT username FROM dba_users;
```

- current user
```sql
SELECT user FROM dual;
```

- local services
```sql
SELECT * FROM global_name;
SELECT name FROM V$ACTIVE_SERVICES;

# service default: ORCLCDB.localdomain
```

- version
```sql
SELECT * FROM v$version;
```

- current containers
```sql
Show the current container;
```


### Administering  User
- create user
```sql
alter session set "_ORACLE_SCRIPT"=true;
CREATE USER airflow IDENTIFIED BY airflow;
```

- Grant Permissions
```sql
GRANT ALL PRIVILEGES TO airflow;

SELECT DISTINCT username
FROM dba_users
ORDER BY username;
```

### PDBs

- Enter in PDB
```sql
ALTER SESSION SET CONTAINER = PDB1;
```
