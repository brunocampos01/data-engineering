# Python and Oracle Instant Client
Docker with Python and Oracle client installed, ready to connect and run statements against the database.

### Versions
- Python: 3
- Oracle: 12.2

### Usage
- Build
```bash
name_project=$(basename "$(pwd)")
echo $name_project

docker build \
            --no-cache \
            -t $name_project \
            .
```

- Run
```bash
docker run -it $name_project /bin/bash
```

- Test
```bash
docker run -d -it -p 1521:1521 --name oracle_db store/oracle/database-enterprise:12.2.0.1
```

### Documentation
- [Oracle InstantClient](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html)
