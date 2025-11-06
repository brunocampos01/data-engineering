# Python and Oracle Instant Client
Docker with Oracle Database 12c pre-config.

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

- Volumes
<br/>
keep the data files of the database in a location outside the Docker container

```
mkdir oradata
chmod a+w oradata
```

- Run
```bash
docker run \
            -it \
            -d \
            -t $name_project \
            -p 1521:1521 -p 5500:5500 \
            -v /oradata:/opt/oracle/oradata \
            /bin/bash
```

### Documentation
- [Oracle developer Containers](https://developer.oracle.com/cloud-native/)
