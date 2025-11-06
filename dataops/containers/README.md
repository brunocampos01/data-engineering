#  :whale: DockerFiles and Docker-Composes  :whale:

```
                  ##         .
            ## ## ##        ==
         ## ## ## ## ##    ===
     /"""""""""""""""""\___/ ===
~~~ {~~ ~~~~ ~~~ ~~~~ ~~~ ~ /  ===- ~~~
     \______ o           __/
       \    \         __/
        \____\_______/
```

## Requirements
- Docker HUB account


## Containers
Containers provide a consistent, isolated execution environment for applications.

The application and all its dependencies is packaged into a "container" and then a standard runtime environment is used to execute the app. This allows the container to start up in just a few seconds, because there's no OS to boot and initialize. You only need the app to launch.


The portability of the container makes it easy for applications to be deployed in multiple environments, either on-premises or in the cloud, often with no changes to the application.


- Docker Hub – para armazenamento público de “Docker Images”;
- Docker Registry – para armazenar imagens em ambiente on-premises;
- Docker Cloud – um serviço gerenciado para criar e executar contêiner;

#### Uninstall old versions
```
sudo apt remove docker docker-engine docker.io
```

The content of */var/lib/docker*, including images, containers, volumes and networks are preserved.


```
sudo ls /var/lib/docker
```


### Docker Comands
- Run container:
```
docker run [OPTIONS] CONTAINER [CONTAINER...]

# docker run -it -p 8888:8888 image:version
```

- Stop container:
```
docker stop [OPTIONS] CONTAINER [CONTAINER...]
```

- Open terminal inside docker :
```
docker exec -it --user root <DOCKER_NAME> /bin/bash
```

- Principal docker comands:
```
docker images  # images
docker ps      # containers
docker volume  # volumes
docker network # network

nmap -n -p- localhost # show port in use
```

---
