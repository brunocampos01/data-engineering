# Deploys

## Confserver

`git clone https://github.com/chaordic/platform-confserver.git`

- Generate img<br/>
`sbt docker:publishLocal`

- Running imgs confserver(local) and dynamoDB<br/>
`docker-compose up -d`<BR/>
**NOTE:** Docker compose running multiples container

 - Check img:<br/>
 `docker images`

- View if image generate confserver is equals docker-compose.yml<br/>
`    image: docker-registry.chaordicsystems.com:5001/platform-confserver:latest`

- Open security group to cassandra<br/>
```
227874271258/sg-1cfd2068
vpn platform
```

- DynamoDB <br/>
`http://localhost:8000/shell/`
