# Insert user Shib

- Alter cookbook in Recipe: platform-shib
    - Insert login and key user in files/passwd
    - alter version metadata
   - `berks install `<br/>
- `berks upload platform-shib`
- git push
- go yarn machine:
    - `sudo chef-client -o 'recipe[platform-shib]'`
- test login in shib
http://hive.platform.chaordicsystems.com/
- git push chaordic-cookbooks new user


# Shib running in container  (CONTAINER)
running in container , orquestration in marathon

1. Repository clone<br/>
`git clone SHIB`

2. First, insert user from file passwd

3. Construtuin image<br/>
`docker build .`

4. Create tab e rename img<br/>
`docker tag {generated image hash}  docker-registry.chaordicsystems.com:5000/platform-shib:{lastest-tag + 1}`
{generated image hash} , example: 
`Successfully built c39669738055`

5. Push to docker registry<br/>
`docker push docker-registry.chaordicsystems.com:5000/platform-shib:{lastest-tag + 1}`

6. Update marathon<br/>
http://marathon.platform.chaordicsystems.com:8080/ui/#/apps/%2Fplatform-shib/configuration

and change version (tag)

