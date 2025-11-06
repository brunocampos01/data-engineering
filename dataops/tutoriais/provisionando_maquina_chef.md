# Subir uma nova máquina

```
- Knife = roles
- berks = recipes
- chef  = cockbook
```

1. Update platform-chef-repo 
- update metadata

2. Upload changes to chef-server
- `berks upload <repice>`

3. Run script to create new instance
- `cd platform-central/instances`
- `./run-ec2.rb`

- Options:
```
Ok platform-luigi, where?
1
1) us-east-1a
2) us-east-1b
3) us-east-1c
4) us-east-1d
5) us-east-1e
6) us-east-1f
7) quit

AZ?
3
```
- choose price = 0.5
- Attach: <br/>
```
* ruby_block[wait for device] action run[2019-03-29T20:23:32+00:00] INFO: Processing ruby_block[wait for device] action run (/var/chef/cache/cookbooks/filesystem/providers/default.rb line 88)
```

4. Attacht disk
- ECS->ELASTIC BLOCK STORE -> VOLUMES
- `create volume`
- Actions-> Attach Volume

5. Remove machine
- Terminate machine from AWS in EC2/instances
`knife node delete <name-machine>`<br/>
`knife client delete  <name-machine>`


## Update recipe

- Make changes and upload to chef server, after run:<br/>
`berks upload <NAME_RECIPE> --force`
<br/>
e.g:<br/>
```
berks upload platform-repos --force
```

- Vá na máquina provisionada e rode o chef novamente:<br/>
```
sudo chef-client -o "recipe[NAME_COOKBOOK::NAME_RECIPE]"
```

or

```
sudo chef-client -o "recipe[platform-repos::platform_neemu_etl]"
```

## Update cookbook

- Check cockbook's install in machine:<br/>
```
cd /etc/chef; \
cat first-boot.json	# output: {"run_list":"role[NAME_COOKBOOK]"}
```

- Copy cookbook e run:<br/>
`sudo chef-client <NAME_COOKBOOK>`


## Update role
` knife upload from file roles/<NAME_ROLES.rb>`


## Drop 
- Remove machine in chef server:<br/>
```
cd $HOME/platform-chef-repo/
knife node delete platform-luigi-08 --yes
knife client delete platform-luigi-08 --yes
```
or<br/>
`knife ec2 server delete i-0a8d49b68d37e8e9e --node-name platform-luigi-08 --purge`

- Apague a maquina no EC2 > instances
