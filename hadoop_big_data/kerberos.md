# Kerberos
- PROD: sempre faça as coisas no node02
- DEV: sempre faça as coisas no node08

### Listar os tokens
```
klist
```

### Listar os usuários
```
kadmin.local
listprincs
```

### Criar usuário
- Usuário deve existir no O.S
```
kadmin.local 
addprinc bigdata@DEV.SEF.SC.GOV.BR
```
### Alterar senha de usuário
```
kadmin.local
change_password <principal>
```

### Acessar o HDFS com permissões de sudo
```
sudo -u hdfs -i
kinit # welcome1
```
A partir de agora é possível executar comandos do tipo: `chown`

