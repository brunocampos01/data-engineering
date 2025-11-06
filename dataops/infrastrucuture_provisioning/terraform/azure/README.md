# Terraform at Azure

## ATTENTION 
Due to a lack of option on how to release the container service to connect to the bank automatically (No access to the OUTPUT ip by TF)
Execute the following sequence of commands whenever the terraform is executed

- Identify the Group ID: 
```bash
GROUPID=$(az sql server firewall-rule list --resource-group <PRODUCT_NAME>-<CLIENT_NAME>-rg --server <PRODUCT_NAME>-<CLIENT_NAME> | jq -r '.[] | select(.name=="access_aci<SUFIX>-tf") | .id')
```
Exchanging <SUFFIX > for customer id, e.g. tjam

- Invoke api container shell:
```bash
az container exec --resource-group <PRODUCT_NAME>-<CLIENT_NAME>-rg --name <PRODUCT_NAME>-<CLIENT_NAME> --container-name api --exec-command "/bin/sh"
```

- Execute the command sequence below:
```bash
apk update && apk add curl && curl ifconfig.io
```

- Write down the number of the ip

- Change the ip of the security group:
```
az sql server firewall-rule update --ids "${GROUPID}" --end-ip-address <IP> --start-ip-address <IP>
```
