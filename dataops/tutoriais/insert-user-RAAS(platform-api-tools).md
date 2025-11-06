


### Clone repository
`git clone https://github.com/chaordic/platform-api-tools.git`

### Open bashrc in computer's user:
`code ~/.bashrc`
<br/>
```
export PLAT_USER='bruno.rozza'
export PLAT_PASSWORD='xxx'
```

### save bashrc
`source ~/.bashrc`

### Execute list-users
`./plat-auth-list-users`

### Change password
`./plat-auth-change-password bruno.rozza mirimdoce`

---

## platform-api-tools

### Execute list-users
`./plat-auth-list-users`

### Add 
`./plat-auth-add-or-update-user fabio.ribeiro KEY CHAORDIC_READ`

### Generate Key
In terminal: <br/>
`< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32};`
 
### Test list-users
`https://platform.chaordicsystems.com/raas/v2/clients/`<br/> or  <br/>`https://platform.chaordicsystems.com/raas/v2/clients/autoline/products/4528033`
