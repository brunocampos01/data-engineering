## Load Balancer
- Open in EC2/load balancer
- view which load balancer
- Ex: private-mesos-lb
- check config port
- edit inbound


## Change playbook ansible

`open platform-central`

- Ex security group
<br/>
`ansible/roles/security_groups/bigdata/platform-mesos.yml`
- Insert rule:<br/>
```
- proto: tcp
  from_port: 443
  to_port: 443
```

