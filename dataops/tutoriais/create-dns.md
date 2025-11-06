# Service route 53
Create route intern in DNS. Help transparent IP by users with uses.

- create in hosted zones intern `platform.linximpulse.net.`
- Next step click in `create record set`
- Test<br/>
`dig cockpit-db.platform.linximpulse.net.`
<br/>

```
; <<>> DiG 9.10.3-P4-Ubuntu <<>> cockpit-db.platform.linximpulse.net.
;; global options: +cmd
;; Got answer:
;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 42012
;; flags: qr rd ra; QUERY: 1, ANSWER: 2, AUTHORITY: 0, ADDITIONAL: 1

;; OPT PSEUDOSECTION:
; EDNS: version: 0, flags:; udp: 4096
;; QUESTION SECTION:
;cockpit-db.platform.linximpulse.net. IN	A

;; ANSWER SECTION:
cockpit-db.platform.linximpulse.net. 60	IN CNAME mysqldatabasename.cwrpsaeyjug8.us-east-1.rds.amazonaws.com.
mysqldatabasename.cwrpsaeyjug8.us-east-1.rds.amazonaws.com. 5 IN A 172.28.157.98

;; Query time: 173 msec
;; SERVER: 10.50.0.2#53(10.50.0.2)
;; WHEN: Fri Jan 18 15:56:12 -02 2019
;; MSG SIZE  rcvd: 152
```

