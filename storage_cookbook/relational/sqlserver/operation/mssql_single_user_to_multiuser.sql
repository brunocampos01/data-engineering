USE master
ALTER DATABASE SSISDB
SET MULTI_USER;
GO

select 
    d.name, 
    d.dbid, 
    spid, 
    login_time, 
    nt_domain, 
    nt_username, 
    loginame
from sysprocesses p 
    inner join sysdatabases d 
        on p.dbid = d.dbid
where d.name = 'DBNAME'
GO

kill 95 --=> kill the number in spid field
GO

USE DBNAME
EXEC sp_dboption 'DBNAME', 'single user', 'FALSE'
GO
