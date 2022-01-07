CREATE EXTERNAL TABLE raw.sharepoint_users
(
  id int,
  title string,
  email string,
  loginName string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/users';
