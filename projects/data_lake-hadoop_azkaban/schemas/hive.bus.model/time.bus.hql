create table if not exists dim_time (id int, fulltime string, hours tinyint, minutes tinyint, seconds tinyint, period char(2), primary key (id) disable novalidate) stored as avro;
with times as (
	select substr(from_unixtime(unix_timestamp('1970-01-01 00:00:00') + a.pos), 12) as actualtime
	from (select posexplode(split(repeat("o", 86399), "o"))) a
) insert overwrite table dim_time select replace(actualtime, ':', '') as id,
	actualtime as fulltime,
	split(actualtime, ':')[0] as hours, 
	split(actualtime, ':')[1] as minutes, 
	split(actualtime, ':')[2]as seconds,
	if(actualtime <= '12:00:00', 'AM', 'PM') as period
from times;