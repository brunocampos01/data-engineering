set hivevar:start_day=1970-01-01;
set hivevar:end_day=2080-12-31;

create table if not exists dim_date (id smallint, fulldate date, year smallint, month tinyint, day tinyint, quarter tinyint, daynumber_of_week tinyint, dayname_of_week string, daynumber_of_year smallint, week_of_month smallint, week_of_year smallint, weekend boolean, holiday boolean, primary key (id) disable novalidate) 
stored as avro;
with dates as (
	select date_add("${start_day}", a.pos) as actualdate
	from (select posexplode(split(repeat("o", datediff("${end_day}", "${start_day}")), "o"))) a
)
insert overwrite table dim_date select
  date_format(actualdate, 'yMMdd') as id,
  actualdate as fulldate,
  year(actualdate) as year,
  month(actualdate) as month,
  day(actualdate) as day,
  quarter(actualdate) as quarter,
  date_format(actualdate, 'u') as daynumber_of_week,
  date_format(actualdate, 'EEEE') as dayname_of_week,
  date_format(actualdate, 'D') as daynumber_of_year,
  date_format(actualdate, 'W') as week_of_month,
  date_format(actualdate, 'w') as week_of_year,
  if(date_format(actualdate, 'u') BETWEEN 6 AND 7, true, false) as weekend,
  date_format(actualdate, 'MM-dd') in ('01-01', '04-10', '04-21', '05-01', '06-11', '09-07', '10-12', '11-02', '11-15', '12-25') as holiday
from dates sort by id;