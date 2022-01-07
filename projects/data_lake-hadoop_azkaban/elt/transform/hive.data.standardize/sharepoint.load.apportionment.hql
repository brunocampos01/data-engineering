INSERT OVERWRITE TABLE
	staging.sharepoint_apportionment
SELECT
  data				    as idDate
  ,cc     			  as costCenterFrom
  ,'UNJ-C000066'	as costCenterTo
  ,tj_unjc000066  as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				    as idDate
  ,cc     			  as costCenterFrom
  ,'UNJ-C000063'	as costCenterTo
  ,pj_unjc000063  as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				    as idDate
  ,cc     			  as costCenterFrom
  ,'UNJ-C000061'	as costCenterTo
  ,mp_unjc000061  as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				        as idDate
  ,cc     			      as costCenterFrom
  ,'UNJ-C000126'	    as costCenterTo
  ,pgmbox_unjc000126  as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				          as idDate
  ,cc     			        as costCenterFrom
  ,'UNJ-D000034'	      as costCenterTo
  ,inovacao_unjd000034  as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				          as idDate
  ,cc     			        as costCenterFrom
  ,'UNJ-C000054'	      as costCenterTo
  ,adv_unjc000054       as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';

INSERT INTO TABLE
	staging.sharepoint_apportionment
SELECT
  data				          as idDate
  ,cc     			        as costCenterFrom
  ,'UNJ-D000036'	      as costCenterTo
  ,inter_unjd000036     as percentage
FROM raw.sharepoint_rateio as h
WHERE cc <> '';
