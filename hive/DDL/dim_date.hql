set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:start_date=1990-01-01;
set hivevar:days=10956;
set hivevar:table_name=dim_date;

CREATE TABLE IF NOT EXISTS ${table_name} (
  	pk_datekey int,
	a_date date,
    a_year int,
    a_quarter int,
    a_month int,
    a_week_of_month int,
    a_week_of_year int, 
    a_day int,
    a_day_of_week int,
    a_day_of_week_s string, 
    a_day_of_year int, 
    a_day_of_epoch int,
    a_weekend  boolean
)    COMMENT 'Date dimension table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name}
SELECT datekey,
    dat,
    year,
    cast(month(dat)/4 + 1 AS BIGINT) as quarter,
    month,
    date_format(dat, 'W') as week_of_month, 
    date_format(dat, 'w') as week_of_year, 
    day,
    day_of_week,
    date_format(dat, 'EEE') as day_of_week_s, 
    date_format(dat, 'D') as day_of_year, 
    datediff(dat, "1970-01-01") as day_of_epoch,
    if(day_of_week BETWEEN 6 AND 7, true, false) as weekend
FROM (
  SELECT
    concat(year(dat),lpad(month(dat),2,'0'),lpad(day(dat),2,'0')) datekey,
    dat,
    year(dat) as year,
    month(dat) as month,
    day(dat) as day,
    date_format(dat, 'u') as day_of_week
  FROM (
    SELECT date_add("${start_date}", a.pos) as dat
    FROM (SELECT posexplode(split(repeat(",", ${days}), ","))) a
	) b
) c 
SORT BY datekey;