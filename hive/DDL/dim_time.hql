set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:table_name=dim_time;

CREATE TABLE IF NOT EXISTS ${table_name} (
        pk_timekey int,
		a_hour int,
		a_minute int,
		a_hour_am int,
		a_am string)
    COMMENT 'Time dimension table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name}
select a.pos*100+b.pos, 
a.pos, 
b.pos, 
case when a.pos > 12 then a.pos-12 else a.pos end, 
case when a.pos > 12 then 'PM' else 'AM' end 
from
(select posexplode(split(repeat(",",23),","))) a,
(select posexplode(split(repeat(",",59),","))) b;