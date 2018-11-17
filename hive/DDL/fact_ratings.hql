set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;

create database if not exists movielens;
use movielens;

set hivevar:table_name=fact_ratings;

drop table ${table_name};
CREATE TABLE IF NOT EXISTS ${table_name} (
        fk_userid INT,
        fk_movieid INT,
        fk_datekey INT,
        fk_timekey INT,
        m_rating decimal,
		a_timestamp string)
	COMMENT 'Ratings fact table'
	PARTITIONED BY (a_year int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
	STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name} partition (a_year)
select userid, 
movieid, 
concat(year(from_unixtime(tmstmp)),lpad(month(from_unixtime(tmstmp)),2,'0'),lpad(day(from_unixtime(tmstmp)),2,'0')) datekey,
hour(from_unixtime(tmstmp))*100+minute(from_unixtime(tmstmp)) timekey,
rating,
from_unixtime(tmstmp) tmstmp,
year(from_unixtime(tmstmp)) a_year
from movielens_external.ratings;