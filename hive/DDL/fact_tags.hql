set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:table_name=fact_tags;

CREATE TABLE IF NOT EXISTS ${table_name} (
        fk_userid INT,
        fk_movieid INT,
        fk_datekey INT,
        fk_timekey INT,
        a_tag string,
		a_timestamp string)
    COMMENT 'Tags fact table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name}
select userid, 
movieid, 
concat(year(from_unixtime(tmstmp)),lpad(month(from_unixtime(tmstmp)),2,'0'),lpad(day(from_unixtime(tmstmp)),2,'0')) datekey,
hour(from_unixtime(tmstmp))*100+minute(from_unixtime(tmstmp)) timekey,
tag,
from_unixtime(tmstmp) tmstmp
from tags;