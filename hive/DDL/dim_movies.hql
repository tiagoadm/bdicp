set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:table_name=dim_movies;

CREATE TABLE IF NOT EXISTS ${table_name} (
  pk_movieid int,
  a_title string,
  a_year int,
  a_imdbid string,
  a_tmdbid string
  )
    COMMENT 'Movies dimension table'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"" )
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");
  
INSERT OVERWRITE TABLE ${table_name}
select 
m.movieid, 
split(title,'\\(')[0] title,
cast(regexp_extract(title,'\\(([0-9]+?)\\)', 1) as int) year,
imdbid,
tmdbid
from movielens_external.movies m
inner join movielens_external.links l on m.movieid = l.movieid;



