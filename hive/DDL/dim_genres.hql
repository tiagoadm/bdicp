set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:table_name=dim_genres;

CREATE TABLE IF NOT EXISTS ${table_name} (
  pk_genreid int,
  a_genre string
  )
      COMMENT 'Genres dimension table'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name}
select row_number() over ()-1, val from
(select posexplode(split(genres,'\\|'))
from movielens_external.movies) a
group by val;