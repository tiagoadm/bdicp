set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens;
use movielens;

set hivevar:table_name=dim_moviegenres;

CREATE TABLE IF NOT EXISTS ${table_name} (
  fk_movieid int,
  fk_genreid int,
  a_pos int
  )
    COMMENT 'Movie genres auxiliary dimension table'
	CLUSTERED BY (fk_genreid) SORTED BY (a_pos) INTO 20 BUCKETS
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

INSERT OVERWRITE TABLE ${table_name}
select movieid, pk_genreid, pos from (
select movieid, pos+1 as pos, val from (
select movieid, split(genres,'\\|') genre
from movielens_external.movies) a
LATERAL VIEW posexplode(genre) n1  as pos, val) b
inner join dim_genres on val = a_genre;