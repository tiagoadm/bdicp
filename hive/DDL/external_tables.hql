set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.execution.engine=tez;
create database if not exists movielens_external;
use movielens_external;

--drop table genome_scores;
--drop table genome_tags;
--drop table links;
--drop table movies;
--drop table tags;
--drop table ratings;

create external table if not exists genome_scores (
	movieId int,
 	tagId int,
  	relevance decimal
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/genome-scores'
    tblproperties ("skip.header.line.count"="1");
create external table if not exists genome_tags (
	tagId int,
  	tag string
)     ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/genome-tags'
    tblproperties ("skip.header.line.count"="1");
create external table if not exists links (
	movieId int,
  	imdbId string,
  	tmdbId int
)    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/links'
    tblproperties ("skip.header.line.count"="1");
create external table if not exists movies (
	movieId int,
  	title string,
  	genres string
)   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
	   "separatorChar" = ",",
	   "quoteChar"     = "\""
	)  
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/movies'
    tblproperties ("skip.header.line.count"="1");
create external table if not exists tags (
    userId int,
    movieId int,
    tag string,
    tmstmp bigint
  ) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/tags'
    tblproperties ("skip.header.line.count"="1");
create external table if not exists ratings (
    userId int,
    movieId int,
    rating decimal,
    tmstmp bigint
  ) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/user/maria_dev/movielens/ratings'
    tblproperties ("skip.header.line.count"="1");