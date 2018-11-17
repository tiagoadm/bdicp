4 de junho de 2018
23:03

/*queries de criação de tabelas, importação de dados e tratamento */

//ratings - tabela extração e conversão timestamp

CREATE EXTERNAL TABLE ratings2 (
  userid INT, 
  itemid INT,
  rating FLOAT, 
  tstamp INT
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/ML-100k/u.data' OVERWRITE INTO TABLE ratings2;

	CREATE TABLE ratings as 
		SELECT userid ,itemid, rating, from_unixtime(tstamp)
		FROM ratings2;

		
/*Fazer bucketed table para id's de ratings */

CREATE TABLE ratings_bucketed 
( userid INT, 
  itemid INT,
  rating FLOAT, 
  tstamp INT) 
CLUSTERED BY (userid)
INTO 100 BUCKETS;

INSERT INTO ratings_bucketed SELECT * from ratings;



/*movies tabela extração*/

CREATE EXTERNAL TABLE movies (
  movieid INT, 
  title STRING, 
  release string, 
  videorelease string, 
  url string, 
  unk boolean, 
  g1 boolean,
  g2 boolean,
  g3 boolean,
  g4 boolean,
  g5 boolean,
  g6 boolean,
  g7 boolean,
  g8 boolean,
  g9 boolean,
  g10 boolean,
  g11 boolean,
  g12 boolean,
  g13 boolean,
  g14 boolean,
  g15 boolean,
  g16 boolean,
  g17 boolean,
  g18 boolean) 
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY "|"
  STORED AS TEXTFILE;
  
  LOAD DATA INPATH '/user/maria_dev/ML-100k/u.item' OVERWRITE INTO TABLE movies;
  
//criação de tabela movie_genres 

create table movie_genres as
	select movieid, m_key, m_val from
		(select movieid, map(1,g1,2,g2,3,g3,4,g4,5,g5,6,g6,7,g7,8,g8,9,g9,10,g10,11,g11,12,g12,13,g13,14,g14,15,g15,16,g16,17,g17,18,g18) as map1
		from movies) as t1
	lateral view explode(t1.map1) xyz as m_key, m_val
	where m_val=1;
	
  
/*Fazer bucketed table para id's de filmes */

CREATE TABLE movies_bucketed 
( movieid INT, 
  title STRING, 
  release string,  
  url string) 
CLUSTERED BY (movieid)
INTO 10 BUCKETS;

INSERT INTO movies_bucketed SELECT movieid, title, release, url from movies2;


  
//users -  tabela extração
  
CREATE EXTERNAL TABLE users (
  userid INT, 
  age INT,
  gender STRING, 
  occupation string,
  zipcode STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/ML-100k/u.user' OVERWRITE INTO TABLE users;


/*Fazer bucketed table para id's de ratings */

CREATE TABLE users_bucketed 
( userid INT, 
  age INT,
  gender STRING, 
  occupation string,
  zipcode STRING) 
CLUSTERED BY (userid)
INTO 10 BUCKETS;

INSERT INTO users_bucketed SELECT * from users;



//genres -  tabela extração

CREATE EXTERNAL TABLE genres (
  genre_name STRING, 
  genre_id int
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
lines terminated by '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/ML-100k/u.genre' OVERWRITE INTO TABLE genres;




	
/* Criação de tabela para filmes, # de pessoas a votar. classificação média */

create table movie_rating_summary as
	SELECT m.title, COUNT(r.itemid) as nr_ratings, AVG(r.RATING) as average
	from movies_bucketed as m
	join ratings_bucketed as r
	on r.itemid=m.movieid
	GROUP BY m.title;
	


	
/*	
	CREATE EXTERNAL TABLE users_to_delete (
  userid INT, 
  age INT,
  gender STRING, 
  occupation string
)
PARTITIONED BY (zipcode STRING)
CLUSTERED BY (userid) into 2 buckets 
STORED AS orc
tblproperties ("Transational"="true");

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE users_to_delete
  PARTITION (zipcode=00000)
  SELECT userid, age, gender, occupation
  FROM users WHERE zipcode=00000;
  
  */




