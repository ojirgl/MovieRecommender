
DROP TABLE IF EXISTS GENRES;
CREATE TABLE IF NOT EXISTS GENRES
(
id INT
,name VARCHAR(80)
,cached_slug VARCHAR(80)
,adult BOOL
,market_id SMALLINT
,erotic BOOL
)
;


DROP TABLE IF EXISTS MOVIES_GENRES;
CREATE TABLE IF NOT EXISTS MOVIES_GENRES
(
id INT
,genre_id INT
,movie_id BIGINT
)
;


DROP TABLE IF EXISTS MOVIES;
CREATE TABLE IF NOT EXISTS MOVIES
(
id BIGINT
,original_title VARCHAR(255)
,"year" INT
,duration DECIMAL
,plot VARCHAR(65335)
,rating_average DECIMAL
,market_id SMALLINT
)
;


DROP TABLE IF EXISTS ORDERS;
CREATE TABLE IF NOT EXISTS ORDERS
(
order_id INT
,user_id INT
,market_id SMALLINT
,movie_id BIGINT
,price DECIMAL
,created_at TIMESTAMP
)
;


DROP TABLE IF EXISTS USERS;
CREATE TABLE IF NOT EXISTS USERS
(
user_id INT
,market_id SMALLINT
)
;


DROP TABLE IF EXISTS STREAMS;
CREATE TABLE IF NOT EXISTS STREAMS
(
id VARCHAR(256)
,"type" VARCHAR(20)
,user_id INT
,order_id INT
,market_id SMALLINT
,content_id BIGINT
,viewtime DECIMAL
,content_duration DECIMAL
,created_at TIMESTAMP
)
;

DROP TABLE IF EXISTS PUBLISHING;
CREATE TABLE IF NOT EXISTS PUBLISHING
(
movie_id BIGINT
,published_at TIMESTAMP
)
;


COPY GENRES FROM 'D:\\tvapp\\genres.csv' WITH CSV HEADER;
COPY MOVIES_GENRES FROM 'D:\\tvapp\\movies_genres.csv' WITH CSV HEADER;
COPY MOVIES FROM 'D:\\tvapp\\movies.csv' WITH CSV HEADER;
COPY ORDERS FROM 'D:\\tvapp\\orders.csv' WITH CSV HEADER;
COPY STREAMS FROM 'D:\\tvapp\\streams.csv' WITH CSV HEADER;
COPY USERS FROM 'D:\\tvapp\\users.csv' WITH CSV HEADER;
COPY PUBLISHING FROM 'D:\\tvapp\\movies_published.csv' WITH CSV HEADER;


DROP VIEW v_content_clustering;
CREATE VIEW v_content_clustering
as (
select 
mm.id as movie_id
,mm.plot
,mm.rating_average
,mm.market_id
,case when gg.name isnull then '' else gg.name end as genre
,case when gg.id isnull then '0' else cast(gg.id as varchar(10)) end as genre_id
from movies mm
left join movies_genres mg on mg.movie_id = mm.id
left join genres gg on gg.id = mg.genre_id
)
;



DROP VIEW v_transactions;
CREATE VIEW v_transactions
as (
select 
max(ss.id) as "id"
,count(1) as times_seen
,case when oo.order_id notnull then 'order' else ss.type end as "type"
,ss.user_id
,ss.market_id
,ss.content_id as movie_id
,sum(ss.viewtime / ss.content_duration) as view_pct
,max(ss.created_at) as created_at
from streams ss
left join orders oo on oo.order_id = ss.order_id
--where case when oo.order_id notnull then 'order' else ss.type end = 'order' --duplicate check on orders
group by 3,4,5,6
order by ss.user_id, ss.content_id
)
;


CREATE VIEW V_MOVIES_PUBLISHED
AS
(
select 
pp.movie_id
,pp.published_at
,mm.market_id
from publishing pp
inner join movies mm on mm.id = pp.movie_id
)
;

