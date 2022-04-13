/* create a toy data set of all transcripts for 100 random shows */
/* that have at least 10 episodes each */

/* pick 100 random show that have at least 10 episodes */
create table datalake.toy_sorted_shows_temp as
select t1.*
from datalake.sorted_shows t1
order by random() limit 100;

create table datalake.toy_sorted_shows as
select t1.*
from datalake.toy_sorted_shows_temp t1
order by count;

drop table datalake.toy_sorted_shows_temp;

/* get all the transcripts for those 100 shows */
create table datalake.toy_podcast_transcripts as
select t1.*
from datalake.raw_podcast_transcripts t1
join datalake.toy_sorted_shows t2
  on t1.show_uri_id = t2.show_uri_id
order by t1.show_uri_id;