drop table if exists datalake.show_id_info;
drop table if exists datalake.show_info;

create table datalake.show_id_info as
select
distinct(show_id)
from warehouse.podcast_metadata;

create table datalake.show_info as
select
a.show_id,
b.show_name,
b.show_description,
b.publisher,
CONCAT('https://open.spotify.com/show/', b.show_id) AS URL,
avg(b.duration) as average_episode_length,
count(b.episode_id) as number_of_episodes
from datalake.show_id_info as a
left join  warehouse.podcast_metadata as b on a.show_id=b.show_id
group by 1,2,3,4,5