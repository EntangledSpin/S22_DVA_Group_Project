create table datalake.show_info as
select
show_id,
show_description,
publisher,
sum(duration) as total_episode_length,
count(episode_id) as number_of_episodes
from warehouse.podcast_metadata
group by 1,2,3