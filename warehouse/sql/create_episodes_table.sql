drop table if exists warehouse.podcast_episodes;
create table warehouse.podcast_episodes
as select distinct episode_uri_id,
                   show_uri_id

from datalake.raw_podcast_transcripts;