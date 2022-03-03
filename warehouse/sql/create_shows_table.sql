drop table if exists warehouse.podcast_shows;

create table warehouse.podcast_shows
as select distinct show_uri_id
from datalake.raw_podcast_transcripts;