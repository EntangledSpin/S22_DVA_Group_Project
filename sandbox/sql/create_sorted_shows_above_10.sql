create table datalake.sorted_shows as
select show_uri_id, count(*) as count
from datalake.raw_podcast_transcripts
group by show_uri_id
having count(*) >= 10
order by count, show_uri_id;