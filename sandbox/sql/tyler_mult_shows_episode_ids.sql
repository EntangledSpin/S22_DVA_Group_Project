select episode_uri_id
from datalake.raw_podcast_transcripts t1
WHERE show_uri_id in ('01sodVLCKYujK7e7XAyMLn', '00w2eaZ8XMge93AvYt7jX2')
order by show_uri_id;