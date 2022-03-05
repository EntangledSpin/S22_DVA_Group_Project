/* query for a rock/metal podcast */

select episode_uri_id
from datalake.raw_podcast_transcripts t1
where t1.show_uri_id = '2YIx5mE1Qy9cVSU5Afjczz';