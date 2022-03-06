/* query for an interesting podcast i found */

select episode_uri_id
from datalake.raw_podcast_transcripts t1
WHERE t1.show_uri_id = '0rP0GPTkYpenbzsyuoa6BK';