drop table if exists sandbox.podcast_words_summary;


create table sandbox.podcast_words_summary  (
    word TEXT,
    start_time_secs DECIMAL,
    end_time_secs DECIMAL,
    show_uri_id TEXT,
    episode_uri_id TEXT

);

drop table if exists sandbox.podcast_transcript_summary;

create table sandbox.podcast_transcript_summary  (
    transcript TEXT,
    show_uri_id TEXT,
    episode_uri_id TEXT

);

