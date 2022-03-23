import datetime
import yake
import json
import pandas as pd
import datetime
from db_core.database import Database
import os
import uuid
import numpy as np
import custom_stopwords
import itertools
from collections import Counter
import math
from sandbox.DBpedia_labeler import annotator
import time
from concurrent.futures import ThreadPoolExecutor

db = Database()

# set yake parameters
language = "en"
max_ngram_size = 2
deduplication_threshold = 0.7
deduplication_function = 'seqm'
windows_size = 3
num_of_keywords = 5
features = None

# add new stop words in LOWERCASE only
# stopwords only seem to be removed if in lowercase
podcast_custom_stop_words = ["actually", "only", "guys", "hold", "saying",
                             "fucking", "went", "guy", "cuz", "still",
                             "told", "sure", "cool", "yep", "put", "mary. hkh",
                             "need", "people", "podcast", "podcasts",
                             "blah", "blah blah", "y'all", "stuff",
                             "hello", "welcome", "thought", "hopefully"]

stop_words = custom_stopwords.stopwords_starter_list + podcast_custom_stop_words

# pulls in unique show ids having at least 10 episodes
shows = db.execute_sql('''
    select distinct show_uri_id
    from datalake.raw_podcast_transcripts
    group by show_uri_id
    having count(*) >= 10;
''', return_list=True)

# run a sample of the shows
shows = shows[:50]

def keyword_extraction(show):
    print("show:", show)

    seqm_keyword_lists = []
    show_keyword_dict = dict()

    # set the keyword extractor with parameters
    custom_kw_extractor = yake.KeywordExtractor(lan=language,
                                                n=max_ngram_size,
                                                dedupLim=deduplication_threshold,
                                                dedupFunc=deduplication_function,
                                                top=num_of_keywords,
                                                features=features,
                                                stopwords=stop_words,
                                                windowsSize=windows_size,)

    # grab all the episodes for the current show
    episodes = db.execute_sql('''
            select episode_uri_id
            from datalake.raw_podcast_transcripts
            WHERE show_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=show), return_list=True)

    # loop through each episode for the current show
    for id in episodes:
        keyword_list = []

        # get the transcript for the current episode
        text = db.execute_sql('''
        select lower(transcript)
        from datalake.raw_podcast_transcripts 
        where episode_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=id), return_list=True)[0]

        # get the keywords for the episode
        keywords = custom_kw_extractor.extract_keywords(text)

        # put all the keywords into a list
        for keyword in keywords:
            keyword_list.append(keyword[0])

        # add the episode's keywords to a list pertaining to the show
        seqm_keyword_lists.append(keyword_list)

    # flatten and get most common keywords among all episodes in a show
    flattened = list(itertools.chain(*seqm_keyword_lists))
    flattened = [word for word in flattened if word not in stop_words]

    flattened_count = Counter(flattened)
    flattened_count_all = flattened_count.most_common()
    show_keywords = dict(flattened_count_all)

    # put each show and its keywords into a dictionary
    show_keyword_dict["show_id"] = show
    show_keyword_dict["keywords_counts"] = show_keywords

    show_id = show_keyword_dict["show_id"]

    keywords_counts = show_keyword_dict["keywords_counts"]

    show_keyword_data = pd.DataFrame([{"show_id":show_id,
                                       "keywords_counts":keywords_counts}])

    show_keyword_data['keywords_counts'] = list(map(lambda x: json.dumps(x),
                                               show_keyword_data['keywords_counts']))
    show_keyword_data.to_sql('dummy2', index=False,
                      schema='datalake', con=db.engine, if_exists="append")

for show in shows:
    keyword_extraction(show)
