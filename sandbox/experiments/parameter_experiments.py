import datetime
import yake
import json
import pandas as pd
import datetime
from db_core.database import Database
import os
import uuid
import numpy as np
import sandbox.custom_stopwords
import itertools
from collections import Counter
import math

db = Database()

# set yake parameters
language = "en"
max_ngram_size = 2
deduplication_threshold = 0.1
deduplication_function = 'seqm'
windows_size = 3
num_of_keywords = 25
features = None

# add new stop words in LOWERCASE only
# stopwords only seem to be removed if in lowercase
podcast_custom_stop_words = ["actually", "only", "guys", "hold", "saying",
                             "fucking", "went", "guy", "cuz", "still",
                             "told", "sure", "cool", "yep", "put", "mary. hkh",
                             "need", "people", "podcast", "podcasts",
                             "blah", "blah blah", "y'all", "stuff",
                             "hello", "welcome", "thought", "hopefully"]

stop_words = sandbox.custom_stopwords.stopwords_starter_list + podcast_custom_stop_words

shows = ['2IYLKc8EVVkw4Ch3mNqrwG']

# loop through all the shows
for show in shows:
    print(show)

    show_keyword_lists = []
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
        print(keyword_list)
        show_keyword_lists.append(keyword_list)

    # flatten and get most common keywords among all episodes in a show
    flattened = list(itertools.chain(*show_keyword_lists))
    flattened = [word for word in flattened if word not in stop_words]

    flattened_count = Counter(flattened)
    flattened_count_all = flattened_count.most_common(5)
    show_keywords = dict(flattened_count_all)

    # put each show and its keywords into a dictionary
    show_keyword_dict["show_id"] = show
    show_keyword_dict["keywords_counts"] = show_keywords
    print(show_keyword_dict)

print("")
print("Complete!")


