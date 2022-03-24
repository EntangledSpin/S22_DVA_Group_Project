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
podcast_custom_stop_words = []

stop_words = custom_stopwords.stopwords_starter_list + podcast_custom_stop_words

# pulls in unique show ids having at least 10 episodes
# but fewer than 100
shows = db.execute_sql('''
    select show_uri_id
    from datalake.sorted_shows;
    ''', return_list=True)

append_to_df = []

def keyword_extraction(show):

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

    # grab all the transcripts for the current show
    episodes = db.execute_sql('''
            select lower(transcript)
            from datalake.raw_podcast_transcripts
            WHERE show_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=show), return_list=True)

    # loop through each episode for the current show
    for id in episodes:
        keyword_list = []

        # get the keywords for the episode
        keywords = custom_kw_extractor.extract_keywords(id)

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
    show_keywords = json.dumps(show_keywords)

    # put each show and its keywords into a dictionary
    #show_keyword_dict["show_id"] = show
    #show_keyword_dict["keywords_counts"] = show_keywords

    #show_id = show_keyword_dict["show_id"]

    #keywords_counts = show_keyword_dict["keywords_counts"]

    #show_keywords = list(map(lambda x: json.dumps(x), show_keywords))

    append_to_df.append([show, show_keywords])

global_start_time = time.time()

shows = shows[0:250]

count = 0
for show in shows:
    print("show:", count)
    keyword_extraction(show)

    count += 1

df = pd.DataFrame(append_to_df, columns=['show_id', 'keyword_counts'])

df.to_sql('lala', index=False,
                  schema='datalake', con=db.engine, if_exists="append")

global_end_time = time.time()

print("\nElapsed time:", round((global_end_time - global_start_time) / 60, 2), "minutes")