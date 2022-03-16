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

db = Database() #Database Object

language = "en"
max_ngram_size = 2
deduplication_threshold = 0.7
deduplication_function = 'variable'
windows_size = 3
num_of_keywords = 10
features = None

# add new stop words in LOWERCASE only
# stopwords only seem to be removed if in lowercase
podcast_custom_stop_words = ["actually", "only", "guys", "hold", "saying",
                             "fucking", "went", "guy", "cuz", "still",
                             "told", "sure", "cool", "yep", "put", "mary. hkh",
                             "need"]

stop_words = custom_stopwords.stopwords_starter_list + podcast_custom_stop_words

# pulls in unique show ids
shows = db.execute_sql('''
    select distinct show_uri_id 
    from datalake.shows_with_10_episodes_or_more;
''', return_list=True)

shows = shows[:100]
list_of_shows_and_keywords = []
count = 1

print("")
print("Extracting keywords for all the shows...")
print("")

# loop through all the shows
for show in shows:
    print("show", count, "-", show)

    seqm_keyword_lists = []
    deduplication_function = "seqm"

    show_keyword_dict = dict()

    custom_kw_extractor = yake.KeywordExtractor(lan=language,
                                                n=max_ngram_size,
                                                dedupLim=deduplication_threshold,
                                                dedupFunc=deduplication_function,
                                                top=num_of_keywords,
                                                features=features,
                                                stopwords=stop_words,
                                                windowsSize=windows_size,)

    episodes = db.execute_sql('''
            select episode_uri_id
            from datalake.raw_podcast_transcripts
            WHERE show_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=show), return_list=True)

    for id in episodes:
        keyword_list = []

        text = db.execute_sql('''
        select lower(transcript)
        from datalake.raw_podcast_transcripts 
        where episode_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=id), return_list=True)[0]

        keywords = custom_kw_extractor.extract_keywords(text)

        for keyword in keywords:
            keyword_list.append(keyword[0])

        seqm_keyword_lists.append(keyword_list)

    flattened = list(itertools.chain(*seqm_keyword_lists))
    counter_of_flat_list = Counter(flattened)

    show_most_common_words = counter_of_flat_list.most_common()

    len_most_common_words = len(show_most_common_words)
    twenty_percent_keys = math.floor(len_most_common_words * 0.2)

    final_lst = []
    for k in range(twenty_percent_keys):
        final_lst.append(show_most_common_words[k][0])

    show_keyword_dict["show_id"] = show
    show_keyword_dict["keywords"] = final_lst

    list_of_shows_and_keywords.append(show_keyword_dict)

    count += 1

print("")
print("Loading shows and keywords into database...")

# load shows and keywords into table
for show_and_keywords in list_of_shows_and_keywords:
    show_id = show_and_keywords["show_id"]

    show_name = db.execute_sql('''
            select distinct show_name
            from warehouse.podcast_metadata 
            where show_id = '{REPLACEME_ID}';
            '''.format(REPLACEME_ID=show_id), return_list=True)
    show_name = show_name[0]

    keywords = show_and_keywords["keywords"]

    show_keyword_data = pd.DataFrame([{"show_id":show_id,
                                       "show_name":show_name,
                                       "keywords": keywords}])

    show_keyword_data['keywords'] = list(map(lambda x: json.dumps(x),
                                               show_keyword_data['keywords']))

    show_keyword_data.to_sql('sample_shows_and_keywords', index=False,
                      schema='datalake', con=db.engine, if_exists="append")

print("")
print("Complete!")


