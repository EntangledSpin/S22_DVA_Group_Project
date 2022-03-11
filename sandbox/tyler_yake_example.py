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
sql_folder = os.path.join(os.path.abspath("."),"sql")

language = "en"
max_ngram_size = 3
deduplication_threshold = .9
deduplication_function = 'variable'
windows_size = 2
num_of_keywords = 10
features = None

podcast_custom_stop_words = ["yeah", "he", "she", "his", "hers", "because",
                             "him", "see", "thing", "know", "is", "him",
                             "her", "can", "like", "okay", "ok", "going", "just",
                             "yes", "no", "now", "here", "say", "who",
                             "will", "well", "making", "think", "back", "far",
                             "maybe", "bit", "way", "come", "let", "gonna",
                             "want", "Hey", "guys", "crazy", "kind", "right",
                             "mean", "may", "new", "great", "New", "being"]

stop_words_lst = custom_stopwords.stopwords_starter_list + podcast_custom_stop_words

stop_words = stop_words_lst

experiment_id = uuid.uuid4()

experiment_name = "YAKE - dedupFunc"
experiment_parameter = 'dedupFunc'

parameters = {"lan":language,
              "n":max_ngram_size,
              "dedupFunc":deduplication_function,
              "dedupLim":deduplication_threshold,
              'top':num_of_keywords,
              "features":features,
              "stopwords":stop_words,
              "windowsSize":windows_size}

notes = """Iterating through deduplication algos ['leve','jaro','seqm']"""

ADD_EXPIREMENT = True

if ADD_EXPIREMENT:
    experiment = pd.DataFrame([{"date": datetime.date.today(),
                                "experiment_id":experiment_id,
                                "experiment_name":experiment_name,
                                'experimental_parameter':experiment_parameter,
                                'parameters':parameters,
                                'notes': notes}])

    experiment['parameters'] = list(map(lambda x: json.dumps(x), experiment['parameters']))

    experiment.to_sql('keyword_extraction_experiments', index=False,
                      schema='datalake', con=db.engine, if_exists="append")

UPLOAD = True

shows_query = os.path.join(sql_folder,'tyler_mult_shows_show_ids.sql')
ep_text_file_path = os.path.join(sql_folder,'episode_text.sql')

param_list = ['jeve','jaro','seqm']

# pulls in unique show ids
shows = db.execute_sql('''
    select distinct show_uri_id 
    from datalake.shows_with_5_episodes_or_more;
''', return_list=True)

shows = shows[:50]
list_of_shows_and_keywords = []
count = 1

print("")
print("Extracting keywords for all the shows...")
print("")

# loop through all the shows
for show in shows:
    print("show", count, "-", show)

    # for each show, give it an empty list for each dedupe param
    jeve_keyword_lists = []
    jaro_keyword_lists = []
    seqm_keyword_lists = []

    show_keyword_dict = dict()

    for i in param_list:
        deduplication_function = i

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

            keyword_dict = dict({'date':datetime.date.today(),
                                 'expirement_uuid':experiment_id,
                                 'episode_uri_id': id,
                                 'algorithm': "YAKE",
                                 'results': {"keywords":[]},
                                 'parameters': {'lan': language,
                                                'dedupLim': deduplication_threshold,
                                                'dedupFunc': deduplication_function,
                                                'n': max_ngram_size,
                                                'top': num_of_keywords,
                                                'features':features,
                                                'stopwords':stop_words,
                                                'windowsSize':windows_size
                                                }
                                 })

            sql_text = db.read_sql_path(ep_text_file_path)
            sql_text = sql_text.replace('REPLACEME_ID',id)
            text = db.execute_sql(sql=sql_text, return_list=True)[0]

            keywords = custom_kw_extractor.extract_keywords(text)

            for keyword in keywords:
                keyword_list.append(keyword[0])

            if i == "jeve":
                jeve_keyword_lists.append(keyword_list)
            elif i == "jaro":
                jaro_keyword_lists.append(keyword_list)
            else:
                seqm_keyword_lists.append(keyword_list)

            keyword_dict['results']['keywords'] = keyword_list

            keyword_df = pd.DataFrame([keyword_dict])

            keyword_df['parameters'] = list(map(lambda x: json.dumps(x), keyword_df['parameters']))
            keyword_df['results'] = list(map(lambda x: json.dumps(x), keyword_df['results']))

            if UPLOAD:
                keyword_df.to_sql('keyword_extraction_results', index=False,
                                  schema='datalake', con=db.engine, if_exists="append")

    # only keep keywords that are in all 3 param keyword lists
    all_3_param_keywords_lst = []
    for j in range(len(jeve_keyword_lists)):
        jeve_set_lst = set(jeve_keyword_lists[j])
        jaro_set_lst = set(jaro_keyword_lists[j])
        seqm_set_lst = set(seqm_keyword_lists[j])

        int1 = jeve_set_lst.intersection(jaro_set_lst)
        int2 = int1.intersection(seqm_set_lst)

        all_3_param_keywords_lst.append(list(int2))

    # only keep keywords that are in all 3 param keyword lists
    all_3_param_keywords_lst = []
    for j in range(len(jeve_keyword_lists)):
        jeve_set_lst = set(jeve_keyword_lists[j])
        jaro_set_lst = set(jaro_keyword_lists[j])
        seqm_set_lst = set(seqm_keyword_lists[j])

        int1 = jeve_set_lst.intersection(jaro_set_lst)
        int2 = int1.intersection(seqm_set_lst)

        all_3_param_keywords_lst.append(list(int2))

    flattened = list(itertools.chain(*all_3_param_keywords_lst))
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
    show_keyword_data = pd.DataFrame([{"show_and_keywords": show_and_keywords}])

    show_keyword_data['show_and_keywords'] = list(map(lambda x: json.dumps(x),
                                               show_keyword_data['show_and_keywords']))

    show_keyword_data.to_sql('sample_shows_and_keywords', index=False,
                      schema='datalake', con=db.engine, if_exists="append")

print("")
print("Complete!")


