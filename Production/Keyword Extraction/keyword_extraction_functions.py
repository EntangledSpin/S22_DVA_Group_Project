# Keyword Extraction Functions

# import all necessary libraries
import yake
import json
import pandas as pd
import word_lists as wl
import itertools
from collections import Counter

# set yake parameters
language = "en"
max_ngram_size = 2
deduplication_threshold = 0.7
deduplication_function = 'seqm'
windows_size = 3
num_of_keywords = 5
features = None
stop_words = wl.stop_words

# import_shows
#      arguments: db - a database connection
#      returns:   shows - a list of show_ids
#
def import_shows(db):
    shows = db.execute_sql('''
        select show_uri_id
        from datalake.toy_sorted_shows;
        ''', return_list=True)

    return shows

# keyword_extraction
#      arguments: show - a show_id
#                 final_lst - a list containing each show_id and keyword list pair
#                 db - a database connection
#                 word_list - the corpus of wordnet english words
#
#      returns: final_lst - a list with a new show_id and keyword list pair added to it
#
def keyword_extraction(show, final_lst, db, word_list):

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
            from datalake.toy_podcast_transcripts
            WHERE show_uri_id = '{REPLACEME_ID}';
        '''.format(REPLACEME_ID=show), return_list=True)

    # loop through each episode for the current show
    for id in episodes:
        keyword_list = []

        # get the keywords for the episode
        keywords = custom_kw_extractor.extract_keywords(id)

        # put all the keywords into a list
        for keyword in keywords:
            just_keyword = keyword[0]

            # remove keywords that are just the same word repeated (ex: "harry harry")
            just_keyword = just_keyword.split()
            if len(just_keyword) == 1:
                keyword_list.append(keyword[0])
            else:
                if len(set(just_keyword)) > 1:
                    keyword_list.append(keyword[0])

        # if a keyword is a subset of another keyword, remove it
        keyword_list2 = []
        for kw in keyword_list:
            if not any([kw in r for r in keyword_list if kw != r]):
                keyword_list2.append(kw)

        # add the episode's keywords to a list pertaining to the show
        seqm_keyword_lists.append(keyword_list2)

    # flatten and get most common keywords among all episodes in a show
    flattened = list(itertools.chain(*seqm_keyword_lists))

    # only keep words that are in the dictionary
    flattened = [word for word in flattened if word in word_list]

    # do another pass at stop word removal
    flattened = [word for word in flattened if word not in stop_words]

    # flatten the lists and count the number of occurences of each keyword
    flattened_count = Counter(flattened)
    flattened_count_all = flattened_count.most_common()

    # create a show, keyword list pairing and append to the output list
    show_keywords = dict(flattened_count_all)
    show_keywords = json.dumps(show_keywords)

    final_lst.append([show, show_keywords])

    return final_lst

# batch_keyword_extraction
#      arguments: show_list - a list of show ids
#                 append_to_df - the list of show, keyword list pairings
#                 word_list - the corpus of wordnet english words
#                 db - a database connection
#
#      returns: count - the count of successful show keyword extractions
#               append_to_df - the list of show, keyword list pairings
#               need_to_reprocess - a list of shows that need to be reprocessed because of an exception
#
def batch_keyword_extraction(show_list, append_to_df, word_list, db):
    need_to_reprocess = []
    count = 0
    for show in show_list:
        print("show:", count)
        try:
            append_to_df = keyword_extraction(show, append_to_df, db, word_list)
            count += 1
        except:
            need_to_reprocess.append(show)
            print("\nEXCEPTION OCCURED")
            print("show id: ", show)
            print("")

    return count, append_to_df, need_to_reprocess

# reprocess_shows
#      arguments: show_list - a list of show ids
#                 append_to_df - the list of show, keyword list pairings
#                 word_list - the corpus of wordnet english words
#                 db - a database connection
#
#      returns: count - the count of successful show keyword extractions
#               append_to_df - the list of show, keyword list pairings
#               need_to_reprocess - a list of shows that need to be reprocessed because of an exception
#
def reprocess_shows(show_list, append_to_df, word_list, db):
    print("\nNumber of shows to reprocess:", len(show_list))
    print("")

    reprocessed_count, append_to_df, need_to_reprocess = batch_keyword_extraction(show_list, append_to_df, word_list, db)
    return reprocessed_count, append_to_df, need_to_reprocess

# load_data_to_db
#      arguments: append_to_df -the list of show ids and keyword lists to be appended to the db
#                 db - a database connection
#      returns: None
#
def load_data_to_db(append_to_df, db):

    df = pd.DataFrame(append_to_df, columns=['show_id', 'keyword_counts'])

    df.to_sql('toy_shows_and_keywords', index=False,
              schema='datalake', con=db.engine, if_exists="append")



