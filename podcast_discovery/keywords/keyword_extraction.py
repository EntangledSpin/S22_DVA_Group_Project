# Keyword Extraction Driver Program
# see keyword_extraction_functions.py for the functions implemented in this program
# note: if this process gives you lots of exceptions, it will help to run small batches of shows
#       instead of running all of them at once
from db_core.database import Database
import time
from podcast_discovery.keywords import word_lists as wl
from podcast_discovery.keywords import keyword_extraction_functions as kef


class Keywords:
    def __init__(self):
        self.global_start_time = time.time()

        # connect to db
        self.db = Database()

        # pull back a corpus of words in the NLTK Wordnet library
        # we will use this list to remove any keywords that appear as gibberish (ex: "dfjskdjdk")
        self.word_list = wl.wordnet_corpus

        # bring in all show ids that have at least 10 episodes
        # we want to remove small outliers because they may not generate representative keyword lists
        self.shows = kef.get_shows(self.db)

    def run(self):

        # the shows and keywords lists will be appended to this list, and then loaded into the db
        append_to_df = []

        # perform the keyword extraction process on the shows
        count, append_to_df, need_to_reprocess = kef.batch_keyword_extraction(self.shows, append_to_df, self.word_list, self.db)

        # if there is a time-out issue, we will re-process these shows
        # sometimes when processing hundreds of shows a handful will time-out
        if len(need_to_reprocess) > 0:
            reprocessed_count, append_to_df, need_to_reprocess = kef.reprocess_shows(need_to_reprocess, append_to_df, self.word_list, self.db)
        else:
            reprocessed_count = 0

        # once all shows are processed, load them to the db to be used in the similarity matrix process
        kef.load_data_to_db(append_to_df, self.db)

        global_end_time = time.time()

        print("\nTotal shows processed:", count + reprocessed_count)
        print("Total elapsed time:", round((global_end_time - self.global_start_time) / 60, 2), "minutes")