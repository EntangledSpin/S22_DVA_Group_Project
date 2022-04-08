# Keyword Extraction Driver Program
# see keyword_extraction_functions.py for the functions implemented in this program
from db_core.database import Database
import time
import keyword_extraction_functions as kef

global_start_time = time.time()

# connect to db
db = Database()

# pull back a corpus of words in the NLTK Wordnet library
# we will use this list to remove any keywords that appear as gibberish (ex: "dfjskdjdk")
word_list = kef.wordnet()

# bring in all show ids that have at least 10 episodes, but not more than 100
# we want to remove small outliers because they may not generate representative keyword lists
# the extremely large shows show our algorithm down massively. There are only a few dozen extremely large shows
shows = kef.import_shows(db)

shows = shows[:100]

# the shows and keywords lists will be appended to this list, and then loaded into the db
append_to_df = []

# perform the keyword extraction process on the shows
count, append_to_df, need_to_reprocess = kef.batch_keyword_extraction(shows, append_to_df, word_list, db)

# if there is a time-out issue, we will re-process these shows
# sometimes when processing hundreds of shows a handful will time-out
if len(need_to_reprocess) > 0:
    reprocessed_count, append_to_df, need_to_reprocess = kef.reprocess_shows(need_to_reprocess, append_to_df, word_list, db)
else:
    reprocessed_count = 0

# once all shows are processed, load them to the db to be used in the similarity matrix process
kef.load_data_to_db(append_to_df, db)

global_end_time = time.time()

print("\nTotal shows processed:", count + reprocessed_count)
print("Total elapsed time:", round((global_end_time - global_start_time) / 60, 2), "minutes")