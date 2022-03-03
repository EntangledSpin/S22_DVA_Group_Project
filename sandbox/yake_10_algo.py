import datetime

import yake
import json
import pandas as pd
import datetime
from db_core.database import Database
import os
import uuid
import numpy as np

db = Database() #Database Object
sql_folder = os.path.join(os.path.abspath("."),"sql")


### Expiremental information for expirement table
### MUST FILL THIS OUT WITH CORRECT INFORMATION

###PARAMETERS - SET VARIABLE TO "VARIABLE"
language = "en"
max_ngram_size = 3
deduplication_threshold = .9
deduplication_function = 'variable'
windows_size = 1
num_of_keywords = 20
features = None
stop_words = None
#### Adding Expirement Information
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
              "windowsSize":windows_size
                   }
notes = """Iterating through deduplication algos ['leve','jaro','seqm']"""


###################Adding Experiment to table#################################
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


## Keyword Extraction Parameters####
UPLOAD = True

ep_ids_10_file_path = os.path.join(sql_folder,'episode_ids_limit_10_experiments.sql')
ep_text_file_path = os.path.join(sql_folder,'episode_text.sql')
ep_keywords_file_path = os.path.join(sql_folder,'episode_keywords.sql')

### change this list for correct param increments
param_list = ['jeve','jaro','seqm']

# print (ep_ids_file_path)

## Iterating through parameters for keyword extraction on 100 episodes per parameter value
for i in param_list:
    print(i)

    ## Custom YAKE parameters
    ## ensure i is assigned to expiremental parameter - with
    # defaults set to itself -  values are defined above in experiment parameters section

    language = language
    max_ngram_size = max_ngram_size
    deduplication_threshold = deduplication_threshold
    deduplication_function =  i
    windows_size = windows_size
    num_of_keywords = num_of_keywords
    features = features
    stop_words = stop_words

    ## Custom Yake extractor object


    custom_kw_extractor = yake.KeywordExtractor(lan=language,
                                                n=max_ngram_size,
                                                dedupLim=deduplication_threshold,
                                                dedupFunc=deduplication_function,
                                                top=num_of_keywords,
                                                features=features,
                                                stopwords=stop_words,
                                                windowsSize=windows_size,
                                                )






######################################


#### Iterating through all episode ids


    episodes = db.execute_sql(sql_path=ep_ids_10_file_path,return_list=True) #extract episode id's from db

    total_episodes = len(episodes)  # Completion tracker - total episodes
    count = 0  # Completion tracker - completed count

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





        text = db.execute_sql(sql=sql_text, return_list=True)[0]  # May need to extend the db class method for return_item = True


        keywords = custom_kw_extractor.extract_keywords(text)

        for keyword in keywords:

            keyword_list.append(keyword[0])

        print(keyword_list)

        keyword_dict['results']['keywords'] = keyword_list
        print(keyword_dict)

        keyword_df = pd.DataFrame([keyword_dict])

        keyword_df['parameters'] = list(map(lambda x: json.dumps(x), keyword_df['parameters']))
        keyword_df['results'] = list(map(lambda x: json.dumps(x), keyword_df['results']))

        if UPLOAD:
            keyword_df.to_sql('keyword_extraction_results', index=False,
                              schema='datalake', con=db.engine, if_exists="append")


        count +=1
        remaining = total_episodes - count
        print(remaining)