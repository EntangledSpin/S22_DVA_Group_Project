

import os
import pandas as pd
from db_core.database import Database
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
path = os.path.abspath(".")
db = Database()
directory = os.path.join(path,'podcasts-transcripts')
THREADED = False


def get_file_count():

    count = 0
    for root, subdirectories, files in os.walk(directory):  # Mechanism to iterate through subdirectories

        for file in files:  # Iterate through files in subdirectories

            if not file[0] == '.':  # Ignore .DS (Mac generated hidden file)
                count += 1

    return count

def multi_thread_import():

    threads = []

    for root, subdirectories, files in os.walk(directory):  # Mechanism to iterate through subdirectories

        for file in files:  # Iterate through files in subdirectories

            if not file[0] == '.':  # Ignore .DS (Mac generated hidden file)

                transcript_path = os.path.join(root, file)  # Build file path for file

                if THREADED:
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        threads.append(executor.submit(import_word_tokens, transcript_path))

                else:
                    #import_word_tokens(transcript_path)
                    try:
                        import_transcripts(transcript_path)
                    except json.decoder.JSONDecodeError:
                        pass


def import_word_tokens(transcript_path):

    episode_id, show_id = get_ids(transcript_path)  # Extract ID's from filename

    json_text = extract_json(transcript_path)  # Load JSON to Python readable format

    words_df = get_words(json_text)  # Extract Words from JSON

    words_df['episode_uri_id'] = episode_id  # Add episode ID for each word
    words_df['show_uri_id'] = show_id  # Add show ID for each word

    # Insert using SQLAlchemy DB engine and Pandas 'to_sql'
    words_df.to_sql('raw_podcast_word_tokens', index=False,
                    schema='datalake', con=db.engine, if_exists="append")


def import_transcripts(transcript_path):

    episode_id, show_id = get_ids(transcript_path)  # Extract ID's from filename

    json_text = extract_json(transcript_path)  # Load JSON to Python readable format

    transcript_df = get_transcript(json_text)  # Extract Words from JSON

    transcript_df['episode_uri_id'] = episode_id  # Add episode ID for each word
    transcript_df['show_uri_id'] = show_id  # Add show ID for each word

    # Insert using SQLAlchemy DB engine and Pandas 'to_sql'
    transcript_df.to_sql('raw_podcast_transcripts', index=False,
                    schema='datalake', con=db.engine, if_exists="append")


def get_ids(transcript_path):
    episode_id = transcript_path.split('/')[-1].split('.')[0]

    show_id = os.path.dirname(transcript_path).split('/')[-1].split('_')[1]

    return episode_id, show_id


def extract_json(transcript_path):

    """ Individual Podcast JSON FILE IS OF FORM:
        {"results":[{"alternatives":[
                     {"transcript": "String of text here",
                       "confidence": 0-1,
                       "words": [{"start_time":00:00,"end_time":00:00,"word":'this'},
                                 {"start_time": 00: 00, "end_time": 00:00, "word": 'this'}
                                ]
                     },
                    {"transcript": "String of text here",
                     "confidence": 0-1,
                     "words": [{"start_time": 00: 00, "end_time": 00:00, "word": 'this'},
                               {"start_time": 00: 00, "end_time": 00:00, "word": 'this'}
                              ]
                    },
                    {"alternatives":[
                     {"transcript": "String of text here",
                       "confidence": 0-1,
                       "words": [{"start_time":00:00,"end_time":00:00,"word":'this'},
                                 {"start_time": 00: 00, "end_time": 00:00, "word": 'this'}
                                ]
                     },
                    {"transcript": "String of text here",
                     "confidence": 0 - 1,
                     "words": [{"start_time": 00: 00, "end_time": 00:00, "word": 'this'},
                               {"start_time": 00: 00, "end_time": 00:00, "word": 'this'}
                              ]
                    }
                              ]
        """

    json_file = open(transcript_path)
    return json.loads(json_file.read())


def get_words(json_text):

    words = json_text['results'][-1]['alternatives'][0]['words']
    words_df = pd.DataFrame(words)
    return words_df


def get_transcript(json_text):
    transcript_dict = {"transcript":""}

    results = json_text['results']

    for result in results:

        for alt in result['alternatives']:

            if 'transcript' in list(alt.keys()):

                transcript_dict['transcript'] = transcript_dict['transcript'] + alt['transcript']

    transcript_df = pd.DataFrame(transcript_dict,index=[0])

    return transcript_df



if __name__ == '__main__':
    #print(get_file_count())
    multi_thread_import()
