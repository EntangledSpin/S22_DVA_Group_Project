import os
import pandas as pd
from db_core.database import Database
import json

path = os.path.abspath(".")
db = Database()
directory = os.path.join(path,'spotify_summary/transcripts')


def import_data():

    for root, subdirectories, files in os.walk(directory):
        for file in files:
            if not file[0] =='.':
                
                transcript_path = os.path.join(root, file)

                episode_id, show_id = get_ids(transcript_path)

                json_text = extract_json(transcript_path)

                text,words = split_words_text(json_text)

                text_dict = dict({'transcript': text})
                text_df = pd.DataFrame(text_dict,index=[0])
                text_df['episode_uri_id'] = episode_id
                text_df['show_uri_id'] = show_id
                word_df = pd.DataFrame(words)
                word_df['episode_uri_id'] = episode_id
                word_df['show_uri_id'] = show_id
                word_df.to_sql('podcast_words_summary',index=False,
                               schema = 'sandbox',con = db.engine,if_exists ="append")
                text_df.to_sql('podcast_transcript_summary',index=False,
                               schema = 'sandbox',con = db.engine,if_exists ="append")




def get_ids(transcript_path):

    episode_id = transcript_path.split('/')[-1].split('_')[1].split('.')[0]
    show_id = os.path.dirname(transcript_path).split('/')[-1].split('_')[1]

    return episode_id,show_id

def extract_json(transcript_path):
    json_file = open(transcript_path)
    return json.loads(json_file.read())

def split_words_text(json_text):
    text = json_text['transcript_text']
    words = json_text['words']
    return text,words



if __name__ == '__main__':
    import_data()
    print("test")