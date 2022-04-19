import os

from bs4 import BeautifulSoup
import pandas as pd
from podcast_discovery.db_core.database import Database
import json
import re
from concurrent.futures import ThreadPoolExecutor

db = Database()
sql_folder = os.path.join(os.path.abspath("."), 'sql')
print(sql_folder)
THREADED = False


class RSS:

    def __init__(self):
        self.df = pd.DataFrame()
        self.abs_path = os.path.abspath(".")
        self.directory = os.path.join(self.abs_path, 'data/rss_feeds')
        self.error_list = []
        self.schema = None
        self.table = None

    def multi_thread_import(self,schema,table):
        self.schema = schema
        self.table = table
        threads = []

        for root, subdirectories, files in os.walk(self.directory):  # Mechanism to iterate through subdirectories

            for file in files:  # Iterate through files in subdirectories

                if not file[0] == '.':  # Ignore .DS (Mac generated hidden file)

                    rss_path = os.path.join(root, file)  # Build file path for file

                    if THREADED:
                        with ThreadPoolExecutor(max_workers=10) as executor:
                            threads.append(executor.submit(self.import_rss, rss_path))

                    else:

                        self.import_rss(rss_path)

    def import_rss(self, rss_path):

        file = open(rss_path, "r")
        contents = file.read()

        show_id = self.get_show_id(rss_path)

        xml = BeautifulSoup(contents, 'xml')

        rss_dict = self.build_rss_dict(xml, show_id)

        df = pd.DataFrame([rss_dict])

        self.upload_rss_df(df)

    def build_rss_dict(self, xml, show_id):
        upload_dict = dict()
        upload_dict["category"], upload_dict["sub-category"] = self.get_xml_category(xml)
        upload_dict["link"] = self.get_xml_link(xml)
        upload_dict["image"] = self.get_xml_image(xml)
        upload_dict['authors'] = self.get_xml_authors(xml)
        upload_dict['show_id'] = show_id
        return upload_dict

    def get_xml_category(self, xml):
        regex = r'"([a-zA-Z&; ]+)"'
        category = str(xml.find("itunes:category"))
        match = re.findall(regex, category)

        if len(match) > 1:
            category = match[0].replace("&amp;", "and")
            subcategory = match[1].replace("&amp;", "and")

            return category, subcategory
        elif len(match) == 1:
            return match[0].replace("&amp;", "and"), "None"
        else:
            return "None", "None"

    def get_xml_image(self, xml):
        regex = r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'
        image = xml.find("itunes:image")
        image_url = re.findall(regex, str(image))[0].replace("\"/>", "")
        return str(image_url)

    def get_xml_authors(self, xml):

        authors = xml.find("itunes:author")
        if authors == None:
            authors = xml.find("<itunes:name>")
        try:
            authors = authors.get_text()
            return authors
        except:
            return "Not Available"

    def get_xml_link(self, xml):
        regex = r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'
        link = xml.find("link")
        try:
            url = link.get_text()
        except:
            url = re.search(regex, str(link))

        return str(url)

    def get_show_id(self, transcript_path):
        show_id = transcript_path.split('/')[-1].split('_')[1].split('.')[0]
        return show_id

    def upload_rss_df(self, df):

        df.to_sql(self.table, index=False,
                  schema=self.schema, con=db.engine, if_exists="append")




class Transcripts:

    def __init__(self):
        self.transcript_samples_path = os.path.join(os.path.abspath("."), 'data/transcripts')
        self.schema = None
        self.table = None

    def get_file_count(self):

        count = 0
        for root, subdirectories, files in os.walk(self.transcript_samples_path):  # Mechanism to iterate through subdirectories

            for file in files:  # Iterate through files in subdirectories

                if not file[0] == '.':  # Ignore .DS (Mac generated hidden file)
                    count += 1

        return count

    def multi_thread_import(self,schema,table):
        self.schema = schema
        self.table = table

        threads = []

        for root, subdirectories, files in os.walk(self.transcript_samples_path):  # Mechanism to iterate through subdirectories

            for file in files:  # Iterate through files in subdirectories

                if not file[0] == '.':  # Ignore .DS (Mac generated hidden file)

                    transcript_path = os.path.join(root, file)  # Build file path for file


                    try:

                        self.import_transcripts(transcript_path)
                    except json.decoder.JSONDecodeError:
                        pass

    def import_word_tokens(self,transcript_path):

        episode_id, show_id = self.get_ids(transcript_path)  # Extract ID's from filename

        json_text = self.extract_json(transcript_path)  # Load JSON to Python readable format

        words_df = self.get_words(json_text)  # Extract Words from JSON

        words_df['episode_uri_id'] = episode_id  # Add episode ID for each word
        words_df['show_uri_id'] = show_id  # Add show ID for each word

        # Insert using SQLAlchemy DB engine and Pandas 'to_sql'
        # words_df.to_sql('raw_podcast_word_tokens', index=False,
        #                 schema='datalake', con=db.engine, if_exists="append")

    def get_uploaded_eps(self):
        uploaded_eps_sql = os.path.join(sql_folder, 'get_uploaded_eps.sql')
        try:
            return db.execute_sql(sql_path=uploaded_eps_sql, return_list=True)
        except:
            return []

    def check_uploaded_eps(self,episode, uploaded_eps):

        return episode in uploaded_eps

    def import_transcripts(self,transcript_path):

        uploaded_episodes = self.get_uploaded_eps()

        episode_id, show_id = self.get_ids(transcript_path)  # Extract ID's from filename

        json_text = self.extract_json(transcript_path)  # Load JSON to Python readable format
        transcript_df = self.get_transcript(json_text)  # Extract Words from JSON

        transcript_df['episode_uri_id'] = episode_id  # Add episode ID for each word
        transcript_df['show_uri_id'] = show_id  # Add show ID for each word

        # Insert using SQLAlchemy DB engine and Pandas 'to_sql'

        if not self.check_uploaded_eps(episode_id, uploaded_episodes):

            transcript_df.to_sql('test_podcast_transcripts', index=False,
                                 schema='datalake', con=db.engine, if_exists="append")

    def get_ids(self,transcript_path):

        episode_id = transcript_path.split('/')[-1].split('.')[0]

        show_id = os.path.dirname(transcript_path).split('/')[-1].split('_')[1]

        return episode_id, show_id

    def extract_json(self,transcript_path):

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

    def get_words(self,json_text):

        words = json_text['results'][-1]['alternatives'][0]['words']
        words_df = pd.DataFrame(words)
        return words_df

    def get_transcript(self,json_text):
        transcript_dict = {"transcript": ""}

        results = json_text['results']

        for result in results:

            for alt in result['alternatives']:

                if 'transcript' in list(alt.keys()):
                    transcript_dict['transcript'] = transcript_dict['transcript'] + alt['transcript']

        transcript_df = pd.DataFrame(transcript_dict, index=[0])

        return transcript_df
