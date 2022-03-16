import json
from db_core.database import Database
from sandbox.DBpedia_labeler import annotator
import os

db = Database()
sql_folder = os.path.join(os.path.abspath("."),"sql")
writer_path = os.path.join(sql_folder, 'DBpedia_add.sql')
writer_query = db.read_sql_path(writer_path)

show_data = db.execute_sql(sql='SELECT show_and_keywords FROM datalake.sample_shows_and_keywords', return_list=True)

for i, episode in enumerate(show_data):
    keywords = json.loads(episode)['keywords']
    WikiCat = annotator(keywords, confidence=0.6)
    Wiki_dict = {'Wiki_Categories': WikiCat}
    Wiki_json = json.dumps(Wiki_dict)
    Wiki_json = Wiki_json.replace("'", "''")
    episode_clean = episode.replace("'", "''")
    print(i)
    writer = writer_query.replace("REPLACE1", Wiki_json)
    writer = writer.replace("REPLACE2", episode_clean)
    db.execute_sql(sql=writer, commit=True)
