import json
from db_core.database import Database
from sandbox.DBpedia_labeler import annotator
import os
import ast

db = Database()
sql_folder = os.path.join(os.path.abspath("."),"sql")
writer_path = os.path.join(sql_folder, 'DBpedia_add.sql')
writer_query = db.read_sql_path(writer_path)

shows = db.execute_sql(sql='SELECT show_id FROM datalake.sample_shows_and_keywords', return_list=True)

for i, show in enumerate(shows):
    keyword_list = db.execute_sql(sql = '''SELECT keywords FROM datalake.sample_shows_and_keywords 
                                       WHERE show_id = '{replace}' '''.format(replace=show), return_list=True)
    keywords = ast.literal_eval(keyword_list[0])
    WikiCat = annotator(keywords, confidence=0.6, split_camel_case=True)
    Wiki_dict = {'Wiki_Categories': WikiCat}
    Wiki_json = json.dumps(Wiki_dict)
    Wiki_json = Wiki_json.replace("'", "''")
    print(i)
    writer = writer_query.replace("REPLACE1", Wiki_json)
    writer = writer.replace("REPLACE2", show)
    db.execute_sql(sql=writer, commit=True)

