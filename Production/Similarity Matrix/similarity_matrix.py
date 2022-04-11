from itertools import combinations
import time
from collections import Counter
import math
import pandas as pd
import ast
from db_core.database import Database
db = Database()

#get list of keywords_count dictionaries
show_keywords = db.execute_sql(sql='SELECT all_shows_and_keywords.keyword_counts FROM warehouse.all_shows_and_keywords', return_list=True)
final_show_keywords = [ast.literal_eval(i) for i in show_keywords]

#get list of show_ids
show_ids = db.execute_sql(sql='SELECT all_shows_and_keywords.show_id FROM warehouse.all_shows_and_keywords', return_list=True)

#similarity matrix for every combination of show ids
li=[]
li2=[]
di={}
for x,y in combinations(final_show_keywords, 2):
    def counter_cosine_similarity(c1, c2):
        terms = set(c1).union(c2)
        dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
        magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
        magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
        if magA==0 or magB==0:
            return 0
        else:
            return dotprod / (magA * magB)
    li.append((counter_cosine_similarity(x, y)))

for i,j in combinations(show_ids,2):
    val=i,j
    li2.append(val)

for key in li2:
    for value in li:
        di[key] = value
        li.remove(value)
        break

#build dataframe and split columns
df2=pd.DataFrame(di.items(), columns=['pairs', 'similarity'])
df2['show_id_1'], df2['show_id_2'] = df2.pairs.str
df2.drop(columns =["pairs"], inplace = True)
df3 = df2[['show_id_1', 'show_id_2','similarity']]

#move to datalake
df3.to_sql('all_similarity_matrix', index=False,schema='datalake', con=db.engine, if_exists="replace")
