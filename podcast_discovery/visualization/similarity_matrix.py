from itertools import combinations
import math
import pandas as pd
import ast
from podcast_discovery.db_core.database import Database
from podcast_discovery.config import schema,keyword_table_name,similarity_matrix_table_name


class SimilarityMatrix:

    def __init__(self):
        self.db = Database()
        self.keywords = []
        self.show_ids = []
        self.li = []
        self.li2 = []
        self.di = {}
        self.matrix_df = pd.DataFrame()

    def run(self):

        self.keywords = self.get_keywords()
        self.show_ids = self.get_shows_ids()
        self.build_matrix()
        self.build_matrix_df()
        self.upload_matrix_df()


    def build_matrix(self):
        for x,y in combinations(self.keywords, 2):
            self.li.append((self.counter_cosine_similarity(x, y)))

        for i, j in combinations(self.show_ids, 2):
            val = i, j
            self.li2.append(val)

        for key in self.li2:
            for value in self.li:
                self.di[key] = value
                self.li.remove(value)
                break

    def build_matrix_df(self):

        df = pd.DataFrame(self.di.items(), columns=['pairs', 'similarity'])
        df['show_id_1'], df['show_id_2'] = df.pairs.str
        df.drop(columns=["pairs"], inplace=True)

        self.matrix_df = df[['show_id_1', 'show_id_2', 'similarity']]

    def upload_matrix_df(self):
        self.matrix_df.to_sql(similarity_matrix_table_name, index=False,
                              schema=schema, con=self.db.engine,
                              if_exists="replace")


    def get_keywords(self):
        # get list of keywords_count dictionaries
        keywords = self.db.execute_sql(
            sql=f'SELECT {keyword_table_name}.keyword_counts FROM {schema}.{keyword_table_name}',
            return_list=True)

        keywords = [ast.literal_eval(i) for i in keywords]

        return keywords

    def get_shows_ids(self):
        # get list of show_ids
        return self.db.execute_sql(sql=f'SELECT {keyword_table_name}.show_id FROM {schema}.{keyword_table_name}', return_list=True)


    def counter_cosine_similarity(self,c1, c2):
        terms = set(c1).union(c2)
        dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
        magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
        magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
        if magA==0 or magB==0:
            return 0
        else:
            return dotprod / (magA * magB)




# #similarity matrix for every combination of show ids
# li=[]
# li2=[]
# di={}
# for x,y in combinations(final_show_keywords, 2):
#     def counter_cosine_similarity(c1, c2):
#         terms = set(c1).union(c2)
#         dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
#         magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
#         magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
#         if magA==0 or magB==0:
#             return 0
#         else:
#             return dotprod / (magA * magB)
#     li.append((counter_cosine_similarity(x, y)))
#
# for i,j in combinations(show_ids,2):
#     val=i,j
#     li2.append(val)
#
# for key in li2:
#     for value in li:
#         di[key] = value
#         li.remove(value)
#         break
#
# #build dataframe and split columns
# df2=pd.DataFrame(di.items(), columns=['pairs', 'similarity'])
# df2['show_id_1'], df2['show_id_2'] = df2.pairs.str
# df2.drop(columns =["pairs"], inplace = True)
# df3 = df2[['show_id_1', 'show_id_2','similarity']]
#
# #move to datalake
# df3.to_sql('all_similarity_matrix', index=False,schema='warehouse', con=db.engine, if_exists="replace")
