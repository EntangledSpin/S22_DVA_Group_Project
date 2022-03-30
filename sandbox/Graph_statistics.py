import networkx as nx
from networkx.algorithms import community
import pandas as pd
from db_core.database import Database

db = Database()

edges_dict = db.execute_sql(sql='''
                                   SELECT show_id_1 as source, show_id_2 as target, similarity as weight
                                   FROM datalake.similarity_matrix 
                                   WHERE similarity > {sim}'''.format(sim=0.5), return_dict=True)
edges = pd.DataFrame(edges_dict)

G = nx.from_pandas_edgelist(edges, edge_attr=True)

communities = community.louvain_communities(G)

degrees = nx.degree(G)

density = nx.density(G)

sim_score = nx.simrank_similarity(G)

print(sorted(sim_score['2a4H7yGK9qhnEvEEtutY3b'], key=sim_score['2a4H7yGK9qhnEvEEtutY3b'].get, reverse=True))
