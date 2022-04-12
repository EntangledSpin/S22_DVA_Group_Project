import networkx as nx
from networkx.algorithms import community
from db_core.database import Database
import pandas as pd



class Coordinates:

    def __init__(self):

        self.db = Database()
        self.graph = None
        self.pos = None
        self.communities = None
        self.pos_df = pd.DataFrame()

    def generate(self,table: str, similarity=0.0, layout='kamada',
                 source='datalake.sample_similarity_matrix', schema='datalake'):

        """
            Function to write coordinates for graph nodes in similarity matrix.

            :param table: name of table to write coordinates to.
            :param similarity: degree of similarity required to make a node pair an edge.
            :param layout: NetworkX layout for graph. Option are 'kamada', 'spiral', 'spring', 'spectral', 'circular', 'random'. Any other value uses random layout.
            :param source: source table for similarity matrix.
            :param schema: Schema to write table to.

            :return: None (writes table to Database).
            """
        print('building graph')
        self.build_graph(similarity,source)
        print('generating positions')
        self.generate_positions(layout)
        print('creating communities')
        self.create_communities()
        self.build_dataframe()
        self.upload_df(table,schema)

    def build_graph(self,similarity,source):
        # Read edges with similarity >= param similarity into a dictionary
        edges_dict = self.db.execute_sql(sql='''
                                        SELECT show_id_1 as source, show_id_2 as target, similarity as weight
                                        FROM {source}
                                        WHERE similarity >= {sim}'''.format(sim=similarity, source=source),
                                    return_dict=True)
        edges = pd.DataFrame(edges_dict)  # convert dictionary to pandas dataframe

        self.graph = nx.from_pandas_edgelist(edges, edge_attr=True)  # initialize a NetworkX Graph Object with the edges dataframe

    def generate_positions(self, layout):

        # Generate coordinates for every node based on the layout parameter
        if layout == 'spiral':
            self.pos = nx.spiral_layout(self.graph)
        elif layout == 'spring':
            self.pos = nx.spring_layout(self.graph, weight='weight')
        elif layout == 'circular':
            self.pos = nx.circular_layout(self.graph)
        elif layout == 'spectral':
            self.pos = nx.spectral_layout(self.graph, weight='weight')
        elif layout == 'kamada':
            self.pos = nx.kamada_kawai_layout(self.graph, weight='weight')
        else:
            self.pos = nx.random_layout(self.graph)

    def create_communities(self):
        # Calculate communities and assign each node to a community
        self.communities = community.louvain_communities(self.graph)

    def build_dataframe(self):

        # Generate final dataframe
        nodes = []
        x = []
        y = []
        coms = []
        for show in self.pos.keys():
            nodes.append(show)
            x.append(self.pos[show][0])
            y.append(self.pos[show][1])

            for com in self.communities:
                if show in com:
                    coms.append(self.communities.index(com))
                    break
        data = {'show_id': nodes, 'x': x, 'y': y, 'community': coms}
        print(data)
        self.pos_df = pd.DataFrame(data)

    def upload_df(self, table, schema):

        self.pos_df.to_sql(table, index=False, schema=schema,
                           con=self.db.engine, if_exists='replace')


