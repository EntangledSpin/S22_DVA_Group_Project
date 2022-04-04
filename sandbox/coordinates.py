import networkx as nx
from networkx.algorithms import community
from db_core.database import Database
import pandas as pd


def coordinates(table: str, similarity=0.0, layout='random', schema='datalake'):

    """
    Function to write coordinates for graph nodes in similarity matrix.

    :param table: name of table to write coordinates to.
    :param similarity: degree of similarity required to make a node pair an edge.
    :param layout: NetworkX layout for graph. Option are 'kamada', 'spiral', 'spring', 'spectral', 'circular', 'random'. Any other value uses random layout.
    :param schema: Schema to write table to.

    :return: None (writes table to Database).
    """

    db = Database()

    edges_dict = db.execute_sql(sql = '''
                                    SELECT show_id_1 as source, show_id_2 as target, similarity as weight
                                    FROM warehouse.all_similarity_matrix 
                                    WHERE similarity >= {sim}'''.format(sim=similarity), return_dict=True)
    edges = pd.DataFrame(edges_dict)

    G = nx.from_pandas_edgelist(edges, edge_attr=True)

    if layout == 'spiral':
        pos = nx.spiral_layout(G)
    elif layout == 'spring':
        pos = nx.spring_layout(G, weight='weight')
    elif layout == 'circular':
        pos = nx.circular_layout(G)
    elif layout == 'spectral':
        pos = nx.spectral_layout(G, weight='weight')
    elif layout == 'kamada':
        pos = nx.kamada_kawai_layout(G, weight='weight')
    else:
        pos = nx.random_layout(G)

    communities = community.louvain_communities(G)

    nodes = []
    x = []
    y = []
    coms = []
    for show in pos.keys():
        nodes.append(show)
        x.append(pos[show][0])
        y.append(pos[show][1])

        for com in communities:
            if show in com:
                coms.append(communities.index(com))
                break

    data = {'show_id': nodes, 'x': x, 'y': y, 'community': coms}
    pos_df = pd.DataFrame(data)
    pos_df.to_sql(table, index=False, schema=schema, con=db.engine, if_exists='replace')

    return None


if __name__ == '__main__':
    coordinates('tableau_coordinates', 0.75, 'kamada', schema='warehouse')
