import networkx as nx
from db_core.database import Database
import pandas as pd


def coordinates(table: str, similarity=0.0, layout='random'):

    """
    Function to write coordinates for graph nodes in similarity matrix.

    :param table: name of table to write coordinates to.
    :param similarity: degree of similarity required to make a node pair an edge.
    :param layout: NetworkX layout for graph. Option are 'spiral', 'spring', 'circular', 'random'. Any other value uses random layout.

    :return: None (writes table to Database).
    """

    db = Database()

    edges_dict = db.execute_sql(sql = '''
                                    SELECT show_id_1, show_id_2 
                                    FROM datalake.similarity_matrix 
                                    WHERE similarity > {sim}'''.format(sim=similarity), return_dict=True)
    edges = []
    for edge in edges_dict:
        edges.append((edge['show_id_1'], edge['show_id_2']))

    G = nx.from_edgelist(edges)

    if layout == 'spiral':
        pos = nx.spiral_layout(G, )
    elif layout == 'spring':
        pos = nx.spring_layout(G)
    elif layout == 'circular':
        pos = nx.circular_layout(G)
    else:
        pos = nx.random_layout(G)

    nodes =[]
    x = []
    y = []
    for show in pos.keys():
        nodes.append(show)
        x.append(pos[show][0])
        y.append(pos[show][1])
    data = {'show_id': nodes, 'x': x, 'y': y}
    pos_df = pd.DataFrame(data)
    pos_df.to_sql(table, index=False, schema='datalake', con=db.engine, if_exists='replace')


if __name__ == '__main__':
    coordinates('tableau_coordinates_netx', 0.2, 'circular')
