from bs4 import BeautifulSoup
from db_core.database import Database


class Gephi:
    def __init__(self, data):
        self.data = BeautifulSoup(data, 'xml')

    def parse(self, show_id):
        coords = self.data.find('node', {'id': show_id}).find('viz:position')
        x = coords.get('x')
        y = coords.get('y')
        return x, y

    def plot(self, shows:list):
        coord_dict = {'show_id': [], 'x': [], 'y': []}
        for show in shows:
            coord_dict['show_id'].append(str(show))
            xy = self.parse(show)
            coord_dict['x'].append(float(xy[0]))
            coord_dict['y'].append(float(xy[1]))
        return pd.DataFrame.from_dict(coord_dict)


if __name__ == '__main__':
    import pandas as pd
    db = Database()

    shows_list = db.execute_sql(sql_path='sql/nodes_export.sql', return_list=True)

    with open('coordinates_500.gexf', 'r') as f:
        graph_data = f.read()

    gp = Gephi(graph_data)
    show_table = gp.plot(shows_list)
    show_table.to_sql('tableau_coordinates', index=False, schema='datalake', con=db.engine, if_exists='replace')


