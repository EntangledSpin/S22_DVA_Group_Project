from bs4 import BeautifulSoup

with open('graph_test_output.gexf', 'r') as f:
    data = f.read()

bs_data = BeautifulSoup(data,'xml')
nodes = bs_data.find_all('node')
coords = bs_data.find('node', {'id':'2TPvj8tyUhY2UHOzU9kyu4'}).find('viz:position')
x = coords.get('x')
y = coords.get('y')

print(x,',', y)
