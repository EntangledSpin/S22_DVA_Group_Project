import time
from collections import Counter
import math

list_1 = ['apple', 'pear', 'peach', 'blueberry', 'strawberry', 'potato', 'broccoli', 'squash', 'sprout', 'sweet potato','cauliflower']

list_2 = ['pineapple', 'pear', 'orange', 'blueberry', 'squash', 'apple', 'sprout']

t0 = time.time()
CounterA = Counter(list_1)
CounterB = Counter(list_2)

def counter_cosine_similarity(c1, c2):
    terms = set(c1).union(c2)
    dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
    magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
    magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
    return dotprod / (magA * magB)

i = 0
while i <1000:
    counter_cosine_similarity(CounterA, CounterB)
    i+=1
t1 = time.time()
print(t1-t0)
