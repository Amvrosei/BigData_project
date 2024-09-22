from pyspark import SparkConf, SparkContext

try:
    sc = SparkContext(conf=SparkConf().setAppName("MyApp2").setMaster("yarn")) # имя и yarn для запуска через YARN
except:
    pass

def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  cur_v, prev_d, next_v = item[0], item[1][0], item[1][1]
  return (next_v, cur_v)

def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)

n = 400 # количество партиций
edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge).cache() # кэшируем в RAM
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist() # мэпим вершины, кэшируем с persist()

x = 12 # искомый граф начала
v = 34 # искомые граф финала
distances = sc.parallelize([(x, x)]).partitionBy(n) # создаем набор RDD

while True:
  vars = distances.join(forward_edges, n).map(step)
  new_distances = distances.fullOuterJoin(vars, n).map(complete, True).distinct().persist()
  count = new_distances.filter(lambda i: i[0] == v).count()
  if count == 0:
    distances = new_distances
  else:
    break

result = new_distances.collect()
dict_answer = {result[i][0]: result[i][1] for i in range(len(result))}

res=[]
vert = v
while vert!=12:
  res.append(vert)
  vert = dict_answer[vert]
res.append(vert)
for j in res[::-1]:
  if(j != v):
    print(j,end=',')
  else:
    print(j)
