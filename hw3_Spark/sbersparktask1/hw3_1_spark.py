
import re
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from datetime import datetime as dt

DATASET = '/data/wiki/en_articles_part'

conf = SparkConf().setAppName('hw_3_1').setMaster('yarn')
sc.stop()
sc = SparkContext(conf=conf)

def searcher(text):
    
    words = re.findall(r"[\w*]+", re.sub('\d', ' ', text))
    out = []
    for i in range(len(words)):
        if i != len(words) - 1 and words[i] == 'narodnaya':
            out.append(('{}_{}'.format(words[i], words[i+1]), 1))
    return out

rdd = sc.textFile(DATASET) \
    .map(lambda x: x.strip().lower()) \
    .flatMap(searcher) \
    .reduceByKey(lambda a,b : a + b) \
    .sortBy(lambda a: a[0])

for item in rdd.take(10):
    print('{}\t{}'.format(item[0], str(item[1])))