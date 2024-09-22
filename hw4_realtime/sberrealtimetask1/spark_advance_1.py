from pyspark import SparkConf, SparkContext
import re
import math

try:
    sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("yarn")) # имя и yarn для запуска через YARN
except:
    pass

def parse(line):   # парсим статьи
    try:
        article_id, text = line.rstrip().lower().split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

stop_words = [word for word in sc.textFile("/data/wiki/stop_words_en-xpo6.txt").collect()] # парсиv стоп-слова
def stop(words):
    return list(filter(lambda x: x not in stop_words, words))

article = sc.textFile("hdfs:///data/wiki/en_articles_part/articles-part", 16).map(parse) # парсинг статей
article = article.map(stop).cache() # обрабатываем статьи, филтруем стоп-слова, записываем в RAM

word_count = article.map(lambda a: len(a)).sum() 
acticle_count = article.count()
pair_count = word_count - acticle_count # считаем количество целевых пар

word_appear = article.flatMap(lambda word_list: [(word, 1) for word in word_list]).reduceByKey(lambda a, accum: a + accum).cache() # считаем вхождения слов
word_probabilities = word_appear.map(lambda pair: (pair[0], pair[1] / word_count)).cache() # считаем ожидаемую вероятность вхождений


def bigrams(words: list): # оцениваем пары слов на соответствие условиям
    return ['_'.join([a,b]) for a,b in zip(words[:-1], words[1:])]

bigrams = article.map(bigrams).cache() # определяем пары
bigram_count = bigrams.map(lambda a: len(a)).cache().sum()

bigram_appears = bigrams.flatMap(lambda bigram_list: [(bigram, 1) for bigram in bigram_list])\
    .reduceByKey(lambda a,b: a + b).filter(lambda pair: pair[1] >= 500).cache() # фильтруем пары, которые встретились 500 и более раз
bigram_probabilities = bigram_appears.map(lambda pair: (pair[0], pair[1] / bigram_count)).cache() # считаем вероятность пар слов

word_probs = sc.broadcast(word_probabilities.collectAsMap())

def PMI_c(bigram_probs): # считаем PMI для каждой пары
    words = bigram_probs[0].split('_')
    l_word_prob = word_probs.value[words[0]]
    r_word_prob = word_probs.value[words[1]]

    PMI = math.log(bigram_probs[1] / (l_word_prob * r_word_prob))
    return (bigram_probs[0], PMI)

def NPMI_c(bigram_probs): # считаем нормализованный в log PMI
    PMI = PMI_c(bigram_probs)
    return (PMI[0], PMI[1] / (-math.log(bigram_probs[1])))

NPMI = bigram_probabilities.map(NPMI_c).sortBy(lambda pair: -pair[1]) # сортируем по убыванию NPMI

for pair in NPMI.take(39):
    print(pair[0])
