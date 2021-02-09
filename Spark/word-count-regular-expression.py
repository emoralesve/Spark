from pyspark import SparkConf, SparkContext
import re

def normalizedWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/Users/eduardo/Desktop/SparkCourse/Spark/book.txt")
words = input.flatMap(normalizedWords)
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x + y)
w= wordCounts.map(lambda xy: (xy[1],xy[0])).sortByKey()
w = w.collect()
for word in w:
    print(word)
