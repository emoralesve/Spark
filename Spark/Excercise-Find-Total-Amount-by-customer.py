from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("Exercise.1")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    clientID = fields[0]
    amount = fields[2]
    return (int(clientID), float(amount))


input = sc.textFile("C:/Users/eduardo/Desktop/SparkCourse/Spark/customer-orders.csv")
words = input.map(parseLine)
words = words.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
final= words.collect()

for result in final:
    print(result)