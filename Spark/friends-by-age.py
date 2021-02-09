from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


#Spark can do special stuff with KEY/VALUE DATA
#reduceByKey():combine values with the same key using some funcion. rdd.reduceByKey(lambda x,y:x+y) adds themm up
#groupByKey():Group values with the same key
#sortByKey() Sort RDD by key Values
#keys(), values()- Create an RDD of just he keys, or just the values
#Join, righOuterJoin, leftOuterJoin, cogroup, subtractByKey
#We'll look at an exa
lines = sc.textFile("C:/Users/eduardo/Desktop/SparkCourse/Spark/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) #se agarra el valor
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = totalsByAge.collect()
for result in results:
    print(result)
