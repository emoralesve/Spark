from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAgeWithDataframes").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("C:/Users/eduardo/Desktop/SparkCourse/Spark/fakefriends-header.csv")


print("Here is our inferred schema:")
people.printSchema()

#From friendsByAge we group by "age" and then compute average
people.select("age","friends").groupBy("age").avg("friends").show()

#Sorted
people.select("age","friends").groupBy("age").avg("friends").sort("age").show()

#Formatted moroe nicely
people.select("age","friends").groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()


spark.stop()