
from pyspark.sql import *


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), numFriend=int(fields[3]))


spark = SparkSession.builder.master("local").appName("SQL").getOrCreate()

textFile = spark.sparkContext.textFile('./fakefriends.csv')
peoples = textFile.map(mapper)

schemaPeople = spark.createDataFrame(peoples).cache()
schemaPeople.createOrReplaceTempView("people")
teenAgers = spark.sql("select * from people WHERE age BETWEEN 13 AND 19")
for teen in teenAgers.collect():
    print(teen)
schemaPeople.groupBy("age").count().orderBy("age").show()
spark.stop()