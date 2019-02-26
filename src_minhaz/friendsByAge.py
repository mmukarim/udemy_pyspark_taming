from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    return int(fields[2]), int(fields[3])


lines = sc.textFile("./fakefriends.csv")
values = lines.map(parseLine).mapValues(lambda x: (x, 1))
totalsByAge = values.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
avgByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])
results = avgByAge.collect()
for result in results:
    print(result)
