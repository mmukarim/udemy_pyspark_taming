from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


lines = sc.textFile("./customer-orders.csv")
values = lines.map(parseLine)
spentAmount = values.reduceByKey(lambda x, y: (x+y))
results = spentAmount.collect()
for result in results:
    print(str(result[0]) + ': ' + str(result[1]))
