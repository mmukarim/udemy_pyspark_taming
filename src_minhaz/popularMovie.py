from pyspark import SparkConf, SparkContext


def loadMovies():
    movieNames = {}
    with open('./ml-100k/u.item', encoding='ISO-8859-1') as movie:
        for line in movie:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("SortMovies")
sc = SparkContext(conf=conf)

movieDict = sc.broadcast(loadMovies())

lines = sc.textFile('./ml-100k/u.data')
mapped = lines.map(lambda x: (int(x.split()[1]), 1))
counted = mapped.reduceByKey(lambda x, y: x+y)
flipped = counted.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey(ascending=False)
movieWithName = sortedMovies.map(lambda x: (movieDict.value[x[1]], x[0]))
results = movieWithName.collect()
for result in results:
    print(result)
