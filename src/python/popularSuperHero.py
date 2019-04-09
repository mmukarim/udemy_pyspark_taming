from pyspark import SparkConf, SparkContext


def countCoOccurences(line):
    fields = line.split()
    return fields[0], (len(fields)-1)


def loadHeros(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1]


conf = SparkConf().setMaster("local").setAppName("PopularSuperHero")
sc = SparkContext(conf=conf)

heros = sc.textFile('./Marvel-Names.txt')
heroNames = heros.map(loadHeros)

lines = sc.textFile('./Marvel-Graph.txt')
coOccurences = lines.map(countCoOccurences)
total = coOccurences.reduceByKey(lambda x, y: x+y)
flipped = total.map(lambda x: (x[1], x[0]))
mostPopular = flipped.max()
mostPopularName = heroNames.lookup(int(mostPopular[1]))[0]
print(mostPopularName + ' with ' + str(mostPopular[0]) + ' co-appearances.')
