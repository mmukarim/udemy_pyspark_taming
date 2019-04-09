from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

userID = 500


def loadMovies():
    movieNames = {}
    with open('./ml-100k/u.item', encoding='ISO-8859-1') as movie:
        for line in movie:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("SortMovies")
sc = SparkContext(conf=conf)

movieDict = loadMovies()

data = sc.textFile('./ml-100k/u.data')
ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()
rank = 1
numIterations = 5
model = ALS.train(ratings, rank, numIterations)
userRatings = ratings.filter(lambda l: l[0] == userID)

# for rating in userRatings.collect():
#     print(movieDict[int(rating[1])] + ": " + str(rating[2]))

recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(movieDict[int(recommendation[1])] +
          " score " + str(recommendation[2]))
