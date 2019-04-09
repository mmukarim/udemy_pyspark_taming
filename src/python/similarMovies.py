from pyspark import SparkConf, SparkContext
from math import sqrt

conf = SparkConf().setMaster("local[*]").setAppName("SimilarMovies")
sc = SparkContext(conf=conf)

movieID = 55
scoreThreshold = .97
coOccurenceThreshold = 30


def loadMovies():
    movieNames = {}
    with open("./ml-100k/u.item",  encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return (movie1, movie2), (rating1, rating2)


def filtermovies(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def cosineSimilarity(ratingPairs):
    score = numPairs = sumXX = sumYY = sumXY = 0
    for ratingX, ratingY in ratingPairs:
        sumXX += ratingX * ratingX
        sumYY += ratingY * ratingY
        sumXY += ratingX * ratingY
        numPairs += 1
    numerator = sumXY
    denominator = sqrt(sumXX * sumYY)
    if(denominator):
        score = numerator/float(denominator)
    return score, numPairs

movieDict = loadMovies()

data = sc.textFile('./ml-100k/u.data')

ratings = data.map(lambda x: x.split()).map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

joinedRatings = ratings.join(ratings)

filteredRatings = joinedRatings.filter(filtermovies)

pairedRatings = filteredRatings.map(makePairs)

groupedRatings = pairedRatings.groupByKey()

similarMovies = groupedRatings.mapValues(cosineSimilarity).cache()

filteredSimilarRatings = similarMovies.filter(lambda x: (x[0][0] == movieID or x[0][1] == movieID)
                                                        and x[1][0] > scoreThreshold
                                                        and x[1][1] > coOccurenceThreshold)
results = filteredSimilarRatings.map(lambda x: (x[1], x[0])).sortByKey(ascending = False).take(10)

print('Top 10 similar movies for ' + str(movieDict[movieID]))
count = 1
for result in results:
    (similarity, moviePair) = result
    if movieID == moviePair[0]:
        similarMovieID = moviePair[1]
    else:
        similarMovieID = moviePair[0]
    print(str(count) + ' ' + str(movieDict[similarMovieID])
          + ': Score - ' + str(similarity[0])
          + ', Strength - ' + str(similarity[1]))
    count += 1
