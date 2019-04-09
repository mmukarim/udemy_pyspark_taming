import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.io.Source

object SimilarMovies{
  var movieID = 55
  var scoreThreshold = .97
  var coOccurenceThreshold = 30
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Word Count")
    val sc = new SparkContext(conf)

    var movieDict: Map[Int, String] = loadMovies()
    val textFile = sc.textFile("./ml-100k/u.data")
    var ratings = textFile.map(makeRatings)
    var joinedRatings = ratings.join(ratings)
    var filteredRatings = joinedRatings.filter(filterMovies)
    var pairedRatings = filteredRatings.map(makePairs)
    var groupedRatings = pairedRatings.groupByKey().mapValues(x => x.toList)
    var similarMovies = groupedRatings.mapValues(cosineSimilarity).cache()
    var filteredSimilarRatings = similarMovies.filter(filterRatings)
    var results = filteredSimilarRatings.map(x => (x._2, x._1)).sortByKey(ascending = false).take(10)

    var count = 1
    println("Top 10 similar movies for " + movieDict(movieID))
    for((similarity, moviePair) <- results){
      var similarMovieID: Int = 0
      if(movieID == moviePair._1)
        similarMovieID = moviePair._2
      else
        similarMovieID = moviePair._1
      println(count + ". " + movieDict(similarMovieID))
      count += 1
      }
    }

  def loadMovies(): Map[Int, String] = {
    var movieNames: Map[Int, String] = Map()
    val fileName = "./ml-100k/u.item"
    val fileData = Source.fromFile(fileName, enc = "ISO-8859-1")
    for (line <- fileData.getLines()) {
      val fields = line.split("[|]")
      movieNames += (fields(0).toInt -> fields(1).toString())
    }
    return movieNames
  }

  def makeRatings(line: String): (Int, (Int, Float)) = {
    var fields = line.split("\t")
    return (fields(0).toInt, (fields(1).toInt, fields(2).toFloat))
  }

  def filterMovies(userRatings: (Int,((Int, Float), (Int, Float)))): Boolean = {
    var movie1: Int = userRatings._2._1._1
    var movie2: Int = userRatings._2._2._1
    var ratings1: Float = userRatings._2._1._2
    var ratings2: Float = userRatings._2._2._2
    return movie1<movie2
  }

  def makePairs(userRatings: (Int,((Int, Float), (Int, Float)))): ((Int, Int), (Float, Float)) = {
    var movie1: Int = userRatings._2._1._1
    var movie2: Int = userRatings._2._2._1
    var ratings1: Float = userRatings._2._1._2
    var ratings2: Float = userRatings._2._2._2
    return ((movie1, movie2), (ratings1, ratings2))
  }

  def cosineSimilarity(ratingPairs: List[(Float, Float)]): (Float, Int) = {
    var score: Float = 0
    var sumXX: Float = 0
    var sumXY: Float = 0
    var sumYY: Float = 0
    var numPair: Int = 0
    for(ratings <- ratingPairs){
      sumXX += (ratings._1 * ratings._1)
      sumYY += (ratings._2 * ratings._2)
      sumXY += (ratings._1 * ratings._2)
      numPair += 1
    }
    score = sumXY/(math.sqrt(sumXX*sumYY)).toFloat
    return (score, numPair)
  }

  def filterRatings(similarMovies: ((Int, Int), (Float, Int))): Boolean = {
    var flag: Boolean = ((similarMovies._1._1 == movieID) || (similarMovies._1._2 == movieID)) &&
      (similarMovies._2._1 > scoreThreshold) && (similarMovies._2._2 > coOccurenceThreshold)
    return flag
  }
}

