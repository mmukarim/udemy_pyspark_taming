import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.io.Source


object PopularMoviesDataframe{
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder.master("local").appName("PopularMoviesDataframe").getOrCreate()
    var textFile = spark.sparkContext.textFile("././ml-100k/u.data")
    var movies = textFile.map(x => Row(x.split("\t")(1).toInt))
    var schemaMovies = spark.createDataFrame(movies, StructType(List(StructField("movieID", IntegerType, true))))
    schemaMovies.createOrReplaceTempView("movies")
    var countedMovies = schemaMovies.groupBy("movieID").count().orderBy(desc("count")).cache()
    countedMovies.show(40, false)
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
}
