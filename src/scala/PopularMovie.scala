import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.io.Source

object PopularMovie{
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
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Popular Movie").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)
    val textfile = sc.textFile("./ml-100k/u.data")
    val mapped = textfile.map(line => (line.split("\t")(1), 1))
    val counted = mapped.reduceByKey((x, y) => x+y)
    val flipped = counted.map(x => (x._2, x._1))
    val sorted = flipped.sortByKey(ascending = false)
    val movies = sc.broadcast(loadMovies())
    val movieWithName = sorted.map(x => (movies.value((x._2).toInt), x._1))
    val results = movieWithName.collect()
    for (result <- results){
      println(result)
    }
  }
}
