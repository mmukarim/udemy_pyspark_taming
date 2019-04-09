import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object FriendsByAge{
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Friends By Age").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)

    val textfile = sc.textFile("./fakefriends.csv")
    val values = textfile.map(parseLine).mapValues(x => (x, 1))
    val totalsByAge = values.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))
    val avgByAge = totalsByAge.mapValues(x => (x._1/x._2).toInt).sortByKey()
    val results = avgByAge.collect()
    for (result <- results){
      println(result)
    }
  }

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    return (fields(2).toInt, fields(3).toInt)
  }
}
