import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("./Book.txt")
    val words = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x+y)
    val sortedCountedWords = words.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1))
    sortedCountedWords.foreach(println)
    val results = sortedCountedWords.collect()
    for (result <- results){
      println(result)
    }
  }
}
