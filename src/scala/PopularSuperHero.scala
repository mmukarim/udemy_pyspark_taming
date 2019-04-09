import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PopularSuperHero{
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("Popular SeperHero")
    var sc = new SparkContext(conf)

    var heroFile = sc.textFile("./Marvel-Names.txt")
    var heroNames = heroFile.map(loadHeroes)

    var textFile = sc.textFile("./Marvel-Graph.txt")
    var coOccurences = textFile.map(countCoOccurences)
    var total = coOccurences.reduceByKey((x, y) => x+y)
    var flipped = total.map(x => (x._2, x._1)).sortByKey()
    var mostPopular = flipped.max()
    var mostPopularName = heroNames.lookup(mostPopular._2.toInt)(0)
    println(mostPopularName)
  }

  def countCoOccurences(line: String): (String, Int) = {
    var fields = line.split(" ")
    return (fields(0), fields.length-1)
  }

  def loadHeroes(line: String): (Int, String) = {
    var fields = line.split("\"")
    var heroId = fields(0).split(" ")
    if(fields.length>1)
      return (heroId(0).toInt, fields(1))
    else
      return (heroId(0).toInt, "")
  }
}
