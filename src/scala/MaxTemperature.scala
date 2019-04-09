import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object MaxTemperature{
  def ParseLine(line: String): (String, String, Double) = {
    val fields: Array[String] = line.split(",")
    val stationId: String = fields(0)
    val entryType: String = fields(2)
    val temperature: Double = fields(3).toDouble * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId, entryType, temperature)
  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Minimum Temperature").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val textFile = sc.textFile("./1800.csv")
    val mapped = textFile.map(ParseLine)
    val maxTemps = mapped.filter(x => x._2 == "TMAX")
    val stationTemps = maxTemps.map(x => (x._1, x._3))
    val stationMaxTemps = stationTemps.reduceByKey((x, y) => math.max(x, y))
    val results = stationMaxTemps.collect()
    for (result <- results) {
      println(result)
    }
  }
}

