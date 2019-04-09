import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object MinTemperature{
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
    val minTemps = mapped.filter(x => x._2 == "TMIN")
    val stationTemps = minTemps.map(x => (x._1, x._3))
    val stationMinTemps = stationTemps.reduceByKey((x, y) => math.min(x, y))
    val results = stationMinTemps.collect()
    for (result <- results) {
      println(result)
    }
  }
}

