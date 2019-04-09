import org.apache.spark.sql._
import org.apache.spark.sql.types._


object SparkSQL{
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder.master("local").appName("SQL").getOrCreate()
    var textFile = spark.sparkContext.textFile("./fakefriends.csv")
    var peoples = textFile.map(mapper)

    var schema = List(
        StructField("ID", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("numFriends", IntegerType, true)
      )

    var schemaPeople = spark.createDataFrame(peoples, StructType(schema)).cache()
    schemaPeople.createOrReplaceTempView("people")
    schemaPeople.where("age<20").show()
//    var teenAgers = spark.sql("Select * from people where age<20")
//    teenAgers.show()
  }

  def mapper(line: String): Row = {
    var fields = line.split(",")
    return Row(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }
}