import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._


object DegreesOfSeperation{
  var conf = new SparkConf().setMaster("local").setAppName("Popular SeperHero")
  var sc = new SparkContext(conf)

  val startCharacter: Int = 5306
  val endCharacter: Int = 14

  var hitCounter = sc.longAccumulator("")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var textFile = sc.textFile("./Marvel-Graph.txt")
    var bfs = textFile.map(convertToBfs)
    var i: Int = 1
    while (hitCounter.value <= 0){
      print("Running iteration: " + i)
      var mappedBfs = bfs.flatMap(bfsMap)
      println(" --> Processing " + mappedBfs.count() + " values.")
      bfs = mappedBfs.reduceByKey(bfsReduce)
      i += 1
    }
    println("Hit the target value after " + hitCounter.value + " hits")
  }

  def convertToBfs(line: String): (Int, (List[Int], Int, String)) = {
    var fields = line.split(" ")
    var distance = 9999
    var color = "WHITE"
    var heroId = fields(0).toInt
    var connections: List[Int] = List()
    var counter = 1
    while(counter<fields.length){
      connections = connections :+ fields(counter).toInt
      counter += 1
    }
    if(heroId == startCharacter){
      distance = 0
      color = "GRAY"
    }

    return (heroId, (connections, distance, color))
  }

  def bfsMap(node: (Int, (List[Int], Int, String))): List[(Int, (List[Int], Int, String))] = {
    var varId = node._1
    var connections = node._2._1
    var distance = node._2._2
    var color = node._2._3
    var listOfNodes: List[(Int, (List[Int], Int, String))] = List()
    if(color=="GRAY"){
      for (connection <- connections){
        var newStartId = connection
        var newDistance = distance+1
        var newColor = "GRAY"
        if(connection==endCharacter){
          hitCounter.add(1)
        }
        listOfNodes = listOfNodes :+ ((newStartId, (List(), newDistance, newColor)))
      }
      color = "BLACK"
    }
    listOfNodes = listOfNodes :+ ((varId, (connections, distance, color)))
    return listOfNodes
  }

  def bfsReduce(data1: (List[Int], Int, String), data2: (List[Int], Int, String)):  (List[Int], Int, String) = {
    var edges1 = data1._1
    var edges2 = data2._1
    var distace1 = data1._2
    var distance2 = data2._2
    var color1 = data1._3
    var color2 = data2._3

    var edges: List[Int] = List()
    var distance = 9999
    var color = "WHITE"

    if(edges1.length>0)
      edges = edges ++ edges1
    if(edges2.length>0)
      edges = edges ++ edges2

    if(distace1<distance2)
      distance = distace1
    else
      distance = distance2

    if(color1=="BLACK" || color2=="BLACK")
      color = "BLACK"
    else if(color1=="GRAY" || color2=="GRAY")
      color = "GRAY"
    else
      color = "WHITE"
    return (edges, distance, color)
  }
}

