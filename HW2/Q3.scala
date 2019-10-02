import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Q3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Q3").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (args.length < 4) {
      println("Usage: <in1> <in2> <out> <user1> <user2>")
      return
    }

    val in1 = args(0)
    val in2 = args(1)
    val out = args(2)

    val user1 = args(3)
    val user2 = args(4)

    val friendsLines = sc.textFile(args(0))
    val mutualFriends = friendsLines.filter { line => line.startsWith(user1 + "\t") || line.startsWith(user2 + "\t") }
      .flatMap { filteredLine =>
        val parts = filteredLine.split("\t")
        if(parts.length > 1) parts(1).split(",")
        else new Array[String](0)
      }
      .map { friend => (friend, 1) }
      .reduceByKey((a, b) => a + b)
      .filter { case (key, value) => value > 1 }

    val dataLines = sc.textFile(args(1))
    val userInfo = dataLines.map { line =>
      val parts = line.split(",")
      val nameZip = parts(1) + " " + parts(2) + ":" + parts(6);
      (parts(0), nameZip)
    }

    val result = mutualFriends.join(userInfo)
    val nameZipList = for ((k, v) <- result) yield v._2

    val nameZipFormat = user1 + " " + user2 + " [" + nameZipList.collect().mkString(", ") + "]"

    println(nameZipFormat)
    sc.parallelize(List(nameZipFormat)).saveAsTextFile(out)

  }

}