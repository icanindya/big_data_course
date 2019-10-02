import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import java.util.Formatter.DateTime
import java.util.Date

object Q1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Q1").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (args.length < 2) {
      println("Usage: <in1> <out>")
      return
    }

    val in1 = args(0)
    val out = args(1)

    val friendsLines = sc.textFile(args(0))
    val mapper = friendsLines.flatMap { line =>
      val parts = line.split("\t")
      if (parts.length > 1) {
        val friends = parts(1).split(",")

        var friendPairs = for (friend <- friends) yield (parts(0), "1" + friend)
        var fofPairs = new ArrayBuffer[(String, String)]()
        for (i <- 0 to friends.length - 1; j <- i + 1 to friends.length - 1) {
          fofPairs.insert(fofPairs.length, (friends(i), "2" + friends(j)))
          fofPairs.insert(fofPairs.length, (friends(j), "2" + friends(i)))
        }
        friendPairs.union(fofPairs)
      } else
        new Array[(String, String)](0)
    }
      .reduceByKey { (x, y) => x + "," + y }
      .sortByKey()
    
    var mapperStart = new Date().getTime
      
    mapper.map { x =>

      val k = x._1
      val v = x._2

      var recommendedFriends = new HashMap[String, Int]()

      val fofs = v.split(",")
      for (fof <- fofs) {
        val id = fof.substring(1)
        if (fof.startsWith("1")) {
          recommendedFriends.+=((id, -1))
        } else {
          if (recommendedFriends.contains(id)) {
            if (recommendedFriends(id) != -1) {
              recommendedFriends.+=((id, recommendedFriends(id) + 1))
            }
          } else recommendedFriends.+=((id, 1))
        }
      }

      recommendedFriends = recommendedFriends.filter { x => x._2 > -1 }
      var sortedRecoFriends = recommendedFriends.toSeq.sortWith{(x, y) =>
        if(x._2 > y._2) true
        else if(x._2 == y._2) x._1 < y._1
        else false
      }
      var recoFriendIds = for (i <- 0 to Math.min(10, sortedRecoFriends.length - 1)) yield sortedRecoFriends(i)._1
      k + "\t" + recoFriendIds.mkString(",")
    }
    .saveAsTextFile(out)

    println("Mapper time: " + (new Date().getTime - mapperStart)/1000 )
    
  }

}