import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.commons.math3.geometry.spherical.oned.ArcsSet.Split

object Q4 {

  def getAge(dob: String): Int = {
    Util.getAge(dob)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Q4").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (args.length < 5) {
      println("Usage: <in1> <in2> <out1> <out2> <out3>")
      return
    }

    val in1 = args(0)
    val in2 = args(1)

    val out1 = args(2)
    val out2 = args(3)
    val out3 = args(4)

    val friendsLines = sc.textFile(args(0))
    val userFriend = friendsLines.filter { line => line.split("\t").length > 1 }
      .flatMap { line =>
        val parts = line.split("\t")
        val friends = parts(1).split(",")
        for (friend <- friends) yield (parts(0), friend)
      }

    val dataLines = sc.textFile(args(1))
    val userAge = dataLines.map { line =>
      val parts = line.split(",")
      val age = getAge(parts(9))
      (parts(0), age)
    }

    val joined = userFriend.join(userAge)
    val userFriendAge = for ((k, v) <- joined) yield v
    var userFriendAgeCount = userFriendAge.mapValues { v => (v, 1) }
    userFriendAgeCount = userFriendAgeCount.reduceByKey { (x, y) => (x._1 + y._1, x._2 + y._2) }
    val userFriendAvgAge = userFriendAgeCount.mapValues { v => (v._1 / v._2).toInt }.sortByKey()

    var sortedFriendAvgAgeUser = for ((k, v) <- userFriendAvgAge) yield (v, k)
    sortedFriendAvgAgeUser = sortedFriendAvgAgeUser.sortByKey(false)
    val sortedUserFriendAvgAge = for ((k, v) <- sortedFriendAvgAgeUser) yield (v, k)

    userFriendAvgAge.saveAsTextFile(out1)
    sortedUserFriendAvgAge.saveAsTextFile(out2)
    sc.parallelize(sortedUserFriendAvgAge.takeOrdered(20)(Ordering.Int.reverse.on { x => x._2 })).saveAsTextFile(out3)

  }

}