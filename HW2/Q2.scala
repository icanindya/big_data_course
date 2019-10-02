import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Q2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Print Mutual Friends").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if(args.length < 4){
      println("Usage: <in> <out> <user1> <user2>")
      return
    }
    
    val in = args(0)
    val out = args(1)
    
    val user1 = args(2)
    val user2 = args(3)

    val lines = sc.textFile(args(0))
    val mutualFriends = lines.filter { line => line.startsWith(user1 + "\t") || line.startsWith(user2 + "\t") }
      .flatMap { filteredLine =>
        val parts = filteredLine.split("\t")
        if(parts.length > 1) parts(1).split(",")
        else new Array[String](0)
      }
      .map { friend => (friend, 1) }
      .reduceByKey((a, b) => a + b)
      .filter { case (key, value) => value > 1 }
      .keys
    
    val mutualFriendsFormat = user1 + ", " + user2 + " " + mutualFriends.collect().mkString(",") 
           
    println(mutualFriendsFormat)
    sc.parallelize(List(mutualFriendsFormat)).saveAsTextFile(out)

  }

}