import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source._
import scala.util._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

object KMeans {
  
  class Point(var id: String, var x1: Double, var x2: Double){
    this.id = id
    this.x1 = x1
    this.x2 = x2
    
    var clusterNum = -1
    var clusterDist = Double.MaxValue
  }
  
  class Cluster(var id: String, var x1: Double, var x2: Double){
    this.id = id
    this.x1 = x1
    this.x2 = x2
    
    var px1 = 0.0
    var px2 = 0.0
    var members = new ListBuffer[Point]()
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Clustering").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    
    val k = 2
    
    var clusterList = Array(("c1", (2,2)), ("c2", (3,5)))
    var clusters = new Array[Cluster](2)
    var pointList = Array(("p1", (2,2)), ("p2", (3,4)), ("p3", (4,7)), ("p4", (5,3)), ("p5", (6,7)), ("p6", (8,7)), ("p7", (8,1)), ("p8", (9,3)))
    var points = new Array[Point](8)
    
    var i = 0
    clusterList.map{x => 
      clusters(i) = new Cluster(x._1, x._2._1, x._2._2)
      i += 1
    }
    i = 0
    pointList.map{x => 
      points(i) = new Point(x._1, x._2._1, x._2._2)
      i += 1
    }
    
    var itr = 0 
    
    do{
      itr += 1
      println(itr)
      
      for(cluster <- clusters){
        cluster.members = new ListBuffer[Point]()
      }
      
      for(i <- 0 to points.length - 1){
        for(j <- 0 to clusters.length - 1){
          
          val newDist = distance(points(i), clusters(j))
          if(newDist < points(i).clusterDist){
            points(i).clusterNum = j
            points(i).clusterDist = newDist
          }
        }
        clusters(points(i).clusterNum).members += points(i)
      }

      for(cluster <- clusters){
        cluster.px1 = cluster.x1
        cluster.px2 = cluster.x2
        cluster.x1 = 0.0
        cluster.x2 = 0.0
        for(point <- cluster.members){
          cluster.x1 += point.x1  
          cluster.x2 += point.x2  
        }
        cluster.x1 /= cluster.members.length
        cluster.x2 /= cluster.members.length
      }
    }while(change(clusters))
  }
  def distance(p: Point, c: Cluster): Double = {
    Math.sqrt(Math.pow((p.x1 - c.x1), 2) + Math.pow((p.x2 - c.x2), 2))
  }
  
  def change(clusters: Array[Cluster]): Boolean = {
    for(cluster <- clusters){
      if(cluster.px1 != cluster.x1 || cluster.px2 != cluster.x2) {
        return true
      }
    }
    return false
  }

}