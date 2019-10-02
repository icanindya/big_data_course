import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors

object Q1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Clustering").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (args.length < 3) {
      println("Usage: <itemusermat> <movies.dat> <out>")
      return
    }
    val in1 = args(0)
    val in2 = args(1)
    val out = args(2)

    // Load and parse the data
    val data = sc.textFile(in1)
    val parsedData = data.map { s =>
      Vectors.dense(s.split(" ").slice(1, 6041).map(_.toDouble))
    }.cache()

    parsedData.saveAsTextFile("./dataset/parsedData.txt")

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val movieCluster = data.map { s =>
      val m = s.split(" ")(0)
      val f = Vectors.dense(s.split(" ").slice(1, 6041).map(_.toDouble))
      val cid = clusters.predict(f)
      (cid, m)
    }
      .reduceByKey(_ + "," + _)
      .map { x =>
        (x._1, x._2.split(",").slice(0, Math.min(x._2.split(",").length, 5)).mkString(","))
      }
      .flatMap { x =>
        val c = x._1
        x._2.split(",").map { x => (x, c) }
      }

    val movieInfo = sc.textFile(in2)
      .map { x =>
        val tokens = x.split("::")
        (tokens(0), tokens(1) + ", " + tokens(2))
      }

    movieCluster.join(movieInfo).map { x =>
      (x._2._1, x._1 + ", " + x._2._2)
    }
      .groupByKey()
      .sortByKey(true)
      .map { x =>
        "cluster " + x._1 + ": \n" + x._2.mkString("\n")
      }
      .saveAsTextFile(out)

  }
}