import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.util.MLUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source._
import scala.util._
import java.io.PrintWriter
import scala.reflect.io.File
import java.io._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }

object Q2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("DecisionTreeClassification").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    if (args.length < 2) {
      println("Usage: <glass> <out>")
      return
    }

    val in = args(0)
    val out = args(1)
    var res = ""

    val data = sc.textFile(in)
      .map { x =>
        val tokens = x.split(",")
        LabeledPoint(tokens(tokens.length - 1).toInt - 1, Vectors.dense(tokens.slice(1, tokens.length - 1).map { x => x.toDouble }))
      }

    // Split the data into training and test sets (40% held out for testing)
    val splits = data.randomSplit(Array(0.6, 0.4))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 7
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val dtModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    var labelAndPreds = testData.map { point =>
      val prediction = dtModel.predict(point.features)
      (point.label, prediction)
    }
    var accuracy = labelAndPreds.filter(x => x._1 == x._2).count().toDouble / testData.count()
    res = res.concat("Decision Tree Accuracy = " + accuracy + "\n")
    println("Decision Tree Accuracy = " + accuracy)

    val nbModel = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
    labelAndPreds = testData.map(p => (nbModel.predict(p.features), p.label))
    accuracy = 1.0 * labelAndPreds.filter(x => x._1 == x._2).count() / testData.count()
    res = res.concat("Naive Bayes accuracy = " + accuracy)    
    println("Naive Bayes accuracy = " + accuracy)
    
    sc.parallelize(Array(res)).saveAsTextFile(out)

  }
}