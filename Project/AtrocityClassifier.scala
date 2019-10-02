import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import java.io.File
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.DecisionTree

object AtrocityClassifier {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Clustering").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val dictionary = new HashMap[String, Int]

    val lines = sc.textFile("/home/anindya/Dropbox/Academic/UTD S16/Big Data/Project/Code/atrocity/data/atrocity_dataset.tsv")
    val docs = lines.toArray()
    java.util.Collections.shuffle(java.util.Arrays.asList(docs: _*))

    for (doc <- docs) {
      val cols = doc.split("\t")
      for (word <- cols(0).split("\\s+")) {
        dictionary.+=((word, 0))
      }
    }

    val data = for (doc <- docs) yield {
      val cols = doc.split("\t")
      val features = dictionary.clone()
      for (word <- cols(0).split("\\s+")) {
        features.+=((word, 1))
      }
      LabeledPoint(cols(1).toDouble, Vectors.dense(features.values.map { x => x.toDouble }.toArray))
    }

    val rddData = sc.parallelize(data)
    val splits = rddData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //    val nbModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    //    val nbPredictionAndLabel = test.map(p => (nbModel.predict(p.features), p.label))
    //    val nbAccuracy = 1.0 * nbPredictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    //
    //    println("Naive Bayes Accuracy: " + nbAccuracy)
    //
    //    /*--------------------------------------------------------------------*/
    //
    //    val lrModel = new LogisticRegressionWithLBFGS()
    //      .setNumClasses(2)
    //      .run(training)
    //
    //    val lrPredictionAndLabel = test.map(p => (lrModel.predict(p.features), p.label))
    //    val lrAccuracy = 1.0 * lrPredictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    //
    //    println("Logitic Regression Accuracy: " + lrAccuracy)
    /*--------------------------------------------------------------------*/
//    // Run training algorithm to build the model
//    val numIterations = 100
//    val svmModel = SVMWithSGD.train(training, numIterations)
//
//    // Compute raw scores on the test set.
//    val svmPredictionAndLabel = test.map(p => (svmModel.predict(p.features), p.label))
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(svmPredictionAndLabel)
//    val auROC = metrics.areaUnderROC()
//
//    println("Area under ROC = " + auROC)
//    val svmAccuracy = 1.0 * svmPredictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
//    println("SVM Accuracy: " + svmAccuracy)

    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val dtModel = DecisionTree.trainClassifier(training, 2, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val dtPredictionAndLabel = test.map { p => (dtModel.predict(p.features), p.label) }
    val dtAccuracy = 1.0 * dtPredictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Decsion Tree Accuracy: " + dtAccuracy)

  }

}