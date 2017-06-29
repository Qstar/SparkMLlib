package RDDBasedAPIGuide.EvaluationMetrics.RegressionModelEvaluation

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RegressionMetricsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2VecExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    // Load the data
    val data = spark
      .read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
      .rdd.map(row => LabeledPoint(row.getDouble(0), row.get(1).asInstanceOf[Vector]))
      .cache()

    // Build the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(data, numIterations)

    // Get predictions
    val valuesAndPreds = data.map{ point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }

    // Instantiate metrics object
    val metrics = new RegressionMetrics(valuesAndPreds)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")
  }
}
