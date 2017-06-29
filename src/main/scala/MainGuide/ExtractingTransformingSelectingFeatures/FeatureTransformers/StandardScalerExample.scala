package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StandardScalerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StandardScalerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()

    sc.stop()
  }
}
