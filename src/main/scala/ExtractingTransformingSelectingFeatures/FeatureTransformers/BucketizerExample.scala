package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BucketizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BucketizerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length - 1} buckets")
    bucketedData.show()

    sc.stop()
  }
}
