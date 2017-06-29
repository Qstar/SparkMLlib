package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object QuantileDiscretizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QuantileDiscretizerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show()

    sc.stop()
  }
}
