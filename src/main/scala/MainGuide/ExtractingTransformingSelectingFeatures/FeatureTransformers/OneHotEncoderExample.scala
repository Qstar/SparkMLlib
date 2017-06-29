package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OneHotEncoderExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show(false)

    sc.stop()
  }
}
