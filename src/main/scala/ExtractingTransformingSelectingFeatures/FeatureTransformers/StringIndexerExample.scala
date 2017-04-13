package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StringIndexerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()

    sc.stop()
  }
}
