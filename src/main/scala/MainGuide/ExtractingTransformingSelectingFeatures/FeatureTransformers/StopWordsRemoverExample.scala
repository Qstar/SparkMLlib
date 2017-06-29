package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StopWordsRemoverExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)

    sc.stop()
  }
}
