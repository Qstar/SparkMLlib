package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TokenizerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import org.apache.spark.ml.feature.VectorIndexer

    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()

    sc.stop()
  }
}
