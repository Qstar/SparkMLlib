package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PolynomialExpansionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PolynomialExpansionExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val polyDF = polyExpansion.transform(df)
    polyDF.show(false)

    sc.stop()
  }
}
