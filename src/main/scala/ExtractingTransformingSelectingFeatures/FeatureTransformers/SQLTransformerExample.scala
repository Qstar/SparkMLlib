package ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLTransformerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLTransformerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val df = spark.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    sqlTrans.transform(df).show()

    sc.stop()
  }
}
