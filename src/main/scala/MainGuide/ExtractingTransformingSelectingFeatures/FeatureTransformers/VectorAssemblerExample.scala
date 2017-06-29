package MainGuide.ExtractingTransformingSelectingFeatures.FeatureTransformers

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorAssemblerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VectorAssemblerExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    dataset.show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)

    sc.stop()
  }
}
