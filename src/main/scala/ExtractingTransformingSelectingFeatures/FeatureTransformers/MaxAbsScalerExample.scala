//package ExtractingTransformingSelectingFeatures.FeatureTransformers
//
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//object MaxAbsScalerExample {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("MaxAbsScalerExample")
//    val sc = new SparkContext(conf)
//    val spark = new SQLContext(sc)
//
//    val dataFrame = spark.createDataFrame(Seq(
//      (0, Vectors.dense(1.0, 0.1, -8.0)),
//      (1, Vectors.dense(2.0, 1.0, -4.0)),
//      (2, Vectors.dense(4.0, 10.0, 8.0))
//    )).toDF("id", "features")
//
//    val scaler = new MaxAbsScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//
//    // Compute summary statistics and generate MaxAbsScalerModel
//    val scalerModel = scaler.fit(dataFrame)
//
//    // rescale each feature to range [-1, 1]
//    val scaledData = scalerModel.transform(dataFrame)
//    scaledData.select("features", "scaledFeatures").show()
//
//    sc.stop()
//  }
//}
