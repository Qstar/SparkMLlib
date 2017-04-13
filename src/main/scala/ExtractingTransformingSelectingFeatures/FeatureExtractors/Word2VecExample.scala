package ExtractingTransformingSelectingFeatures.FeatureExtractors

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Word2VecExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2VecExample")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text:   [${text.mkString(", ")}] => \nVector: $features\n")
    }

    sc.stop()
  }
}
