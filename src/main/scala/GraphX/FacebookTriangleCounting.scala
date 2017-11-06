package GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._

// 网站参考：http://www.infoq.com/cn/articles/apache-spark-graphx
// 用Apache Spark进行大数据处理 - 第六部分: 用Spark GraphX进行图数据分析
// data ref:https://snap.stanford.edu/data/egonets-Facebook.html
object FacebookTriangleCounting {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("FacebookTriangleCounting").setMaster("local")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "data/graphx/triangle-count-fb-data.txt")

    println("Number of vertices : " + graph.vertices.count())
    println("Number of edges : " + graph.edges.count())

    println("打印部分顶点信息")
    graph.vertices.take(5).foreach(v => println(v))

    val tc = graph.triangleCount()
    println("打印部分triangle的顶点信息")
    tc.vertices.collect.take(5).foreach(ele => println(ele._1 + " " + ele._2))

    println("tc: " + tc.vertices.take(5).mkString("\n"))

    // println("Triangle counts: " + graph.connectedComponents.triangleCount().vertices.collect().mkString("\n"));

    println("Triangle counts: " + graph.connectedComponents.triangleCount().vertices.top(5).mkString("\n"))

    val sum = tc.vertices.map(a => a._2).reduce((a, b) => a + b)

    println("sum: " + sum)
  }
}
