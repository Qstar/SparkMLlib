package GraphX

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._


// 网站参考：http://www.infoq.com/cn/articles/apache-spark-graphx
// 用Apache Spark进行大数据处理 - 第六部分: 用Spark GraphX进行图数据分析
// data ref: https://snap.stanford.edu/data/com-Youtube.html
object YoutubePagerank {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("YoutubePagerank").setMaster("local")
    val sc = new SparkContext(conf)

    // 先导入边
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/page-rank-yt-data.txt")

    // 计算图中边和节点等信息
    val vertexCount = graph.numVertices
    val vertices = graph.vertices
    println("vertices数量: " + vertices.count())

    val edgeCount = graph.numEdges
    val edges = graph.edges
    println("edges数目: " + edges.count())

    //
    // 现在来看看某些Spark GraphX API，如triplets、indegrees和outdegrees。
    //
    val triplets = graph.triplets
    println("triplets数目: " + triplets.count())
    println("triplets: " + triplets.take(5))

    val inDegrees = graph.inDegrees
    println("inDegrees数目: " +inDegrees.collect())

    val outDegrees = graph.outDegrees
    println("outDegrees数目: " +outDegrees.collect())

    val degrees = graph.degrees
    println("degrees数目: " +degrees.collect())

    // 用迭代次数作为参数
    val staticPageRank = graph.staticPageRank(10)
    staticPageRank.vertices.collect()
    println("staticPageRank数目: " +staticPageRank.vertices.collect())

    println("开始时间: "+ Calendar.getInstance().getTime)
    val pageRank = graph.pageRank(0.001).vertices
    println("结束时间: "+ Calendar.getInstance().getTime)

    // 输出结果中前5个元素
    println(pageRank.top(5).mkString("\n"))
  }
}
