package GraphX

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._

// 网站参考：http://www.infoq.com/cn/articles/apache-spark-graphx
// 用Apache Spark进行大数据处理 - 第六部分: 用Spark GraphX进行图数据分析
// data ref:https://snap.stanford.edu/data/com-LiveJournal.html
object LiveJournalConnectedComponents {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LiveJournalConnectedComponents").setMaster("local")
    val sc = new SparkContext(conf)

    // Connected Components
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/connected-components-lj-data.txt")

    println("开始时间: "+ Calendar.getInstance().getTime)
    val cc = graph.connectedComponents()
    println("结束时间: "+ Calendar.getInstance().getTime)

    cc.vertices.collect()

    // 输出结果中前5个元素
    println(cc.vertices.take(5).mkString("\n"))

    val scc = graph.stronglyConnectedComponents(5)
    scc.vertices.collect().foreach(ele => println(ele._1 + " " + ele._2))
  }
}
