package GraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object StructuralOperators {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuralOperators")
      .getOrCreate()

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      spark.sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      spark.sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    graph.vertices.collect.foreach(println)
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))


    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph1 = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph1)
    validCCGraph.vertices.foreach(println)
  }
}
