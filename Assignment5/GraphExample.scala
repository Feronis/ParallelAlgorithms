import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by Justin Baraboo
  */
object GraphExample {
  def main(args: Array[String]): Unit = {

    //this tricks windows
    System.setProperty("hadoop.hom.dir", "C:\\Users\\Justin Baraboo\\Desktop\\winutils");

    //this sets up spark
    val sparkConf = new SparkConf().setAppName("SparkActions").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)


    // Create an RDD for the vertices
    //skip this part, it's the first stuff we did in class

    /*val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
      (4L, ("peter", "student"))))


    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))


    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
*/

    println("--------------------------------------------------------")

    //start of assignment 5

    //connected components
    //used: https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html
    val graph2 = GraphLoader.edgeListFile(sc, "C:\\Users\\Justin Baraboo\\Desktop\\followers.txt")
    // Find the connected components
    val cc = graph2.connectedComponents().vertices
    // Join the connected components with the usernames
    val users2 = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users2.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))


    //page rank
    //used: https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html
    // Load the edges as a graph
    val graph3 = GraphLoader.edgeListFile(sc, "C:\\Users\\Justin Baraboo\\Desktop\\followers.txt")
    // Run PageRank
    val ranks = graph3.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users3 = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users3.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))


    //triangle counting
    //used: https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html
    // Load the edges in canonical order and partition the graph for triangle count
    val graph4 = GraphLoader.edgeListFile(sc, "C:\\Users\\Justin Baraboo\\Desktop\\followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph4.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users4 = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users4.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))

  }
}
