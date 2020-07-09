package com.spark.icp9

import org.apache.spark._

object DepthFirstSearch {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("merge sort").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]

    val g: Graph=Map(1 -> List(7,9), 7 -> List(1,8),8 -> List(7,9), 9 -> List(1,8))

    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(vertex: Vertex,visited: List[Vertex]): List[Vertex] = {
        if(visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          val result = vertex :: visited
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
        }
      }

      DFS0(start, List()).reverse
    }
    val dfsresult=DFS(8,g)

    println("DFS Output",dfsresult.mkString(","))
  }
}
