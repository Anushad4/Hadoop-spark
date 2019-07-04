import org.apache.spark._


object bfs {

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  val conf: SparkConf = new SparkConf().setAppName("mergeSort").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    class Graph[T] {
      type Vertex = Int
      type Graph = Map[Vertex, List[Vertex]]
      val g: Graph = Map(1 -> List(2, 3, 5, 6, 7), 2 -> List(1, 3, 4, 7, 6), 3 -> List(2, 1), 4 -> List(2, 5, 7))

      def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {

        def BFS0(elems: List[Vertex], visited: List[List[Vertex]]): List[List[Vertex]] = {
          val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
          if (newNeighbors.isEmpty)
            visited


          //newNeighbors.saveAsTextFile("output1")
          else
            BFS0(newNeighbors, newNeighbors :: visited)
        }

        BFS0(List(start), List(List(start))).reverse


      }

    }
  }

}