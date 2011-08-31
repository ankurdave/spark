package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.net.URL

import com.esotericsoftware.kryo._

import it.unimi.dsi.fastutil.io.BinIO
import it.unimi.dsi.fastutil.objects.ObjectList
import it.unimi.dsi.lang.MutableString
import it.unimi.dsi.util.ImmutableExternalPrefixMap
import it.unimi.dsi.webgraph.BVGraph

object WebGraphParser {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(
        "Usage: WebGraphParser <graphBaseName> > <outputFile>")
      System.exit(-1)
    }

    val graphBaseName = args(0)

    System.err.print("Loading fcl...")
    val list =
      (BinIO.loadObject(graphBaseName + ".fcl")
       .asInstanceOf[ObjectList[CharSequence]])
    System.err.println("done.")

    System.err.print("Loading graph...")
    val graph = BVGraph.load(graphBaseName)
    System.err.println("done.")

    val numVertices = graph.numNodes()
    System.err.print("Parsing %d nodes...".format(numVertices))
    for (i <- 0 until numVertices) {
      val outEdges = getSuccessors(i, graph).map(
        targetId => new PREdge( (targetId, getNodePartition(targetId, list))))
      val partition = getNodePartition(i, list)
      val key = (i, partition)
      val entry = (key, new PRVertex(key, 1.0 / numVertices, outEdges))
      println(entry)

      if (i % 10000 == 0) {
        System.err.print(".")
      }
      if (i % 1000000 == 0) {
        System.err.print(i)
      }
    }
    System.err.println("done.")
  }

  def getSuccessors(i: Int, g: BVGraph): ArrayBuffer[Int] = {
    val result = new ArrayBuffer[Int]
    val successors = g.successors(i)
    var d = g.outdegree(i) - 1
    //    System.err.println("Vertex " + i + " has outdegree " + d)
    while (d > 0) {
      result.append(successors.nextInt())
      d -= 1
    }
    result
  }

  def getNodePartition(i: Int, list: ObjectList[CharSequence]): Int = {
    val url = list.get(i).toString()
    val host = new URL(url).getHost()
//    System.err.println("Vertex %d has host %s".format(i, host))
    host.hashCode()
  }
}
