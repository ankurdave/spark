package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}
import java.net.URL

import com.esotericsoftware.kryo._

import it.unimi.dsi.fastutil.io.BinIO
import it.unimi.dsi.fastutil.objects.ObjectList
import it.unimi.dsi.lang.MutableString
import it.unimi.dsi.util.ImmutableExternalPrefixMap
import it.unimi.dsi.webgraph.{ImmutableGraph,BVGraph}

object WebGraphParser {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: WebGraphParser <graphBaseName> <outputDir> <host>")
      System.exit(-1)
    }

    val graphBaseName = args(0)
    val outputDir = args(1)
    val host = args(2)
    val sc = new SparkContext(host, "WebGraphParser")

    print("Loading pmap...")
    val pmap =
      (BinIO.loadObject(graphBaseName + ".pmap")
       .asInstanceOf[ImmutableExternalPrefixMap])
    pmap.setDumpStream(graphBaseName + ".dump")
    println("done.")

    print("Converting pmap to list...")
    val list = pmap.list()
    println("done.")

    print("Loading graph...")
    val graph = BVGraph.load(graphBaseName)
    println("done.")

    val numVertices = graph.numNodes()
    print("Parsing %d nodes into RDD...".format(numVertices))
    val vertices: RDD[(Int, PRVertex[Int])] = sc.parallelize(
      for {
        i <- 0 until numVertices
        outEdges = getSuccessors(i, graph).map(
          targetId => new PREdge(targetId))
      } yield (i, new PRVertex(i, 1.0 / numVertices, outEdges,
                               partition = getNodePartition(i, list)))
    )
    println("done.")

    println("URL for node with ID 100: %s".format(list.get(100)))

    print("Saving RDD...")
    vertices.saveAsTextFile(outputDir)
    println("done.")
  }

  def getSuccessors(i: Int, g: BVGraph): ArrayBuffer[Int] = {
    val result = new ArrayBuffer[Int]
    val successors = g.successors(i)
    var d = g.outdegree(i) - 1
    //    println("Vertex " + i + " has outdegree " + d)
    while (d > 0) {
      result.append(successors.nextInt())
      d -= 1
    }
    result
  }

  def getNodePartition(i: Int, list: ObjectList[MutableString]): Int = {
    val url = list.get(i).toString()
    val host = new URL(url).getHost()
    println("Vertex %d has host %s".format(i, host))
    host.hashCode()
  }
}
