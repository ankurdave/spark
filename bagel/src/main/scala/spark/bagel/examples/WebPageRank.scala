/*package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

import com.esotericsoftware.kryo._

import it.unimi.dsi.webgraph.{ImmutableGraph,BVGraph}

object WebPageRank {
  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: WebPageRank <inputWebGraphBaseName> <threshold> <numSplits> <host> <useCombiner> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serialization", "spark.KryoSerialization")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator[Int]].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val useCombiner = args(4).toBoolean
    val usePartitioner = args(5).toBoolean
    val sc = new SparkContext(host, "WebPageRank")

    // Parse the WebGraph data into an RDD
    val input = BVGraph.loadMapped(inputFile)

    println("Counting vertices...")
    val numVertices = input.numNodes()
    println("Done counting vertices: " + numVertices)

    println("Parsing input file...")
    val vertices: RDD[(String, PRVertex[Int])] = sc.parallelize(
      for {
        i <- 0 until numVertices
        outEdges = WebGraphParser.getSuccessors(i, input).map(targetId => new PREdge(targetId.toString()))
      } yield (i.toString(), new PRVertex(i.toString(), 1.0 / numVertices, outEdges, true))
    ).cache
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(List[(String, PRMessage[Int])]())
    val result =
      if (!useCombiner) {
        Bagel.run(sc, vertices, messages)(numSplits = numSplits)(new PRNoCombiner().compute(numVertices, epsilon))
      } else {
        Bagel.run(sc, vertices, messages)(combiner = new PRCombiner(), numSplits = numSplits)(PRCombiner.compute(numVertices, epsilon))
      }

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}
*/
