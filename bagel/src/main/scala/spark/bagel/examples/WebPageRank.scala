package spark.bagel.examples

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
    if (args.length < 5) {
      System.err.println("Usage: WebPageRank <input> <threshold> <numSplits> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serialization", "spark.KryoSerialization")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator[(Int, Int)]].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WebPageRank")

    val InputLine = """\(\((\d+),(\d+)\),\(\d+,\d+\),(\d+),(\d+),(.*)\)""".r
    val EdgeEntry = """\((\d+),(\d+)\),?""".r
    val vertices = sc.textFile(inputFile).map(line => {
      val InputLine(id, partition, value, active, rest) = line
      val key = (id.toInt, partition.toInt)
      val outEdges = EdgeEntry.findAllIn(rest).map {
        case EdgeEntry(targetId, targetPartition) =>
          new PREdge((targetId.toInt, targetPartition.toInt))
      }.toList
      (key, new PRVertex(key, value.toDouble, ArrayBuffer(outEdges: _*),
                         active.toBoolean))
    }).cache

    println("Counting vertices...")
    val numVertices = vertices.count()
    println("Done counting vertices: " + numVertices)

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(List[((Int, Int), PRMessage[(Int, Int)])]())
    val util = new PageRankUtils[(Int, Int)]
    val result =
      Bagel.run(
        sc, vertices, messages, combiner = new PRCombiner[(Int, Int)](),
        partitioner = new CustomPartitioner(numSplits), numSplits = numSplits)(
        util.computeWithCombiner(numVertices, epsilon))

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top =
      (result
       .filter { case (id, vert) => vert.value >= threshold }
       .map { case (id, vert) =>
         "%s\t%s\n".format(id, vert.value) }
       .collect.mkString)
    println(top)
  }
}
