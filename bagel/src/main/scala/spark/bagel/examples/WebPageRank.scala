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
    if (args.length < 6) {
      System.err.println("Usage: WebPageRank <input> <threshold> <numSplits> <host> <useCombiner> <usePartitioner>")
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

    val vertices = sc.textFile(inputFile).map(line => {
      val fields = line.split(",")
      val outEdges = fields(5).split(" ").map(x => new PREdge(x.toInt))
      (fields(0).toInt,
       new PRVertex(
         fields(1).toInt, fields(2).toDouble, ArrayBuffer(outEdges: _*),
         fields(3).toBoolean, fields(4).toInt))
    }).cache

    println("Counting vertices...")
    val numVertices = vertices.count()
    println("Done counting vertices: " + numVertices)

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(List[(Int, PRMessage[Int])]())
    val util = new PageRankUtils[Int]
    val result =
      if (!useCombiner) {
        Bagel.run[Int, PRVertex[Int], PRMessage[Int], ArrayBuffer[PRMessage[Int]]](
          sc, vertices, messages,
          util.computeNoCombiner(numVertices, epsilon),
          numSplits = numSplits)
      } else {
        Bagel.run[Int, PRVertex[Int], PRMessage[Int], Double](
          sc, vertices, messages,
          util.computeWithCombiner(numVertices, epsilon),
          combiner = new PRCombiner[Int](), numSplits = numSplits)
      }

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}
