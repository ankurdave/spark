package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import java.io.FileOutputStream
import java.net.URL
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.SequenceFile

import it.unimi.dsi.fastutil.io.BinIO
import it.unimi.dsi.fastutil.objects.ObjectList
import it.unimi.dsi.lang.MutableString
import it.unimi.dsi.util.ImmutableExternalPrefixMap
import it.unimi.dsi.webgraph.BVGraph

object WebGraphParser {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: WebGraphParser <graphBaseName> <outputFile>")
      System.exit(-1)
    }

    val graphBaseName = args(0)
    val outputFile = args(1)

    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", classOf[WGKryoRegistrator].getName)

    System.err.print("Loading fcl...")
    val list =
      (BinIO.loadObject(graphBaseName + ".fcl")
       .asInstanceOf[ObjectList[CharSequence]])
    System.err.println("done.")

    System.err.print("Loading graph...")
    val graph = BVGraph.load(graphBaseName)
    System.err.println("done.")

    val numVertices = graph.numNodes()

    val config = new Configuration()
    val fs = FileSystem.get(new URI(outputFile), config)
    val writer = SequenceFile.createWriter(
      fs, config, new Path(outputFile), classOf[NullWritable], classOf[BytesWritable])
    val valWritable = new BytesWritable()

    val seen = new scala.collection.mutable.HashSet[Long]

    System.err.print("Parsing %d nodes...".format(numVertices))
    for (i <- 0 until numVertices) {
      val outEdges = getSuccessors(i, graph).map(
        targetId => getIdPartition(targetId, list))
      val key = getIdPartition(i, list)
      if (seen.contains(key)) {
        System.err.println("Duplicate key %s".format(key.toString))
      } else {
        seen += key
      }
      val entry = Array((key, new PRVertex(1.0 / numVertices, outEdges.toArray)))
      val bytes = Utils.serialize(entry)

      valWritable.set(bytes, 0, bytes.length)
      writer.append(NullWritable.get(), valWritable)

      if (i % 10000 == 0) {
        System.err.print(".")
      }
      if (i % 1000000 == 0) {
        System.err.print("\n" + i)
      }
    }
    writer.close()
    System.err.println("done.")
  }

  def getSuccessors(i: Int, g: BVGraph): Array[Int] = {
    val successors = g.successors(i)
    var d = g.outdegree(i)
    val result = new Array[Int](d)
    for (i <- 0 until d) {
      result(i) = successors.nextInt()
    }
    result
  }

  def getIdPartition(i: Int, list: ObjectList[CharSequence]): Long = {
    val url = list.get(i).toString()
    val host = new URL(url).getHost()
//    System.err.println("Vertex %d has host %s".format(i, host))
    i.toLong << 32 | host.hashCode().toLong & 0x00000000FFFFFFFFL
  }
}
