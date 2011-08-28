package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

import com.esotericsoftware.kryo._
/*
object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: WikipediaPageRank <inputFile> <threshold> <numSplits> <host> [<noCombiner>]")
      System.exit(-1)
    }

    System.setProperty("spark.serialization", "spark.KryoSerialization")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val noCombiner = args.length > 4 && args(4).nonEmpty
    val sc = new SparkContext(host, "WikipediaPageRank")

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    val vertices: RDD[(String, PRVertex[String])] = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
            NodeSeq.Empty
          }
      val outEdges = ArrayBuffer(links.map(link => new PREdge(new String(link.text))): _*)
      val id = new String(title)
      (id, new PRVertex(id, 1.0 / numVertices, outEdges))
    }).cache
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(List[(String, PRMessage[String])]())
    val result =
      if (noCombiner) {
        Bagel.run(sc, vertices, messages)(numSplits = numSplits)(new PRNoCombiner().compute(numVertices, epsilon))
      } else {
        Bagel.run(sc, vertices, messages)(combiner = new PRCombiner(), numSplits = numSplits)(PRCombiner.compute(numVertices, epsilon))
      }

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}
*/
