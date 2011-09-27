package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.xml.{XML,NodeSeq}

import scala.collection.mutable.ArrayBuffer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}

object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: WikipediaPageRank <inputFile> <threshold> <numIterations <host> <useCombiner> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.WPRSerializer")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val host = args(3)
    val useCombiner = args(4).toBoolean
    val usePartitioner = args(5).toBoolean
    val sc = new SparkContext(host, "WikipediaPageRank")

    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    val links = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val id = new String(title)
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
      val outEdges = links.map(link => new String(link.text)).toArray
      (id, outEdges)
    }).partitionBy(partitioner).cache
    val n = links.count
    val defaultRank = 1.0 / n
    var ranks = links.mapValues { edges => defaultRank }
    val a = 0.15

    // Do the computation
    println("Starting computation")
    val startTime = System.currentTimeMillis
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (Seq(links), Seq(rank))) =>
          links.map(dest => (dest, rank / links.size))
        case (id, (Seq(links), Seq())) =>
          links.map(dest => (dest, defaultRank / links.size))
        case (id, (Seq(), Seq(rank))) =>
          Array[(String, Double)]()
      }
      val combine = (x: Double, y: Double) => x + y
      ranks = (contribs.combineByKey(x => x, combine, combine, sc.defaultParallelism, partitioner)
               .mapValues(sum => a/n + (1-a)*sum))

      ranks.foreach(x => {})
      println("Finished iteration %d".format(i))
    }

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) => "%s\t%s\n".format(id, rank) }
       .collect.mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }
}

class WPRSerializer extends spark.Serializer {
  def newInstance(): SerializerInstance = new WPRSerializerInstance()
}

class WPRSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    throw new UnsupportedOperationException()
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new WPRSerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new WPRDeserializationStream(s)
  }
}

class WPRSerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)

  def writeObject[T](t: T): Unit = t match {
    case (id: String, wrapper: ArrayBuffer[_]) => wrapper(0) match {
      case links: Array[String] => {
        dos.writeInt(0) // links
        dos.writeUTF(id)
        dos.writeInt(links.length)
        for (link <- links) {
          dos.writeUTF(link)
        }
      }
      case rank: Double => {
        dos.writeInt(1) // rank
        dos.writeUTF(id)
        dos.writeDouble(rank)
      }
    }
    case (id: String, rank: Double) => {
      dos.writeInt(2) // rank without wrapper
      dos.writeUTF(id)
      dos.writeDouble(rank)
    }
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class WPRDeserializationStream(is: InputStream) extends DeserializationStream {
  val dis = new DataInputStream(is)

  def readObject[T](): T = {
    val typeId = dis.readInt()
    typeId match {
      case 0 => {
        val id = dis.readUTF()
        val numLinks = dis.readInt()
        val links = new Array[String](numLinks)
        for (i <- 0 until numLinks) {
          val link = dis.readUTF()
          links(i) = link
        }
        (id, ArrayBuffer(links)).asInstanceOf[T]
      }
      case 1 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, ArrayBuffer(rank)).asInstanceOf[T]
      }
      case 2 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, rank).asInstanceOf[T]
     }
    }
  }

  def close() { dis.close() }
}
