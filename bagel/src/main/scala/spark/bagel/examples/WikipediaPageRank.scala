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
    if (args.length < 6) {
      System.err.println("Usage: WikipediaPageRank <inputFile> <threshold> <numIterations> <host> <usePartitioner> <useBagel>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val useBagel = args(5).toBoolean
    if (useBagel) {
      System.setProperty("spark.serializer", "spark.KryoSerializer")
      System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)
    } else {
      System.setProperty("spark.serializer", "spark.bagel.examples.WPRSerializer")
    }

    val sc = new SparkContext(host, "WikipediaPageRank")


    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)

    // Do the computation
    val startTime = System.currentTimeMillis
    val ranks =
      if (useBagel)
        pageRankBagel(input, numIterations, partitioner, usePartitioner, sc)
      else
        pageRankStandalone(input, numIterations, partitioner, usePartitioner, sc)

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

  def parseArticle[V](line: String, buildVal: Array[String] => V): (String, V) = {
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
    (id, buildVal(outEdges))
  }

  def pageRankStandalone(
    input: RDD[String],
    numIterations: Int,
    partitioner: Partitioner,
    usePartitioner: Boolean,
    sc: SparkContext
  ): RDD[(String, Double)] = {
    val links =
      if (usePartitioner)
        input.map(article => parseArticle(article, links => links)).partitionBy(partitioner).cache
      else
        input.map(article => parseArticle(article, links => links)).cache
    val n = links.count
    val defaultRank = 1.0 / n
    val a = 0.15
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (linksWrapper, rankWrapper)) =>
          if (linksWrapper.length > 0) {
            if (rankWrapper.length > 0) {
              linksWrapper(0).map(dest => (dest, rankWrapper(0) / linksWrapper(0).size))
            } else {
              linksWrapper(0).map(dest => (dest, defaultRank / linksWrapper(0).size))
            }
          } else {
            Array[(String, Double)]()
          }
      }
      ranks = (contribs.combineByKey((x: Double) => x,
                                     (x: Double, y: Double) => x + y,
                                     (x: Double, y: Double) => x + y,
                                     sc.defaultParallelism,
                                     partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }
    ranks
  }

  def pageRankBagel(
    input: RDD[String],
    numIterations: Int,
    partitioner: Partitioner,
    usePartitioner: Boolean,
    sc: SparkContext
  ): RDD[(String, Double)] = {
    val n = input.count
    val defaultRank = 1.0 / n
    val a = 0.15
    val vertices =
      if (usePartitioner)
        (input.map(article => parseArticle(article, links => new PRVertex(defaultRank, links)))
         .partitionBy(partitioner)
         .cache)
      else
        (input.map(article => parseArticle(article, links => new PRVertex(defaultRank, links)))
         .cache)
    val messages = sc.parallelize(Array[(String, Double)]())
    val finalVerts = Bagel.run[String, PRVertex, Double, Double](sc, vertices, messages, combiner = new PRCombiner(), partitioner = partitioner, numSplits = sc.defaultParallelism)(compute(a, n, numIterations) _)
    finalVerts.mapValues(vert => vert.value)
  }

  def compute(a: Double, n: Long, numIterations: Int)(self: PRVertex, messageSum: Option[Double], superstep: Int): (PRVertex, Array[(String, Double)]) = {
    val newValue = messageSum match {
      case Some(sum) if sum != 0 => a/n + (1-a)*sum
      case _ => self.value
    }
    val terminate = superstep >= numIterations
    val outbox: Array[(String, Double)] =
      if (!terminate)
        self.outEdges.map(targetId => (targetId, newValue / self.outEdges.size))
      else
        Array[(String, Double)]()
    (new PRVertex(newValue, self.outEdges, !terminate), outbox)
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

class WPRBSerializer extends spark.Serializer {
  def newInstance(): SerializerInstance = new WPRBSerializerInstance()
}

class WPRBSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    throw new UnsupportedOperationException()
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new WPRBSerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new WPRBDeserializationStream(s)
  }
}

class WPRBSerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)

  def writeObject[T](t: T): Unit = t match {
    case (id: String, wrapper: ArrayBuffer[_]) => wrapper(0) match {
      case vert: PRVertex => {
        dos.writeInt(0) // vertex
        dos.writeUTF(id)
        dos.writeDouble(vert.value)
        dos.writeBoolean(vert.active)
        dos.writeInt(vert.outEdges.length)
        for (edge <- vert.outEdges) {
          dos.writeUTF(edge)
        }
      }
      case msg: Double => {
        dos.writeInt(1) // message
        dos.writeUTF(id)
        dos.writeDouble(msg)
      }
    }
    case (id: String, msg: Double) => {
      dos.writeInt(2) // message without wrapper
      dos.writeUTF(id)
      dos.writeDouble(msg)
    }
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class WPRBDeserializationStream(is: InputStream) extends DeserializationStream {
  val dis = new DataInputStream(is)

  def readObject[T](): T = {
    val typeId = dis.readInt()
    typeId match {
      case 0 => {
        val id = dis.readUTF()
        val value = dis.readDouble()
        val active = dis.readBoolean()
        val numEdges = dis.readInt()
        val edges = new Array[String](numEdges)
        for (i <- 0 until numEdges) {
          val edge = dis.readUTF()
          edges(i) = edge
        }
        (id, ArrayBuffer(new PRVertex(value, edges, active))).asInstanceOf[T]
      }
      case 1 => {
        val id = dis.readUTF()
        val msg = dis.readDouble()
        (id, ArrayBuffer(msg)).asInstanceOf[T]
      }
      case 2 => {
        val id = dis.readUTF()
        val msg = dis.readDouble()
        (id, msg).asInstanceOf[T]
     }
    }
  }

  def close() { dis.close() }
}
