package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}

object WebPageRankStandalone {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WebPageRankStandalone <inputFile> <threshold> <numIterations> <numSplits> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.PRSASerializer")
    System.setProperty("spark.kryo.registrator", classOf[WGKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val numSplits = args(3).toInt
    val host = args(4)
    val usePartitioner = args(5).toBoolean
    val sc = new SparkContext(host, "WebPageRankStandalone")

    val startTime = System.currentTimeMillis

    val vertices = sc.objectFile[(Long, PRVertex)](inputFile, numSplits).cache()

    val n = vertices.count()
    val defaultRank = 1.0 / n
    val a = 0.15
    val links = vertices.map { case (id, v) => (id, v.outEdges) }.cache()
    var ranks = vertices.map { case (id, v) => (id, v.value) }
    val partitioner =
      if (usePartitioner) new CustomPartitioner(numSplits)
      else new HashPartitioner(numSplits)

    // Do the computation
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (Seq(links), Seq(rank))) =>
          links.map(dest => (dest, rank / links.size))
        case (id, (Seq(links), Seq())) =>
          links.map(dest => (dest, defaultRank / links.size))

      }
      val combine = (x: Double, y: Double) => x + y
      ranks = (contribs.combineByKey(x => x, combine, combine, numSplits, partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }

    // Print the result
    System.err.println("Pages with PageRank >= " + threshold + ":")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) =>
         "%s\t%s\n".format(id, rank) }
       .collect.mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }
}

class PRSASerializer extends spark.Serializer {
  def newInstance(): SerializerInstance = new PRSASerializerInstance()
}

class PRSASerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    throw new UnsupportedOperationException()
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new PRSASerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new PRSADeserializationStream(s)
  }
}

class PRSASerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)

  def writeObject[T](t: T): Unit = t match {
    case (id: Long, wrapper: ArrayBuffer[_]) => wrapper(0) match {
      case links: Array[Long] => {
        dos.writeInt(0) // links
        dos.writeLong(id)
        dos.writeInt(links.length)
        for (link <- links) dos.writeLong(link)
      }
      case rank: Double => {
        dos.writeInt(1) // rank
        dos.writeLong(id)
        dos.writeDouble(rank)
      }
    }
    case (id: Long, rank: Double) => {
      dos.writeInt(2) // rank without wrapper
      dos.writeLong(id)
      dos.writeDouble(rank)
    }
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class PRSADeserializationStream(is: InputStream) extends DeserializationStream {
  val dis = new DataInputStream(is)

  def readObject[T](): T = {
    val typeId = dis.readInt()
    typeId match {
      case 0 => {
        val id = dis.readLong()
        val numLinks = dis.readInt()
        val links = new Array[Long](numLinks)
        for (i <- 0 until numLinks) links(i) = dis.readLong()
        (id, ArrayBuffer(links)).asInstanceOf[T]
      }
      case 1 => {
        val id = dis.readLong()
        val rank = dis.readDouble()
        (id, ArrayBuffer(rank)).asInstanceOf[T]
      }
      case 2 => {
        val id = dis.readLong()
        val rank = dis.readDouble()
        (id, rank).asInstanceOf[T]
     }
    }
  }

  def close() { dis.close() }
}
