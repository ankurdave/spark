/*package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

object WebPageRank {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WebPageRank <inputFile> <threshold> <numSplits> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.PRSerializer")
    System.setProperty("spark.kryo.registrator", classOf[WGKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WebPageRank")

    val vertices = sc.objectFile[(Long, PRVertex)](inputFile, numSplits).cache

    println("Counting vertices...")
    val numVertices = vertices.count()
    println("Done counting vertices: " + numVertices)

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(Long, PRMessage)]())
    val util = new PageRankUtils
    val partitioner =
      if (usePartitioner) new CustomPartitioner(numSplits)
      else new HashPartitioner(numSplits)
    val result =
      Bagel.run(
        sc, vertices, messages, combiner = new PRCombiner(),
        partitioner = partitioner, numSplits = numSplits)(
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

class PRSerializer extends spark.Serializer {
  def newInstance(): SerializerInstance = new PRSerializerInstance()
}

class PRSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    throw new UnsupportedOperationException()
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new PRSerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new PRDeserializationStream(s)
  }
}

class PRSerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)

  def writeObject[T](t: T): Unit = t match {
    case (id: Long, vs: ArrayBuffer[_]) => {
      dos.writeBoolean(false) // vertex
      dos.writeLong(id)
      val vert = vs(0).asInstanceOf[PRVertex] // assume 1 vertex
      dos.writeDouble(vert.value)
      dos.writeBoolean(vert.active)
      dos.writeInt(vert.outEdges.length)
      for (edge <- vert.outEdges) {
        dos.writeLong(edge)
      }
    }
    case (targetId: Long, value: Double) => {
      dos.writeBoolean(true) // message
      dos.writeLong(targetId)
      dos.writeDouble(value)
    }
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class PRDeserializationStream(is: InputStream) extends DeserializationStream {
  val dis = new DataInputStream(is)

  def readObject[T](): T = {
    val isMessage = dis.readBoolean()
    if (isMessage) {
      val targetId = dis.readLong()
      val value = dis.readDouble()

      (targetId, value).asInstanceOf[T]
    } else {
      val id = dis.readLong()
      val value = dis.readDouble()
      val active = dis.readBoolean()
      val numEdges = dis.readInt()
      val outEdges = new Array[Long](numEdges)
      for (i <- 0 until numEdges) {
        outEdges(i) = dis.readLong()
      }

      (id, ArrayBuffer(new PRVertex(value, outEdges, active))).asInstanceOf[T]
    }
  }

  def close() { dis.close() }
}
*/
