package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}

import com.esotericsoftware.kryo._

class PageRankUtils extends Serializable {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(
    self: PRVertex, messageSum: Option[Double], superstep: Int
  ): (PRVertex, Array[PRMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15 / numVertices + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = superstep >= 2

    val outbox: Array[PRMessage] =
      if (!terminate)
        self.outEdges.map(targetId =>
          new PRMessage(targetId, newValue / self.outEdges.size))
      else
        Array[PRMessage]()

    (new PRVertex(newValue, self.outEdges, !terminate), outbox)
  }

  def computeNoCombiner(numVertices: Long, epsilon: Double)(self: PRVertex, messages: Option[Array[PRMessage]], superstep: Int): (PRVertex, Array[PRMessage]) =
    computeWithCombiner(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, superstep)
}

class PRCombiner extends Combiner[PRMessage, Double] with Serializable {
  def createCombiner(msg: PRMessage): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PRVertex() extends Vertex with Serializable {
  var value: Double = _
  var outEdges: Array[Long] = _
  var active: Boolean = _

  def this(value: Double, outEdges: Array[Long], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "PRVertex(value=%f, outEdges.length=%d, active=%s)".format(value, outEdges.length, active.toString)
  }
}

class PRMessage() extends Message with Serializable {
  var targetId: Long = _
  var value: Double = _

  def this(targetId: Long, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
  }
}

class WGKryoRegistrator extends KryoRegistrator {
  def registerClasses(k: Kryo) {
    k.register(classOf[PRVertex])
    k.register(classOf[PRMessage])
    k.register(classOf[Tuple2[Long, PRVertex]])
    k.register(classOf[Array[Tuple2[Long, PRVertex]]])
    k.register(classOf[scala.collection.mutable.ArraySeq[Int]])
  }
}

class CustomPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    val hash = key match {
      case k: Long => (k & 0x00000000FFFFFFFFL).toInt
      case _ => key.hashCode
    }

    val mod = key.hashCode % partitions
    if (mod < 0) mod + partitions else mod
  }

  override def equals(other: Any): Boolean = other match {
    case c: CustomPartitioner =>
      c.numPartitions == numPartitions
    case _ => false
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
