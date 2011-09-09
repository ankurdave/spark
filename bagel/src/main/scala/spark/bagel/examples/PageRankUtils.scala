package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

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
    "PRVertex(value=%f, active=%s)".format(value, active.toString)
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
