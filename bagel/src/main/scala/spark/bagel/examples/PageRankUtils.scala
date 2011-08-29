package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

import com.esotericsoftware.kryo._

class PageRankUtils[A] extends Serializable {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(
    self: PRVertex[A], messageSum: Option[Double], superstep: Int
  ): (PRVertex[A], Iterable[PRMessage[A]]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15 / numVertices + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = (superstep >= 10 && (newValue - self.value).abs < epsilon) || superstep >= 30

    val outbox =
      if (!terminate)
        self.outEdges.map(edge =>
          new PRMessage(edge.targetId, newValue / self.outEdges.size))
      else
        ArrayBuffer[PRMessage[A]]()

    (new PRVertex(self.id, newValue, self.outEdges, !terminate), outbox)
  }

  def computeNoCombiner(numVertices: Long, epsilon: Double)(self: PRVertex[A], messages: Option[ArrayBuffer[PRMessage[A]]], superstep: Int): (PRVertex[A], Iterable[PRMessage[A]]) =
    computeWithCombiner(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, superstep)
}

class PRCombiner[A] extends Combiner[PRMessage[A], Double] with Serializable {
  def createCombiner(msg: PRMessage[A]): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage[A]): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PRVertex[A]() extends Vertex[A] with Serializable {
  var id: A = _
  var value: Double = _
  var outEdges: ArrayBuffer[PREdge[A]] = _
  var active: Boolean = _

  def this(id: A, value: Double, outEdges: ArrayBuffer[PREdge[A]], active: Boolean = true) {
    this()
    this.id = id
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "%s,%s,%s,%s".format(
      id, value, active, outEdges.mkString(","))
  }
}

class PRMessage[A]() extends Message[A] with Serializable {
  var targetId: A = _
  var value: Double = _

  def this(targetId: A, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

class PREdge[A]() extends Edge[A] with Serializable {
  var targetId: A = _

  def this(targetId: A) {
    this()
    this.targetId = targetId
  }

  override def toString(): String = {
    targetId.toString()
  }
}

class PRKryoRegistrator[A] extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex[A]])
    kryo.register(classOf[PRMessage[A]])
    kryo.register(classOf[PREdge[A]])
  }
}

class CustomPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any) = {
    val hash = key match {
      case (id: Int, partition: Int) => partition
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
