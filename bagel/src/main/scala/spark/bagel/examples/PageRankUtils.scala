package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

import com.esotericsoftware.kryo._

@serializable
class PRCombiner[A] extends Combiner[PRMessage[A], Double] {
  def createCombiner(msg: PRMessage[A]): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage[A]): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PageRankUtils[A] {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(self: PRVertex[A], messageSum: Option[Double], aggregated: Option[Nothing], superstep: Int): (PRVertex[A], Iterable[PRMessage[A]]) = {
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

  def computeNoCombiner(numVertices: Long, epsilon: Double)(self: PRVertex[A], messages: Option[ArrayBuffer[PRMessage[A]]], aggregated: Option[Nothing], superstep: Int): (PRVertex[A], Iterable[PRMessage[A]]) =
    computeWithCombiner(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, aggregated, superstep)
}

@serializable class PRVertex[A]() extends Vertex[A] {
  var id: A = _
  var value: Double = _
  var outEdges: ArrayBuffer[PREdge[A]] = _
  var active: Boolean = _
  var partition: Int = _

  def this(id: A, value: Double, outEdges: ArrayBuffer[PREdge[A]], active: Boolean = true, partition: Int = 0) {
    this()
    this.id = id
    this.value = value
    this.outEdges = outEdges
    this.active = active
    this.partition = partition
  }

  override def toString(): String = {
    "[%s,%s,%s,%s,%s]".format(
      id, value, active, partition, outEdges.mkString(" "))
  }
}

@serializable class PRMessage[A]() extends Message[A] {
  var targetId: A = _
  var value: Double = _

  def this(targetId: A, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

@serializable class PREdge[A]() extends Edge[A] {
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

