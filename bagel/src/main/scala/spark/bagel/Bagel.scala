package spark.bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

object Bagel extends Logging {
  def runWithAggregator[I : Manifest, V <: Vertex[I] : Manifest, M <: Message[I] : Manifest, C : Manifest, A : Manifest](
    sc: SparkContext,
    verts: RDD[(I, V)],
    msgs: RDD[(I, M)],
    aggregator: Option[Aggregator[V, A]],
    compute: (V, Option[C], Option[A], Int) => (V, Iterable[M]),
    combiner: Combiner[M, C] = new DefaultCombiner[M],
    superstep: Int = 0,
    numSplits: Int = 0
  ): RDD[V] = {
    val splits = if (numSplits != 0) numSplits else sc.defaultParallelism

    logInfo("Starting superstep "+superstep+".")
    val startTime = System.currentTimeMillis

    val aggregated = agg(verts, aggregator)
    val combinedMsgs = msgs.combineByKey(combiner.createCombiner, combiner.mergeMsg, combiner.mergeCombiners, splits)
    println("partitioner: " + combinedMsgs.partitioner)
    val grouped = verts.groupWith(combinedMsgs)
    val (processed, numMsgs, numActiveVerts) = comp[I, V, M, C](sc, grouped, compute(_, _, aggregated, superstep))

    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Superstep %d took %d s".format(superstep, timeTaken / 1000))

    // Check stopping condition and iterate
    val noActivity = numMsgs == 0 && numActiveVerts == 0
    if (noActivity) {
      processed.map { case (id, (vert, msgs)) => vert }
    } else {
      val newVerts = processed.mapValues { case (vert, msgs) => vert }
      val newMsgs = processed.flatMap {
        case (id, (vert, msgs)) => msgs.map(m => (m.targetId, m))
      }
      runWithAggregator[I, V, M, C, A](sc, newVerts, newMsgs, aggregator, compute, combiner, superstep + 1, splits)
    }
  }

  def run[I : Manifest, V <: Vertex[I] : Manifest, M <: Message[I] : Manifest, C : Manifest](
    sc: SparkContext,
    verts: RDD[(I, V)],
    msgs: RDD[(I, M)],
    compute: (V, Option[C], Int) => (V, Iterable[M]),
    combiner: Combiner[M, C] = new DefaultCombiner[M],
    superstep: Int = 0,
    numSplits: Int = 0
  ): RDD[V] = {
    runWithAggregator[I, V, M, C, Nothing](sc, verts, msgs, None, addAggregatorArg(compute), combiner, superstep, numSplits)
  }

  /**
   * Aggregates the given vertices using the given aggregator, if it
   * is specified.
   */
  def agg[I, V <: Vertex[I], A : Manifest](verts: RDD[(I, V)], aggregator: Option[Aggregator[V, A]]): Option[A] = aggregator match {
    case Some(a) =>
      Some(verts.map {
        case (id, vert) => a.createAggregator(vert)
      }.reduce(a.mergeAggregators(_, _)))
    case None => None
  }

  /**
   * Processes the given vertex-message RDD using the compute
   * function. Returns the processed RDD, the number of messages
   * created, and the number of active vertices.
   */
  def comp[I : Manifest, V <: Vertex[I], M <: Message[I], C](sc: SparkContext, grouped: RDD[(I, (Seq[V], Seq[C]))], compute: (V, Option[C]) => (V, Iterable[M])): (RDD[(I, (V, Iterable[M]))], Int, Int) = {
    var numMsgs = sc.accumulator(0)
    var numActiveVerts = sc.accumulator(0)
    val processed = grouped.flatMapValues {
      case (Seq(), _) => None
      case (Seq(v), c) =>
          val (newVert, newMsgs) =
            compute(v, c match {
              case Seq(comb) => Some(comb)
              case Seq() => None
            })

          numMsgs += newMsgs.size
          if (newVert.active)
            numActiveVerts += 1

          Some((newVert, newMsgs))
    }.cache

    // Force evaluation of processed RDD for accurate performance measurements
    processed.foreach(x => {})

    (processed, numMsgs.value, numActiveVerts.value)
  }

  /**
   * Converts a compute function that doesn't take an aggregator to
   * one that does, so it can be passed to Bagel.run.
   */
  implicit def addAggregatorArg[
    I, V <: Vertex[I] : Manifest, M <: Message[I] : Manifest, C
  ](
    compute: (V, Option[C], Int) => (V, Iterable[M])
  ): (V, Option[C], Option[Nothing], Int) => (V, Iterable[M]) = {
    (vert: V, messages: Option[C], aggregator: Option[Nothing], superstep: Int) => compute(vert, messages, superstep)
  }
}

// TODO: Simplify Combiner interface and make it more OO.
trait Combiner[M, C] {
  def createCombiner(msg: M): C
  def mergeMsg(combiner: C, msg: M): C
  def mergeCombiners(a: C, b: C): C
}

trait Aggregator[V, A] {
  def createAggregator(vert: V): A
  def mergeAggregators(a: A, b: A): A
}

class DefaultCombiner[M] extends Combiner[M, ArrayBuffer[M]] with Serializable {
  def createCombiner(msg: M): ArrayBuffer[M] =
    ArrayBuffer(msg)
  def mergeMsg(combiner: ArrayBuffer[M], msg: M): ArrayBuffer[M] =
    combiner += msg
  def mergeCombiners(a: ArrayBuffer[M], b: ArrayBuffer[M]): ArrayBuffer[M] =
    a ++= b
}

/**
 * Represents a Bagel vertex.
 *
 * Subclasses may store state along with each vertex and must
 * inherit from java.io.Serializable or scala.Serializable.
 */
trait Vertex[A] {
  def id: A
  def active: Boolean
}

/**
 * Represents a Bagel message to a target vertex.
 *
 * Subclasses may contain a payload to deliver to the target vertex
 * and must inherit from java.io.Serializable or scala.Serializable.
 */
trait Message[A] {
  def targetId: A
}

/**
 * Represents a directed edge between two vertices.
 *
 * Subclasses may store state along each edge and must inherit from
 * java.io.Serializable or scala.Serializable.
 */
trait Edge[A] {
  def targetId: A
}
