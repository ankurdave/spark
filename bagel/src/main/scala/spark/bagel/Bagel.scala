package spark.bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

object Bagel extends Logging {
  def run[I : Manifest, V <: Vertex : Manifest, M <: Message[I] : Manifest,
          C : Manifest, A : Manifest](
    sc: SparkContext,
    vertices: RDD[(I, V)],
    messages: RDD[(I, M)],
    combiner: Combiner[M, C],
    aggregator: Option[Aggregator[V, A]],
    partitioner: Partitioner,
    numSplits: Int
  )(
    compute: (V, Option[C], Option[A], Int) => (V, Array[M])
  ): RDD[(I, V)] = {
    val splits = if (numSplits != 0) numSplits else sc.defaultParallelism

    var superstep = 0
    var verts = vertices
    var msgs = messages
    var noActivity = false
    do {
      logInfo("Starting superstep "+superstep+".")
      val startTime = System.currentTimeMillis

      val aggregated = agg(verts, aggregator)
      val combinedMsgs = msgs.combineByKey(
        combiner.createCombiner, combiner.mergeMsg, combiner.mergeCombiners,
        splits, partitioner)
      println("partitioner: " + combinedMsgs.partitioner)
      val grouped = verts.groupWith(combinedMsgs)
      val (processed, numMsgs, numActiveVerts) =
        comp[I, V, M, C](sc, grouped, compute(_, _, aggregated, superstep))

      val timeTaken = System.currentTimeMillis - startTime
      logInfo("Superstep %d took %d s".format(superstep, timeTaken / 1000))

      verts = processed.mapValues { case (vert, msgs) => vert }
      msgs = processed.flatMap {
        case (id, (vert, msgs)) => msgs.map(m => (m.targetId, m))
      }
      superstep += 1

      noActivity = numMsgs == 0 && numActiveVerts == 0
    } while (!noActivity)

    verts
  }

  def run[I : Manifest, V <: Vertex : Manifest, M <: Message[I] : Manifest,
          C : Manifest](
    sc: SparkContext,
    vertices: RDD[(I, V)],
    messages: RDD[(I, M)],
    combiner: Combiner[M, C],
    partitioner: Partitioner,
    numSplits: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(I, V)] = {
    run[I, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, partitioner, numSplits)(
      addAggregatorArg[I, V, M, C](compute))
  }

  def run[I : Manifest, V <: Vertex : Manifest, M <: Message[I] : Manifest,
          C : Manifest](
    sc: SparkContext,
    vertices: RDD[(I, V)],
    messages: RDD[(I, M)],
    combiner: Combiner[M, C],
    numSplits: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(I, V)] = {
    val part = new HashPartitioner(numSplits)
    run[I, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, part, numSplits)(
      addAggregatorArg[I, V, M, C](compute))
  }

  def run[I : Manifest, V <: Vertex : Manifest, M <: Message[I] : Manifest](
    sc: SparkContext,
    vertices: RDD[(I, V)],
    messages: RDD[(I, M)],
    numSplits: Int
  )(
    compute: (V, Option[Array[M]], Int) => (V, Array[M])
  ): RDD[(I, V)] = {
    val part = new HashPartitioner(numSplits)
    run[I, V, M, Array[M], Nothing](
      sc, vertices, messages, new DefaultCombiner(), None, part, numSplits)(
      addAggregatorArg[I, V, M, Array[M]](compute))
  }

  /**
   * Aggregates the given vertices using the given aggregator, if it
   * is specified.
   */
  private def agg[I, V <: Vertex, A : Manifest](
    verts: RDD[(I, V)],
    aggregator: Option[Aggregator[V, A]]
  ): Option[A] = aggregator match {
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
  private def comp[I : Manifest, V <: Vertex, M <: Message[I], C](
    sc: SparkContext,
    grouped: RDD[(I, (Seq[V], Seq[C]))],
    compute: (V, Option[C]) => (V, Array[M])
  ): (RDD[(I, (V, Array[M]))], Int, Int) = {
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

      case (v, c) =>
        logError("Multiple vertices had the same ID")
        for (vert <- v) {
          logInfo(vert.toString)
        }
        throw new Exception()

    }.cache

    // Force evaluation of processed RDD for accurate performance measurements
    processed.foreach(x => {})

    (processed, numMsgs.value, numActiveVerts.value)
  }

  /**
   * Converts a compute function that doesn't take an aggregator to
   * one that does, so it can be passed to Bagel.run.
   */
  private def addAggregatorArg[
    I, V <: Vertex : Manifest, M <: Message[I] : Manifest, C
  ](
    compute: (V, Option[C], Int) => (V, Array[M])
  ): (V, Option[C], Option[Nothing], Int) => (V, Array[M]) = {
    (vert: V, msgs: Option[C], aggregated: Option[Nothing], superstep: Int) =>
      compute(vert, msgs, superstep)
  }
}

trait Combiner[M, C] {
  def createCombiner(msg: M): C
  def mergeMsg(combiner: C, msg: M): C
  def mergeCombiners(a: C, b: C): C
}

trait Aggregator[V, A] {
  def createAggregator(vert: V): A
  def mergeAggregators(a: A, b: A): A
}

class DefaultCombiner[M : Manifest] extends Combiner[M, Array[M]] with Serializable {
  def createCombiner(msg: M): Array[M] =
    Array(msg)
  def mergeMsg(combiner: Array[M], msg: M): Array[M] =
    combiner :+ msg
  def mergeCombiners(a: Array[M], b: Array[M]): Array[M] =
    a ++ b
}

/**
 * Represents a Bagel vertex.
 *
 * Subclasses may store state along with each vertex and must
 * inherit from java.io.Serializable or scala.Serializable.
 */
trait Vertex {
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
