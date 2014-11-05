/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.impl

import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkEnv
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet

/**
 * A collection of edges along with any vertex attributes referenced, and an optional active vertex
 * set for filtering computation on the edges.
 *
 * @tparam ED the edge attribute type
 * @tparam VD the vertex attribute type
 */
private[graphx]
trait EdgePartition[ED, VD] extends Serializable {

  /** Return a new `EdgePartition` with the specified active set, provided as an iterator. */
  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD]

  /** Return a new `EdgePartition` with updates to vertex attributes specified in `iter`. */
  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD]

  /** Return a new `EdgePartition` without any locally cached vertex attributes. */
  def clearVertices[VD2: ClassTag](): EdgePartition[ED, VD2]

  /** Look up vid in the active set, throwing an exception if it is None. */
  def isActive(vid: VertexId): Boolean

  /** The number of active vertices, if any exist. */
  def numActives: Option[Int]

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: EdgePartition[ED, VD]

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * Be careful not to keep references to the objects passed to `f`.
   * To improve GC performance the same object is re-used for each call.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[ED2, VD]

  /**
   * Construct a new edge partition by using the edge attributes
   * contained in the iterator.
   *
   * @note The input iterator should return edge attributes in the
   * order of the edges returned by `EdgePartition.iterator` and
   * should return attributes equal to the number of edges.
   *
   * @param iter an iterator for the new attribute values
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the attribute values replaced
   */
  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD]

  /**
   * Construct a new edge partition containing only the edges matching `epred` and where both
   * vertices match `vpred`.
   */
  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD]

  /**
   * Apply the function f to all edges in this partition.
   *
   * @param f an external state mutating user defined function.
   */
  def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  /**
   * Merge all the edges with the same src and dest id into a single
   * edge using the `merge` function
   *
   * @param merge a commutative associative merge operation
   * @return a new edge partition without duplicate edges
   */
  def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD]

  /**
   * Apply `f` to all edges present in both `this` and `other` and return a new `EdgePartition`
   * containing the resulting edges.
   *
   * If there are multiple edges with the same src and dst in `this`, `f` will be invoked once for
   * each edge, but each time it may be invoked on any corresponding edge in `other`.
   *
   * If there are multiple edges with the same src and dst in `other`, `f` will only be invoked
   * once.
   */
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[ED3, VD]

  /**
   * The number of edges in this partition
   *
   * @return size of the partition
   */
  def size: Int

  /** The number of unique source vertices in the partition. */
  def indexSize: Int

  /**
   * Get an iterator over the edges in this partition.
   *
   * Be careful not to keep references to the objects from this iterator.
   * To improve GC performance the same object is re-used in `next()`.
   *
   * @return an iterator over edges in the partition
   */
  def iterator: Iterator[Edge[ED]]

  /**
   * Get an iterator over the edge triplets in this partition.
   *
   * It is safe to keep references to the objects from this iterator.
   */
  def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]]

  /**
   * Send messages along edges and aggregate them at the receiving vertices. Implemented by scanning
   * all edges sequentially and filtering them with `idPred`.
   *
   * @param sendMsg generates messages to neighboring vertices of an edge
   * @param mergeMsg the combiner applied to messages destined to the same vertex
   * @param sendMsgUsesSrcAttr whether or not `mapFunc` uses the edge's source vertex attribute
   * @param sendMsgUsesDstAttr whether or not `mapFunc` uses the edge's destination vertex attribute
   * @param idPred a predicate to filter edges based on their source and destination vertex ids
   *
   * @return iterator aggregated messages keyed by the receiving vertex id
   */
  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      idPred: (VertexId, VertexId) => Boolean): Iterator[(VertexId, A)]

  /**
   * Send messages along edges and aggregate them at the receiving vertices. Implemented by
   * filtering the source vertex index with `srcIdPred`, then scanning edge clusters and filtering
   * with `dstIdPred`. Both `srcIdPred` and `dstIdPred` must match for an edge to run.
   *
   * @param sendMsg generates messages to neighboring vertices of an edge
   * @param mergeMsg the combiner applied to messages destined to the same vertex
   * @param srcIdPred a predicate to filter edges based on their source vertex id
   * @param dstIdPred a predicate to filter edges based on their destination vertex id
   *
   * @return iterator aggregated messages keyed by the receiving vertex id
   */
  def aggregateMessagesWithIndex[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      srcIdPred: VertexId => Boolean,
      dstIdPred: VertexId => Boolean): Iterator[(VertexId, A)]
}

object EdgePartition {
  def newBuilder[ED: ClassTag, VD: ClassTag](onDisk: Boolean): EdgePartitionBuilder[ED, VD] = {
    if (onDisk) DiskEdgePartition.newBuilder[ED, VD]
    else MemoryEdgePartition.newBuilder[ED, VD]
  }
}

/** An EdgeContext that aggregates messages using an array indexed by local vertex id. */
private[impl] class AggregatingEdgeContext[VD, ED, A](
    mergeMsg: (A, A) => A,
    aggregates: Array[A],
    bitset: BitSet)
  extends EdgeContext[VD, ED, A] {

  var srcId: VertexId = _
  var dstId: VertexId = _
  var srcAttr: VD = _
  var dstAttr: VD = _
  var attr: ED = _

  var localSrcId: Int = _
  var localDstId: Int = _

  override def sendToSrc(msg: A) {
    send(localSrcId, msg)
  }
  override def sendToDst(msg: A) {
    send(localDstId, msg)
  }

  private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}

private[impl] class EdgeWithLocalIds[ED] extends Edge[ED] {
  var localSrcId = -1
  var localDstId = -1
  def set(e: EdgeWithLocalIds[ED]) {
    srcId = e.srcId
    dstId = e.dstId
    attr = e.attr
    localSrcId = e.localSrcId
    localDstId = e.localDstId
  }
}

private[impl] object EdgeWithLocalIds {
  implicit def lexicographicOrdering[ED] = new Ordering[EdgeWithLocalIds[ED]] {
    override def compare(a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]): Int = {
      if (a.srcId == b.srcId) {
        if (a.dstId == b.dstId) 0
        else if (a.dstId < b.dstId) -1
        else 1
      } else if (a.srcId < b.srcId) -1
      else 1
    }
  }
}
