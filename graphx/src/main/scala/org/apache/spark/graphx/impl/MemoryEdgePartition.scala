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

import scala.reflect.{classTag, ClassTag}

import scala.util.Sorting
import org.apache.spark.SparkEnv

import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector, ExternalSorter}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet

/**
 * A collection of edges stored in memory in columnar format, along with any vertex attributes
 * referenced. The edges are stored in 3 large columnar arrays (src, dst, attribute). The arrays are
 * clustered by src. There is an optional active vertex set for filtering computation on the edges.
 *
 * @tparam ED the edge attribute type
 * @tparam VD the vertex attribute type
 *
 * @param localSrcIds the local source vertex id of each edge as an index into `local2global` and
 *   `vertexAttrs`
 * @param localDstIds the local destination vertex id of each edge as an index into `local2global`
 *   and `vertexAttrs`
 * @param data the attribute associated with each edge
 * @param index a clustered index on source vertex id as a map from each global source vertex id to
 *   the offset in the edge arrays where the cluster for that vertex id begins
 * @param global2local a map from referenced vertex ids to local ids which index into vertexAttrs
 * @param local2global an array of global vertex ids where the offsets are local vertex ids
 * @param vertexAttrs an array of vertex attributes where the offsets are local vertex ids
 * @param activeSet an optional active vertex set for filtering computation on the edges
 */
private[impl]
class MemoryEdgePartition[
    @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
    val localSrcIds: Array[Int] = null,
    val localDstIds: Array[Int] = null,
    val data: Array[ED] = null,
    val index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int] = null,
    val global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int] = null,
    val local2global: Array[VertexId] = null,
    val vertexAttrs: Array[VD] = null,
    val activeSet: Option[VertexSet] = None)
  extends EdgePartition[ED, VD] {

  /** Return a new `EdgePartition` with the specified edge data. */
  def withData[ED2: ClassTag](data: Array[ED2]): EdgePartition[ED2, VD] = {
    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
  }

  override def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val activeSet = new VertexSet
    while (iter.hasNext) { activeSet.add(iter.next()) }
    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs,
      Some(activeSet))
  }

  override def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      newVertexAttrs(global2local(kv._1)) = kv._2
    }
    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
  }

  override def clearVertices[VD2: ClassTag](): EdgePartition[ED, VD2] = {
    val newVertexAttrs = new Array[VD2](vertexAttrs.length)
    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
  }

  private def srcIds(pos: Int): VertexId = local2global(localSrcIds(pos))

  private def dstIds(pos: Int): VertexId = local2global(localDstIds(pos))

  override def isActive(vid: VertexId): Boolean = {
    activeSet.get.contains(vid)
  }

  override def numActives: Option[Int] = activeSet.map(_.size)

  override def reverse: EdgePartition[ED, VD] = {
    val builder = MemoryEdgePartition.fromExisting[ED, VD](this, size)
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val srcId = local2global(localSrcId)
      val dstId = local2global(localDstId)
      val attr = data(i)
      builder.add(dstId, srcId, localDstId, localSrcId, attr)
      i += 1
    }
    builder.toEdgePartition
  }

  override def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[ED2, VD] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    val size = data.size
    var i = 0
    while (i < size) {
      edge.srcId  = srcIds(i)
      edge.dstId  = dstIds(i)
      edge.attr = data(i)
      newData(i) = f(edge)
      i += 1
    }
    this.withData(newData)
  }

  override def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[ED2](data.size)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.size == i)
    this.withData(newData)
  }

  override def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD] = {
    val builder = MemoryEdgePartition.fromExisting[ED, VD](this)
    var i = 0
    while (i < size) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val et = new EdgeTriplet[VD, ED]
      et.srcId = local2global(localSrcId)
      et.dstId = local2global(localDstId)
      et.srcAttr = vertexAttrs(localSrcId)
      et.dstAttr = vertexAttrs(localDstId)
      et.attr = data(i)
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) {
        builder.add(et.srcId, et.dstId, localSrcId, localDstId, et.attr)
      }
      i += 1
    }
    builder.toEdgePartition
  }

  override def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  override def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD] = {
    val builder = MemoryEdgePartition.fromExisting[ED, VD](this)
    var currSrcId: VertexId = null.asInstanceOf[VertexId]
    var currDstId: VertexId = null.asInstanceOf[VertexId]
    var currLocalSrcId = -1
    var currLocalDstId = -1
    var currAttr: ED = null.asInstanceOf[ED]
    // Iterate through the edges, accumulating runs of identical edges using the curr* variables and
    // releasing them to the builder when we see the beginning of the next run
    var i = 0
    while (i < size) {
      if (i > 0 && currSrcId == srcIds(i) && currDstId == dstIds(i)) {
        // This edge should be accumulated into the existing run
        currAttr = merge(currAttr, data(i))
      } else {
        // This edge starts a new run of edges
        if (i > 0) {
          // First release the existing run to the builder
          builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
        }
        // Then start accumulating for a new run
        currSrcId = srcIds(i)
        currDstId = dstIds(i)
        currLocalSrcId = localSrcIds(i)
        currLocalDstId = localDstIds(i)
        currAttr = data(i)
      }
      i += 1
    }
    // Finally, release the last accumulated run
    if (size > 0) {
      builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
    }
    builder.toEdgePartition
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[ED3, VD] = {
    val builder = MemoryEdgePartition.fromExisting[ED3, VD](this)
    // Walk the two iterators forward until they match
    val thisIter = localIdIterator
    val otherIter = other.iterator
    if (thisIter.hasNext && otherIter.hasNext) {
      var thisE = thisIter.next()
      var otherE = otherIter.next()
      var stop = false
      while (!stop) {
        if (thisE.srcId < otherE.srcId) {
          if (thisIter.hasNext) { thisE = thisIter.next() } else { stop = true }
        } else if (otherE.srcId < thisE.srcId) {
          if (otherIter.hasNext) { otherE = otherIter.next() } else { stop = true }
        } else {
          if (thisE.dstId < otherE.dstId) {
            if (thisIter.hasNext) { thisE = thisIter.next() } else { stop = true }
          } else if (otherE.dstId < thisE.dstId) {
            if (otherIter.hasNext) { otherE = otherIter.next() } else { stop = true }
          } else {
            builder.add(thisE.srcId, thisE.dstId, thisE.localSrcId, thisE.localDstId,
              f(thisE.srcId, thisE.dstId, thisE.attr, otherE.attr))
            if (thisIter.hasNext) { thisE = thisIter.next() } else { stop = true }
            if (otherIter.hasNext) { otherE = otherIter.next() } else { stop = true }
          }
        }
      }
    }
    builder.toEdgePartition
  }

  override def size: Int = localSrcIds.size

  override def indexSize: Int = index.size

  override def iterator = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < MemoryEdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  private def localIdIterator = new Iterator[EdgeWithLocalIds[ED]] {
    private[this] val edge = new EdgeWithLocalIds[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < MemoryEdgePartition.this.size

    override def next(): EdgeWithLocalIds[ED] = {
      edge.localSrcId = localSrcIds(pos)
      edge.localDstId = localDstIds(pos)
      edge.srcId = local2global(edge.localSrcId)
      edge.dstId = local2global(edge.localDstId)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  override def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]] = {
    new EdgeTripletIterator(this, includeSrc, includeDst)
  }

  override def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      idPred: (VertexId, VertexId) => Boolean): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val srcId = local2global(localSrcId)
      val localDstId = localDstIds(i)
      val dstId = local2global(localDstId)
      if (idPred(srcId, dstId)) {
        ctx.localSrcId = localSrcId
        ctx.localDstId = localDstId
        ctx.srcId = srcId
        ctx.dstId = dstId
        ctx.attr = data(i)
        if (tripletFields.useSrc) { ctx.srcAttr = vertexAttrs(localSrcId) }
        if (tripletFields.useDst) { ctx.dstAttr = vertexAttrs(localDstId) }
        sendMsg(ctx)
      }
      i += 1
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }

  override def aggregateMessagesWithIndex[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      srcIdPred: VertexId => Boolean,
      dstIdPred: VertexId => Boolean): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    index.iterator.foreach { cluster =>
      val clusterSrcId = cluster._1
      val clusterPos = cluster._2
      val clusterLocalSrcId = localSrcIds(clusterPos)
      if (srcIdPred(clusterSrcId)) {
        var pos = clusterPos
        ctx.srcId = clusterSrcId
        ctx.localSrcId = clusterLocalSrcId
        if (tripletFields.useSrc) { ctx.srcAttr = vertexAttrs(clusterLocalSrcId) }
        while (pos < size && localSrcIds(pos) == clusterLocalSrcId) {
          val localDstId = localDstIds(pos)
          val dstId = local2global(localDstId)
          if (dstIdPred(dstId)) {
            ctx.dstId = dstId
            ctx.localDstId = localDstId
            ctx.attr = data(pos)
            if (tripletFields.useDst) { ctx.dstAttr = vertexAttrs(localDstId) }
            sendMsg(ctx)
          }
          pos += 1
        }
      }
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }
}

private[impl] object MemoryEdgePartition {
  private def fromExisting[ED: ClassTag, VD: ClassTag](
      ep: MemoryEdgePartition[_, VD], size: Int = 64) = {
    new ExistingMemoryEdgePartitionBuilder[ED, VD](
      ep.global2local, ep.local2global, ep.vertexAttrs, ep.activeSet, size)
  }

  def newBuilder[ED: ClassTag, VD: ClassTag]: EdgePartitionBuilder[ED, VD] = {
    new MemoryEdgePartitionBuilder[ED, VD]()
  }
}

private class MemoryEdgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](size: Int = 64)
  extends EdgePartitionBuilder[ED, VD] {

  private var edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    edges += Edge(src, dst, d)
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    Sorting.quickSort(edgeArray)(Edge.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index. Also populate a map from vertex id to a sequential local offset.
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var i = 0
      while (i < edgeArray.size) {
        val srcId = edgeArray(i).srcId
        val dstId = edgeArray(i).dstId
        localSrcIds(i) = global2local.changeValue(srcId,
          { currLocalId += 1; local2global += srcId; currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstId,
          { currLocalId += 1; local2global += dstId; currLocalId }, identity)
        data(i) = edgeArray(i).attr
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }

        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs)
  }
}

private class ExistingMemoryEdgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    activeSet: Option[VertexSet],
    size: Int) {

  private var edges = new PrimitiveVector[EdgeWithLocalIds[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED) {
    val e = new EdgeWithLocalIds[ED]
    e.srcId = src
    e.dstId = dst
    e.localSrcId = localSrc
    e.localDstId = localDst
    e.attr = d
    edges += e
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    Sorting.quickSort(edgeArray)(EdgeWithLocalIds.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var i = 0
      while (i < edgeArray.size) {
        localSrcIds(i) = edgeArray(i).localSrcId
        localDstIds(i) = edgeArray(i).localDstId
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcId != currSrcId) {
          currSrcId = edgeArray(i).srcId
          index.update(currSrcId, i)
        }
        i += 1
      }
    }

    new MemoryEdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
  }
}

/**
 * The Iterator type returned when constructing edge triplets. This could be an anonymous class in
 * EdgePartition.tripletIterator, but we name it here explicitly so it is easier to debug / profile.
 */
private class EdgeTripletIterator[VD: ClassTag, ED: ClassTag](
    val edgePartition: MemoryEdgePartition[ED, VD],
    val includeSrc: Boolean,
    val includeDst: Boolean)
  extends Iterator[EdgeTriplet[VD, ED]] {

  // Current position in the array.
  private var pos = 0

  override def hasNext: Boolean = pos < edgePartition.size

  override def next() = {
    val triplet = new EdgeTriplet[VD, ED]
    val localSrcId = edgePartition.localSrcIds(pos)
    val localDstId = edgePartition.localDstIds(pos)
    triplet.srcId = edgePartition.local2global(localSrcId)
    triplet.dstId = edgePartition.local2global(localDstId)
    if (includeSrc) {
      triplet.srcAttr = edgePartition.vertexAttrs(localSrcId)
    }
    if (includeDst) {
      triplet.dstAttr = edgePartition.vertexAttrs(localDstId)
    }
    triplet.attr = edgePartition.data(pos)
    pos += 1
    triplet
  }
}
