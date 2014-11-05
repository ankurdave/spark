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
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.Sorting
import org.apache.spark.SparkEnv

import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector, ExternalSorter}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkEnv
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.Utils

/**
 * A collection of edges stored on disk, along with referenced vertex attributes and an optional
 * active vertex set for filtering computation on the edges. The latter two data structures are
 * stored in memory on the assumption that there are many more edges than vertices.
 *
 * @tparam ED the edge attribute type
 * @tparam VD the vertex attribute type
 *
 * @param localIdFile a file containing the local source and destination vertex id of each edge as
 *   indexes into `local2global` and `vertexAttrs`
 * @param attrFile a file containing the attribute associated with each edge
 * @param index a clustered index on source vertex id as a map from each global source vertex id to
 *   the offset in the edge arrays where the cluster for that vertex id begins
 * @param global2local a map from referenced vertex ids to local ids which index into vertexAttrs
 * @param local2global an array of global vertex ids where the offsets are local vertex ids
 * @param vertexAttrs an array of vertex attributes where the offsets are local vertex ids
 * @param activeSet an optional active vertex set for filtering computation on the edges
 */
private[graphx]
class DiskEdgePartition[ED: ClassTag, VD: ClassTag](
    val localIdFile: File = null,
    val attrFile: File = null,
    val size: Int = 0,
    val index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int] = null,
    val global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int] = null,
    val local2global: Array[VertexId] = null,
    val vertexAttrs: Array[VD] = null,
    val activeSet: Option[VertexSet] = None)
  extends EdgePartition[ED, VD] {

  override def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val activeSet = new VertexSet
    iter.foreach(activeSet.add(_))
    new DiskEdgePartition(
      localIdFile, attrFile, size, index, global2local, local2global, vertexAttrs,
      Some(activeSet))
  }

  override def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    val vertexAttrs = new Array[VD](this.vertexAttrs.length)
    System.arraycopy(this.vertexAttrs, 0, vertexAttrs, 0, this.vertexAttrs.length)
    iter.foreach { kv =>
      vertexAttrs(global2local(kv._1)) = kv._2
    }
    new DiskEdgePartition(localIdFile, attrFile, size, index, global2local, local2global, vertexAttrs,
      activeSet)
  }

  override def clearVertices[VD2: ClassTag](): EdgePartition[ED, VD2] = {
    val vertexAttrs = new Array[VD2](this.vertexAttrs.length)
    new DiskEdgePartition(localIdFile, attrFile, size, index, global2local, local2global, vertexAttrs,
      activeSet)
  }

  override def isActive(vid: VertexId): Boolean = {
    activeSet.get.contains(vid)
  }

  override def numActives: Option[Int] = activeSet.map(_.size)

  override def reverse: EdgePartition[ED, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED, VD](this)
    localIdIterator.foreach { e =>
      builder.add(e.dstId, e.srcId, e.localDstId, e.localSrcId, e.attr)
    }
    builder.toEdgePartition
  }

  override def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[ED2, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED2, VD](this)
    localIdIterator.foreach { e =>
      builder.add(e.srcId, e.dstId, e.localSrcId, e.localDstId, f(e))
    }
    builder.toEdgePartition
  }

  override def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED2, VD](this)
    localIdIterator.foreach { e =>
      builder.add(e.srcId, e.dstId, e.localSrcId, e.localDstId, iter.next())
    }
    builder.toEdgePartition
  }

  override def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED, VD](this)
    localIdTripletIterator().foreach { et =>
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) {
        builder.add(et.srcId, et.dstId, et.localSrcId, et.localDstId, et.attr)
      }
    }
    builder.toEdgePartition
  }

  override def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  override def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED, VD](this)
    val currEdge = new EdgeWithLocalIds[ED]
    // Iterate through the edges, accumulating runs of identical edges into currEdge and releasing
    // them when we see the beginning of the next run
    val iter = localIdIterator
    if (iter.hasNext) {
      currEdge.set(iter.next())
      while (iter.hasNext) {
        val newEdge = iter.next()
        if (currEdge.srcId == newEdge.srcId && currEdge.dstId == newEdge.dstId) {
          // newEdge should be grouped with currEdge
          currEdge.attr = merge(currEdge.attr, newEdge.attr)
        } else {
          // newEdge starts a new run of edges, so we must release currEdge
          builder.add(
            currEdge.srcId, currEdge.dstId, currEdge.localSrcId, currEdge.localDstId, currEdge.attr)
          // Set currEdge for the next iteration
          currEdge.set(newEdge)
        }
      }
      // Finally, we must release the last edge
      builder.add(
        currEdge.srcId, currEdge.dstId, currEdge.localSrcId, currEdge.localDstId, currEdge.attr)
    }
    builder.toEdgePartition
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[ED3, VD] = {
    val builder = DiskEdgePartition.fromExisting[ED3, VD](this)
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

  override def indexSize: Int = index.size

  override def iterator: Iterator[Edge[ED]] = localIdIterator

  override def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]] =
    localIdIterator.map { e =>
      val et = new EdgeTriplet[VD, ED]
      et.set(e)
      if (includeSrc) { et.srcAttr = vertexAttrs(e.localSrcId) }
      if (includeDst) { et.dstAttr = vertexAttrs(e.localDstId) }
      et
    }

  override def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      idPred: (VertexId, VertexId) => Boolean): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    localIdIterator.foreach { e =>
      if (idPred(e.srcId, e.dstId)) {
        ctx.localSrcId = e.localSrcId
        ctx.localDstId = e.localDstId
        ctx.srcId = e.srcId
        ctx.dstId = e.dstId
        ctx.attr = e.attr
        if (tripletFields.useSrc) { ctx.srcAttr = vertexAttrs(e.localSrcId) }
        if (tripletFields.useDst) { ctx.dstAttr = vertexAttrs(e.localDstId) }
        sendMsg(ctx)
      }
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }

  override def aggregateMessagesWithIndex[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      srcIdPred: VertexId => Boolean,
      dstIdPred: VertexId => Boolean): Iterator[(VertexId, A)] = {
    aggregateMessages(sendMsg, mergeMsg, tripletFields, (a, b) => srcIdPred(a) && dstIdPred(b))
    // val aggregates = new Array[A](vertexAttrs.length)
    // val bitset = new BitSet(vertexAttrs.length)

    // var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    // index.iterator.foreach { cluster =>
    //   val clusterSrcId = cluster._1
    //   val clusterPos = cluster._2
    //   val clusterLocalSrcId = localSrcIds(clusterPos)
    //   if (srcIdPred(clusterSrcId)) {
    //     var pos = clusterPos
    //     ctx.srcId = clusterSrcId
    //     ctx.localSrcId = clusterLocalSrcId
    //     if (tripletFields.useSrc) { ctx.srcAttr = vertexAttrs(clusterLocalSrcId) }
    //     while (pos < size && localSrcIds(pos) == clusterLocalSrcId) {
    //       val localDstId = localDstIds(pos)
    //       val dstId = local2global(localDstId)
    //       if (dstIdPred(dstId)) {
    //         ctx.dstId = dstId
    //         ctx.localDstId = localDstId
    //         ctx.attr = data(pos)
    //         if (tripletFields.useDst) { ctx.dstAttr = vertexAttrs(localDstId) }
    //         sendMsg(ctx)
    //       }
    //       pos += 1
    //     }
    //   }
    // }

    // bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }

  private def localIdIterator = {
    val localIdStream = new ObjectInputStream(new FileInputStream(localIdFile))
    val ser = SparkEnv.get.serializer.newInstance()
    val attrStream = ser.deserializeStream(new FileInputStream(attrFile))
    new Iterator[EdgeWithLocalIds[ED]] {
      private[this] val edge = new EdgeWithLocalIds[ED]
      private[this] var pos = 0

      override def hasNext: Boolean = pos < DiskEdgePartition.this.size

      override def next(): EdgeWithLocalIds[ED] = {
        val localSrcId = localIdStream.readInt()
        val localDstId = localIdStream.readInt()
        val attr = attrStream.readObject[ED]()
        edge.srcId = local2global(localSrcId)
        edge.dstId = local2global(localDstId)
        edge.localSrcId = localSrcId
        edge.localDstId = localDstId
        edge.attr = attr
        pos += 1
        edge
      }
    }
  }

  private def localIdTripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true)
    : Iterator[EdgeTripletWithLocalIds[VD, ED]] =
    localIdIterator.map { e =>
      val et = new EdgeTripletWithLocalIds[VD, ED]
      et.set(e)
      et.localSrcId = e.localSrcId
      et.localDstId = e.localDstId
      if (includeSrc) { et.srcAttr = vertexAttrs(e.localSrcId) }
      if (includeDst) { et.dstAttr = vertexAttrs(e.localDstId) }
      et
    }
}

object DiskEdgePartition {
  private def fromExisting[ED: ClassTag, VD: ClassTag](
      ep: DiskEdgePartition[_, VD]) = {
    new ExistingDiskEdgePartitionBuilder[ED, VD](
      ep.global2local, ep.local2global, ep.vertexAttrs, ep.activeSet)
  }

  def newBuilder[ED: ClassTag, VD: ClassTag]: EdgePartitionBuilder[ED, VD] = {
    new DiskEdgePartitionBuilder[ED, VD]
  }
}

private class DiskEdgePartitionBuilder[ED: ClassTag, VD: ClassTag]
    extends EdgePartitionBuilder[ED, VD] {

  private var size = 0

  private val edgeSorter = new ExternalSorter[(VertexId, VertexId), ED, ED](
    ordering = Some(EdgePartitionBuilder.pairLexicographicOrdering))

  /** Add a new edge to the partition. */
  override def add(src: VertexId, dst: VertexId, d: ED) {
    edgeSorter.insertAll(Iterator(Tuple2((src, dst), d)))
    size += 1
  }

  override def toEdgePartition: EdgePartition[ED, VD] = {
    val basePath = Utils.getLocalDir(SparkEnv.get.conf) +
      "/spark-EdgePartition-" + UUID.randomUUID.toString + "-"

    val localIdFile = new File(basePath + "localIds")
    val localIdStream = new ObjectOutputStream(new FileOutputStream(localIdFile))

    val attrFile = new File(basePath + "attrs")
    val attrFileStream = new FileOutputStream(attrFile)
    val attrSerStream = SparkEnv.get.serializer.newInstance().serializeStream(attrFileStream)

    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index. Also populate a map from vertex id to a sequential local offset.
    var initialized = false
    var currSrcId: VertexId = -1
    var currLocalId = -1
    var i = 0
    edgeSorter.iterator.foreach { kv =>
      val srcId = kv._1._1
      val dstId = kv._1._2
      val attr = kv._2
      // Update the mapping between global vid and local vid
      val localSrcId = global2local.changeValue(srcId,
        { currLocalId += 1; local2global += srcId; currLocalId }, identity)
      val localDstId = global2local.changeValue(dstId,
        { currLocalId += 1; local2global += dstId; currLocalId }, identity)
      // Write the edge to disk
      localIdStream.writeInt(localSrcId)
      localIdStream.writeInt(localDstId)
      attrSerStream.writeObject(attr)
      // Update the clustered index on source vertex id
      if (srcId != currSrcId || !initialized) {
        currSrcId = srcId
        index.update(currSrcId, i)
      }
      i += 1
    }
    edgeSorter.stop()
    localIdStream.close()
    attrSerStream.close()
    attrFileStream.close()

    val vertexAttrs = new Array[VD](currLocalId + 1)
    new DiskEdgePartition(localIdFile, attrFile, size, index, global2local,
      local2global.trim().array, vertexAttrs)
  }
}

private class ExistingDiskEdgePartitionBuilder[ED: ClassTag, VD: ClassTag](
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    activeSet: Option[VertexSet]) {
  var size = 0
  val edgeSorter = new ExternalSorter[(VertexId, VertexId), (Int, Int, ED), (Int, Int, ED)](
    ordering = Some(EdgePartitionBuilder.pairLexicographicOrdering))

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED) {
    edgeSorter.insertAll(Iterator(Tuple2((src, dst), (localSrc, localDst, d))))
    size += 1
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val basePath = Utils.getLocalDir(SparkEnv.get.conf) +
      "/spark-EdgePartition-" + UUID.randomUUID.toString + "-"

    val localIdFile = new File(basePath + "localIds")
    val localIdStream = new ObjectOutputStream(new FileOutputStream(localIdFile))

    val attrFile = new File(basePath + "attrs")
    val attrFileStream = new FileOutputStream(attrFile)
    val attrSerStream = SparkEnv.get.serializer.newInstance().serializeStream(attrFileStream)

    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    var initialized = false
    var currSrcId: VertexId = -1
    var i = 0
    edgeSorter.iterator.foreach { kv =>
      val srcId = kv._1._1
      val dstId = kv._1._2
      val localSrcId = kv._2._1
      val localDstId = kv._2._2
      val attr = kv._2._3

      // Write the edge to disk
      localIdStream.writeInt(localSrcId)
      localIdStream.writeInt(localDstId)
      attrSerStream.writeObject(attr)

      // Update the clustered index on source vertex id
      if (srcId != currSrcId || !initialized) {
        currSrcId = srcId
        index.update(currSrcId, i)
      }
      i += 1
    }
    edgeSorter.stop()
    localIdStream.close()
    attrSerStream.close()
    attrFileStream.close()

    new DiskEdgePartition(localIdFile, attrFile, size, index, global2local, local2global,
      vertexAttrs, activeSet)
  }
}

private class EdgeTripletWithLocalIds[VD, ED] extends EdgeTriplet[VD, ED] {
  var localSrcId = -1
  var localDstId = -1
}
