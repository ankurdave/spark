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

import scala.reflect.ClassTag
import scala.util.Sorting

import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx]
class FreshEdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    size: Int = 64) {
  var edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    edges += Edge(src, dst, d)
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    Sorting.quickSort(edgeArray)(Edge.lexicographicOrdering)
    val srcIds = new Array[VertexId](edgeArray.size)
    val dstIds = new Array[VertexId](edgeArray.size)
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
      index.update(srcIds(0), 0)
      var currSrcId: VertexId = srcIds(0)
      var currLocalId = -1
      var i = 0
      while (i < edgeArray.size) {
        srcIds(i) = edgeArray(i).srcId
        dstIds(i) = edgeArray(i).dstId
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcId != currSrcId) {
          currSrcId = edgeArray(i).srcId
          index.update(currSrcId, i)
        }
        // Assign each vertex id to an offset
        localSrcIds(i) = global2local.changeValue(srcIds(i),
          { currLocalId += 1; local2global += srcIds(i); currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstIds(i),
          { currLocalId += 1; local2global += dstIds(i); currLocalId }, identity)
        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    new EdgePartition(
      srcIds, dstIds, localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs)
  }
}

private[graphx]
class VertexPreservingEdgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    size: Int = 64) {
  var edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED) {
    val e = Edge(src, dst, d)
    e.localSrcId = localSrc
    e.localDstId = localDst
    edges += e
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    Sorting.quickSort(edgeArray)(Edge.lexicographicOrdering)
    val srcIds = new Array[VertexId](edgeArray.size)
    val dstIds = new Array[VertexId](edgeArray.size)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (edgeArray.length > 0) {
      index.update(srcIds(0), 0)
      var currSrcId: VertexId = srcIds(0)
      var i = 0
      while (i < edgeArray.size) {
        srcIds(i) = edgeArray(i).srcId
        dstIds(i) = edgeArray(i).dstId
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

    new EdgePartition(srcIds, dstIds, localSrcIds, localDstIds, data, index,
      global2local, local2global, vertexAttrs)
  }
}
