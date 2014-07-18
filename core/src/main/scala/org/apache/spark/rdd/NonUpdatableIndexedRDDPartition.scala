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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id

/**
 * An immutable map of key-value `(Id, V)` pairs that enforces key uniqueness and pre-indexes the
 * entries for efficient joins and point lookups. Two IndexedRDDPartitions with the same index can
 * be joined efficiently. All operations except [[reindex]] preserve the index. To construct an
 * `IndexedRDDPartition`, use the [[org.apache.spark.rdd.IndexedRDDPartition$ IndexedRDDPartition
 * object]].
 *
 * @tparam V the value associated with each entry in the set.
 */
private[spark] class NonUpdatableIndexedRDDPartition[@specialized(Long, Int, Double) V](
    override val index: OpenHashSet[Long],
    override val values: Array[V],
    override val mask: BitSet)
   (implicit override val vTag: ClassTag[V])
  extends NonUpdatableIndexedRDDPartitionLike[V, NonUpdatableIndexedRDDPartition] {

  override def self = this

  override def withIndex(index: OpenHashSet[Long]) = {
    new NonUpdatableIndexedRDDPartition(index, values, mask)
  }

  override def withValues[V2: ClassTag](values: Array[V2]) = {
    new NonUpdatableIndexedRDDPartition(index, values, mask)
  }

  override def withMask(mask: BitSet) = {
    new NonUpdatableIndexedRDDPartition(index, values, mask)
  }
}

private[spark] object NonUpdatableIndexedRDDPartition {
  /**
   * Constructs an IndexedRDDPartition from an iterator of pairs, merging duplicate keys
   * arbitrarily.
   */
  def apply[V: ClassTag](iter: Iterator[(Id, V)]): NonUpdatableIndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    new NonUpdatableIndexedRDDPartition(map.keySet, map.values, map.keySet.getBitSet)
  }

  /** Constructs an IndexedRDDPartition from an iterator of pairs. */
  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : NonUpdatableIndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    new NonUpdatableIndexedRDDPartition(map.keySet, map.values, map.keySet.getBitSet)
  }
}
