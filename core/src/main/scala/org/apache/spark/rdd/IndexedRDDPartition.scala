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

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.BitSet
import scala.collection.immutable.LongMap
import scala.collection.immutable.Vector
import scala.language.higherKinds
import scala.reflect.ClassTag

import IndexedRDD.Id
import IndexedRDDPartition.Index

private[spark] object IndexedRDDPartition {
  type Index = LongMap[Int]

  def apply[V: ClassTag](iter: Iterator[(Id, V)]): IndexedRDDPartition[V] = {
    IndexedRDDPartition(iter, (a, b) => b)
  }

  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : IndexedRDDPartition[V] = {
    var index = LongMap.empty[Int]
    val values = new ArrayBuffer[V]
    val mask = BitSet.newBuilder
    var i = 0
    iter.foreach { pair =>
      index.get(pair._1) match {
        case Some(iExisting) =>
          values(iExisting) = mergeFunc(values(iExisting), pair._2)
        case None =>
          index = index.updated(pair._1, i)
          values += pair._2
          mask += i
          i += 1
      }
    }
    new IndexedRDDPartition(index, values.toVector, mask.result)
  }
}

private[spark] trait IndexedRDDPartitionBase[@specialized(Long, Int, Double) V] {
  def index: Index
  def values: Vector[V]
  def mask: BitSet

  def size: Int = mask.size

  /** Return the value for the given key. */
  def apply(k: Id): V = values(index(k))

  def isDefined(k: Id): Boolean = {
    val pos = index.getOrElse(k, -1)
    pos >= 0 && mask.contains(pos)
  }

  def iterator: Iterator[(Id, V)] =
    index.iterator.filter(kv => mask.contains(kv._2)).map(kv => (kv._1, values(kv._2)))
}

private[spark] class IndexedRDDPartition[@specialized(Long, Int, Double) V](
    val index: Index,
    val values: Vector[V],
    val mask: BitSet)
   (implicit val vTag: ClassTag[V])
  extends IndexedRDDPartitionBase[V]
  with IndexedRDDPartitionOps[V, IndexedRDDPartition] {

  def self: IndexedRDDPartition[V] = this

  def withIndex(index: Index): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withValues[V2: ClassTag](values: Vector[V2]): IndexedRDDPartition[V2] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withMask(mask: BitSet): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }
}
