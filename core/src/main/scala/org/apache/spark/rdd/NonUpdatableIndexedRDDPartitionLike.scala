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

import scala.collection.immutable.LongMap
import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id

private[spark] trait NonUpdatableIndexedRDDPartitionLike[
    @specialized(Long, Int, Double) V,
    Self[X] <: NonUpdatableIndexedRDDPartitionLike[X, Self]]
  extends IndexedRDDPartitionLike[V, Self] with Logging {

  def index: OpenHashSet[Long]
  def values: Array[V]
  def mask: BitSet

  def withIndex(index: OpenHashSet[Long]): Self[V]
  def withValues[V2: ClassTag](values: Array[V2]): Self[V2]
  def withMask(mask: BitSet): Self[V]

  override val capacity: Int = index.capacity

  override def size: Int = mask.cardinality()

  override def apply(k: Id): V = values(index.getPos(k))

  override def isDefined(k: Id): Boolean = {
    val pos = index.getPos(k)
    pos >= 0 && mask.get(pos)
  }

  override def iterator: Iterator[(Id, V)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))

  override def multiget(ks: Array[Id]): LongMap[V] = {
    var result = LongMap.empty[V]
    var i = 0
    while (i < ks.length) {
      val k = ks(i)
      if (self.isDefined(k)) {
        result = result.updated(k, self(k))
      }
      i += 1
    }
    result
  }

  override def multiput(kvs: Seq[(Id, V)], merge: (Id, V, V) => V): Self[V] = {
    if (kvs.forall(kv => self.isDefined(kv._1))) {
      // Pure updates can be implemented by modifying only the values
      join(kvs.iterator)(merge)
    } else {
      // There is an insertion, so we must rebuild all three data structures
      // TODO: Don't rehash existing elements
      val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
      for ((k, v) <- (self.iterator ++ kvs.iterator)) {
        hashMap.setMerge(k, v, (a, b) => merge(k, a, b))
      }
      this.withIndex(hashMap.keySet).withValues(hashMap.values).withMask(hashMap.keySet.getBitSet)
    }
  }

  override def delete(ks: Array[Id]): Self[V] = {
    val newMask = self.mask.copy()
    for (k <- ks) {
      val pos = self.index.getPos(k)
      if (pos != -1) {
        newMask.unset(pos)
      }
    }
    this.withMask(newMask)
  }

  override def mapValues[V2: ClassTag](f: (Id, V) => V2): Self[V2] = {
    val newValues = new Array[V2](self.capacity)
    self.mask.iterator.foreach { i =>
      newValues(i) = f(self.index.getValue(i), self.values(i))
    }
    this.withValues(newValues)
  }

  override def filter(pred: (Id, V) => Boolean): Self[V] = {
    val newMask = new BitSet(self.capacity)
    self.mask.iterator.foreach { i =>
      if (pred(self.index.getValue(i), self.values(i))) {
        newMask.set(i)
      }
    }
    this.withMask(newMask)
  }

  override def diff(other: Self[V]): Self[V] = {
    if (self.index != other.index) {
      logWarning("Diffing two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val keep =
          if (other.index.getValue(i) == vid && other.mask.get(i)) {
            // The indices agree on this entry
            self.values(i) != other.values(i)
          } else if (other.isDefined(vid)) {
            // The indices do not agree, but the corresponding entry exists somewhere
            self.values(i) != other(vid)
          } else {
            // There is no corresponding entry
            false
          }

        if (keep) {
          newMask.set(i)
        }
      }
      this.withMask(newMask)
    } else {
      val newMask = self.mask & other.mask
      newMask.iterator.foreach { i =>
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
      }
      this.withMask(newMask)
    }
  }

  override def join[U: ClassTag](other: Self[U])(f: (Id, V, U) => V): Self[V] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newValues = self.values.clone()

      other.mask.iterator.foreach { otherI =>
        val vid = other.index.getValue(otherI)
        val selfI =
          if (self.index.getValue(otherI) == vid) {
            if (self.mask.get(otherI)) otherI else -1
          } else {
            if (self.isDefined(vid)) self.index.getPos(vid) else -1
          }
        if (selfI != -1) {
          newValues(selfI) = f(vid, self.values(selfI), other.values(otherI))
        }
      }
      this.withValues(newValues)
    } else {
      val iterMask = self.mask & other.mask
      val newValues = self.values.clone()

      iterMask.iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
      }
      this.withValues(newValues)
    }
  }

  override def join[U: ClassTag](other: Iterator[(Id, U)])(f: (Id, V, U) => V): Self[V] = {
    val newValues = self.values.clone()
    other.foreach { kv =>
      val id = kv._1
      val otherValue = kv._2
      val i = self.index.getPos(kv._1)
      newValues(i) = f(id, self.values(i), otherValue)
    }
    this.withValues(newValues)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Self[V2])
      (f: (Id, V, Option[V2]) => V3): Self[V3] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newValues = new Array[V3](self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        val otherV = if (otherI != -1) Some(other.values(otherI)) else None
        newValues(i) = f(vid, self.values(i), otherV)
      }
      this.withValues(newValues)
    } else {
      val newValues = new Array[V3](self.capacity)

      self.mask.iterator.foreach { i =>
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
      }
      this.withValues(newValues)
    }
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V2): Self[V2] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)
      val newValues = new Array[V2](self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        if (otherI != -1) {
          newValues(i) = f(vid, self.values(i), other.values(otherI))
          newMask.set(i)
        }
      }
      this.withValues(newValues).withMask(newMask)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      newMask.iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
      }
      this.withValues(newValues).withMask(newMask)
    }
  }

  override def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): Self[V] = {
    val newMask = new BitSet(self.capacity)
    val newValues = self.values.clone()
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  override def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[Id, V2]]): Self[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  override def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): Self[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    iter.foreach { product =>
      val id = product._1
      val value = product._2
      val pos = self.index.getPos(id)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), value)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = value
        }
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  override def reindex(): Self[V] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
    val arbitraryMerge = (a: V, b: V) => a
    for ((k, v) <- self.iterator) {
      // TODO: Why use setMerge instead of update?
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(hashMap.keySet).withValues(hashMap.values).withMask(hashMap.keySet.getBitSet)
  }
}
