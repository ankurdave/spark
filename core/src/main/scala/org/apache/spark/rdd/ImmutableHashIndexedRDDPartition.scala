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
import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.ImmutableBitSet
import org.apache.spark.util.collection.ImmutableLongOpenHashSet
import org.apache.spark.util.collection.ImmutableVector
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import ImmutableHashIndexedRDDPartition.Index

/**
 * An immutable map of key-value `(Id, V)` pairs that enforces key uniqueness and pre-indexes the
 * entries for efficient joins and point lookups/updates. Two ImmutableHashIndexedRDDPartitions with the same
 * index can be joined efficiently. All operations except [[reindex]] preserve the index. To
 * construct an `ImmutableHashIndexedRDDPartition`, use the [[org.apache.spark.rdd.ImmutableHashIndexedRDDPartition$
 * ImmutableHashIndexedRDDPartition object]].
 *
 * @tparam V the value associated with each entry in the set.
 */
private[spark] class ImmutableHashIndexedRDDPartition[@specialized(Long, Int, Double) V](
    val index: Index,
    val values: ImmutableVector[V],
    val mask: ImmutableBitSet)
   (implicit val vTag: ClassTag[V])
  extends IndexedRDDPartition[V, ImmutableHashIndexedRDDPartition] with Logging {

  def self: ImmutableHashIndexedRDDPartition[V] = this

  def withIndex(index: Index): ImmutableHashIndexedRDDPartition[V] = {
    new ImmutableHashIndexedRDDPartition(index, values, mask)
  }

  def withValues[V2: ClassTag](values: ImmutableVector[V2]): ImmutableHashIndexedRDDPartition[V2] = {
    new ImmutableHashIndexedRDDPartition(index, values, mask)
  }

  def withMask(mask: ImmutableBitSet): ImmutableHashIndexedRDDPartition[V] = {
    new ImmutableHashIndexedRDDPartition(index, values, mask)
  }

  val capacity: Int = index.capacity

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

  override def multiput(kvs: Seq[(Id, V)], merge: (Id, V, V) => V): ImmutableHashIndexedRDDPartition[V] = {
    if (kvs.forall(kv => self.isDefined(kv._1))) {
      // Pure updates can be implemented by modifying only the values
      join(kvs.iterator)(merge)
    } else {
      multiputIterator(kvs.iterator, merge)
    }
  }

  private def multiputIterator(
      kvs: Iterator[Product2[Id, V]], merge: (Id, V, V) => V): ImmutableHashIndexedRDDPartition[V] = {
    var newIndex = self.index
    var newValues = self.values
    var newMask = self.mask

    var preMoveValues: ImmutableVector[V] = null
    var preMoveMask: ImmutableBitSet = null
    def grow(newSize: Int) {
      preMoveValues = newValues
      preMoveMask = newMask

      newValues = ImmutableVector.fill(newSize)(null.asInstanceOf[V])
      newMask = new ImmutableBitSet(newSize)
    }
    def move(oldPos: Int, newPos: Int) {
      newValues = newValues.updated(newPos, preMoveValues(oldPos))
      if (preMoveMask.get(oldPos)) newMask = newMask.set(newPos)
    }

    for (kv <- kvs) {
      val id = kv._1
      val otherValue = kv._2
      newIndex = newIndex.addWithoutResize(id)
      if ((newIndex.focus & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        // This is a new key - need to update index
        val pos = newIndex.focus & OpenHashSet.POSITION_MASK
        newValues = newValues.updated(pos, otherValue)
        newMask = newMask.set(pos)
        newIndex = newIndex.rehashIfNeeded(grow, move)
      } else {
        // Existing key - just need to set value and ensure it appears in newMask
        val pos = newIndex.focus
        val newValue = if (newMask.get(pos)) merge(id, newValues(pos), otherValue) else otherValue
        newValues = newValues.updated(pos, newValue)
        newMask = newMask.set(pos)
      }
    }

    this.withIndex(newIndex).withValues(newValues).withMask(newMask)
  }

  override def delete(ks: Array[Id]): ImmutableHashIndexedRDDPartition[V] = {
    var newMask = self.mask
    for (k <- ks) {
      val pos = self.index.getPos(k)
      if (pos != -1) {
        newMask = newMask.unset(pos)
      }
    }
    this.withMask(newMask)
  }

  override def mapValues[V2: ClassTag](f: (Id, V) => V2): ImmutableHashIndexedRDDPartition[V2] = {
    val newValues = new Array[V2](self.capacity)
    self.mask.iterator.foreach { i =>
      newValues(i) = f(self.index.getValue(i), self.values(i))
    }
    this.withValues(ImmutableVector.fromArray(newValues))
  }

  override def filter(pred: (Id, V) => Boolean): ImmutableHashIndexedRDDPartition[V] = {
    val newMask = new BitSet(self.capacity)
    self.mask.iterator.foreach { i =>
      if (pred(self.index.getValue(i), self.values(i))) {
        newMask.set(i)
      }
    }
    this.withMask(newMask.toImmutableBitSet)
  }

  override def diff(
      other: ImmutableHashIndexedRDDPartition[V]): ImmutableHashIndexedRDDPartition[V] = {
    if (self.index != other.index) {
      logWarning("Diffing two ImmutableHashIndexedRDDPartitions with different indexes is slow.")
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
      this.withMask(newMask.toImmutableBitSet)
    } else {
      var newMask = self.mask & other.mask
      newMask.iterator.foreach { i =>
        if (self.values(i) == other.values(i)) {
          newMask = newMask.unset(i)
        }
      }
      this.withMask(newMask)
    }
  }

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: ImmutableHashIndexedRDDPartition[V2])
      (f: (Id, Option[V], Option[V2]) => W): ImmutableHashIndexedRDDPartition[W] = {
    if (self.index != other.index) {
      logWarning("Joining two ImmutableHashIndexedRDDPartitions with different indexes is slow.")
      val newValues = new Array[W](self.capacity)

      // First run on all values in `this`. No need to modify the index or mask.
      self.mask.iterator.foreach { selfI =>
        val vid = self.index.getValue(selfI)
        val otherI =
          if (other.index.getValue(selfI) == vid) {
            if (other.mask.get(selfI)) selfI else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        val otherValue = if (otherI != -1) Some(other.values(otherI)) else None
        newValues(selfI) = f(vid, Some(self.values(selfI)), otherValue)
      }

      // Then run on all values in `other` that are not in `this`.
      val newOtherIter = other.mask.iterator.flatMap { otherI =>
        val vid = other.index.getValue(otherI)
        if (!this.isDefined(vid)) {
          Some(Tuple2(vid, f(vid, None, Some(other.values(otherI)))))
        } else {
          None
        }
      }

      this.withValues(ImmutableVector.fromArray(newValues))
        .multiputIterator(newOtherIter, (id, a, b) => throw new Exception(
          "merge function was called but newOtherIter should only contain new elements"))
    } else {
      val newValues = new Array[W](self.capacity)
      val newMask = self.mask | other.mask

      (self.mask & other.mask).iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), Some(self.values(i)), Some(other.values(i)))
      }

      self.mask.andNot(other.mask).iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), Some(self.values(i)), None)
      }

      other.mask.andNot(self.mask).iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), None, Some(other.values(i)))
      }

      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask)
    }
  }

  override def join[U: ClassTag]
      (other: ImmutableHashIndexedRDDPartition[U])
      (f: (Id, V, U) => V): ImmutableHashIndexedRDDPartition[V] = {
    if (self.index != other.index) {
      logWarning("Joining two ImmutableHashIndexedRDDPartitions with different indexes is slow.")
      var newValues = self.values

      other.mask.iterator.foreach { otherI =>
        val vid = other.index.getValue(otherI)
        val selfI =
          if (self.index.getValue(otherI) == vid) {
            if (self.mask.get(otherI)) otherI else -1
          } else {
            if (self.isDefined(vid)) self.index.getPos(vid) else -1
          }
        if (selfI != -1) {
          newValues = newValues.updated(selfI, f(vid, self.values(selfI), other.values(otherI)))
        }
      }
      this.withValues(newValues)
    } else {
      val iterMask = self.mask & other.mask
      var newValues = self.values

      iterMask.iterator.foreach { i =>
        newValues = newValues.updated(i, f(self.index.getValue(i), self.values(i), other.values(i)))
      }
      this.withValues(newValues)
    }
  }

  override def join[U: ClassTag]
      (other: Iterator[(Id, U)])
      (f: (Id, V, U) => V): ImmutableHashIndexedRDDPartition[V] = {
    var newValues = self.values
    other.foreach { kv =>
      val id = kv._1
      val otherValue = kv._2
      val i = self.index.getPos(kv._1)
      newValues = newValues.updated(i, f(id, self.values(i), otherValue))
    }
    this.withValues(newValues)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: ImmutableHashIndexedRDDPartition[V2])
      (f: (Id, V, Option[V2]) => V3): ImmutableHashIndexedRDDPartition[V3] = {
    if (self.index != other.index) {
      logWarning("Joining two ImmutableHashIndexedRDDPartitions with different indexes is slow.")
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
      this.withValues(ImmutableVector.fromArray(newValues))
    } else {
      val newValues = new Array[V3](self.capacity)

      self.mask.iterator.foreach { i =>
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
      }
      this.withValues(ImmutableVector.fromArray(newValues))
    }
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(Id, V2)])
      (f: (Id, V, Option[V2]) => V3): ImmutableHashIndexedRDDPartition[V3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: ImmutableHashIndexedRDDPartition[U])
      (f: (Id, V, U) => V2): ImmutableHashIndexedRDDPartition[V2] = {
    if (self.index != other.index) {
      logWarning("Joining two ImmutableHashIndexedRDDPartitions with different indexes is slow.")
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
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask.toImmutableBitSet)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      newMask.iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
      }
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask)
    }
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (iter: Iterator[Product2[Id, U]])
      (f: (Id, V, U) => V2): ImmutableHashIndexedRDDPartition[V2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  override def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): ImmutableHashIndexedRDDPartition[V] = {
    val newMask = new BitSet(self.capacity)
    var newValues = self.values
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues = newValues.updated(pos, pair._2)
      }
    }
    this.withValues(newValues).withMask(newMask.toImmutableBitSet)
  }

  override def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[Id, V2]])
    : ImmutableHashIndexedRDDPartition[V2] = {
    aggregateUsingIndex(iter, (a, b) => b)
  }

  override def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): ImmutableHashIndexedRDDPartition[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    val newElements = new PrimitiveVector[Product2[Id, V2]]
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
      } else {
        newElements += product
      }
    }

    val aggregated = this.withValues(ImmutableVector.fromArray(newValues))
      .withMask(newMask.toImmutableBitSet)
    if (newElements.length > 0) {
      val newElementsIter = newElements.trim().array.iterator
      aggregated.multiputIterator(newElementsIter, (id, a, b) => throw new Exception(
        "merge function was called but newElementsIter should only contain new elements"))
    } else {
      aggregated
    }
  }

  override def reindex(): ImmutableHashIndexedRDDPartition[V] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
    val arbitraryMerge = (a: V, b: V) => a
    for ((k, v) <- self.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(ImmutableLongOpenHashSet.fromLongOpenHashSet(hashMap.keySet))
      .withValues(ImmutableVector.fromArray(hashMap.values))
      .withMask(hashMap.keySet.getBitSet.toImmutableBitSet)
  }
}

private[spark] object ImmutableHashIndexedRDDPartition {
  type Index = ImmutableLongOpenHashSet

  /**
   * Constructs an ImmutableHashIndexedRDDPartition from an iterator of pairs, merging duplicate keys
   * arbitrarily.
   */
  def apply[V: ClassTag](iter: Iterator[(Id, V)]): ImmutableHashIndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    new ImmutableHashIndexedRDDPartition(
      ImmutableLongOpenHashSet.fromLongOpenHashSet(map.keySet),
      ImmutableVector.fromArray(map.values),
      map.keySet.getBitSet.toImmutableBitSet)
  }

  /** Constructs an ImmutableHashIndexedRDDPartition from an iterator of pairs. */
  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : ImmutableHashIndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    new ImmutableHashIndexedRDDPartition(
      ImmutableLongOpenHashSet.fromLongOpenHashSet(map.keySet),
      ImmutableVector.fromArray(map.values),
      map.keySet.getBitSet.toImmutableBitSet)
  }
}
