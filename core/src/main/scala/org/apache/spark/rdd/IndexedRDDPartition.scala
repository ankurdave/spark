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

/**
 * Contains members that are shared among all variants of IndexedRDDPartition (e.g.,
 * IndexedRDDPartition, ShippableVertexPartition).
 *
 * @tparam V the type of the values stored in the IndexedRDDPartition
 * @tparam Self the type of the implementing container. This allows transformation methods on any
 * implementing container to yield a result of the same type.
 */
private[spark] trait IndexedRDDPartition[
    V,
    Self[X] <: IndexedRDDPartition[X, Self]]
  extends Serializable with Logging {

  /** A generator for ClassTags of the value type V. */
  implicit def vTag: ClassTag[V]

  /** Accessor for the IndexedRDDPartition variant that is mixing in this trait. */
  def self: Self[V]

  def size: Int

  /** Return the value for the given key. */
  def apply(k: Id): V

  def isDefined(k: Id): Boolean

  def iterator: Iterator[(Id, V)]

  /**
   * Gets the values corresponding to the specified keys, if any.
   */
  def multiget(ks: Array[Id]): LongMap[V]

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDDPartition that reflects the modification.
   */
  def multiput(kvs: Seq[(Id, V)], merge: (Id, V, V) => V): Self[V]

  /** Deletes the specified keys. Returns a new IndexedRDDPartition that reflects the deletions. */
  def delete(ks: Array[Id]): Self[V]

  /** Maps each value, supplying the corresponding key and preserving the index. */
  def mapValues[V2: ClassTag](f: (Id, V) => V2): Self[V2]

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDDPartition and is implemented using soft
   * deletions.
   */
  def filter(pred: (Id, V) => Boolean): Self[V]

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: Self[V]): Self[V]

  /** Joins `this` with `other`, running `f` on the values of all keys in both sets. */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Self[V2])
      (f: (Id, Option[V], Option[V2]) => W): Self[W]

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V): Self[V]

  /**
   * Left outer joins `this` with the iterator `other`, running `f` on the values of corresponding
   * keys. Because values in `this` with no corresponding entries in `other` are preserved, `f`
   * cannot change the value type.
   */
  def join[U: ClassTag]
      (other: Iterator[(Id, U)])
      (f: (Id, V, U) => V): Self[V]

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Self[V2])
      (f: (Id, V, Option[V2]) => V3): Self[V3]

  /** Left outer joins `this` with the iterator `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(Id, V2)])
      (f: (Id, V, Option[V2]) => V3): Self[V3]

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V2): Self[V2]

  /**
   * Inner joins `this` with the iterator `other`, running `f` on the values of corresponding
   * keys.
   */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (iter: Iterator[Product2[Id, U]])
      (f: (Id, V, U) => V2): Self[V2]

  /**
   * Inner joins `this` with `iter`, taking values from `iter` and hiding other values using the
   * bitmask.
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): Self[V]

  /**
   * Creates a new IndexedRDDPartition with values from `iter` that may share an index with `this`,
   * merging duplicate keys in `messages` arbitrarily. If `iter` contains keys not in the index of
   * `this`, the new index will be different.
   */
  def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[Id, V2]])
    : Self[V2]

  /**
   * Creates a new IndexedRDDPartition with values from `iter` that may share an index with `this`,
   * merging duplicate keys using `reduceFunc`. If `iter` contains keys not in the index of `this`,
   * the new index will be different.
   */
  def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): Self[V2]

  /**
   * Rebuilds the indexes of this IndexedRDDPartition, removing deleted entries. The resulting
   * IndexedRDDPartition will not support efficient joins with the original one.
   */
  def reindex(): Self[V]
}
