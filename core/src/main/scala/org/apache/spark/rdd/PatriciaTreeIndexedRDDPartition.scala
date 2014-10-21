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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.spark.Logging
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id

/**
 * An immutable map of key-value `(Id, V)` pairs that enforces key uniqueness and pre-indexes the
 * entries for efficient joins and point lookups/updates. Two PatriciaTreeIndexedRDDPartitions with the same
 * index can be joined efficiently. All operations except [[reindex]] preserve the index. To
 * construct an `PatriciaTreeIndexedRDDPartition`, use the [[org.apache.spark.rdd.PatriciaTreeIndexedRDDPartition$
 * PatriciaTreeIndexedRDDPartition object]].
 *
 * @tparam V the value associated with each entry in the set.
 */
private[spark] class PatriciaTreeIndexedRDDPartition[V](
    private var map: LongMap[V])
   (implicit val vTag: ClassTag[V])
  extends IndexedRDDPartition[V, PatriciaTreeIndexedRDDPartition]
  with Logging
  with KryoSerializable {

  def self: PatriciaTreeIndexedRDDPartition[V] = this

  def withMap[V2: ClassTag](map: LongMap[V2]): PatriciaTreeIndexedRDDPartition[V2] = {
    new PatriciaTreeIndexedRDDPartition(map)
  }

  def read(kryo: Kryo, in: Input) {
    val size = in.readInt()
    val keys = new Array[Long](size)
    for (i <- 0 until size) { keys(i) = in.readLong() }
    if (map == null) {
      map = LongMap.empty[V]
    }
    for (i <- 0 until size) {
      map = map.updated(keys(i), kryo.readClassAndObject(in).asInstanceOf[V])
    }
  }

  def write(kryo: Kryo, out: Output) {
    out.writeInt(size)
    map.keysIterator.foreach { k => out.writeLong(k) }
    map.valuesIterator.foreach { v => kryo.writeClassAndObject(out, v) }
  }

  override def size: Int = map.size

  override def apply(k: Id): V = map(k)

  override def isDefined(k: Id): Boolean = map.isDefinedAt(k)

  override def iterator: Iterator[(Id, V)] = map.iterator

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

  override def multiput(
      kvs: Iterator[(Id, V)], merge: (Id, V, V) => V): PatriciaTreeIndexedRDDPartition[V] = {
    this.withMap(map.unionWith(PatriciaTreeIndexedRDDPartition.iteratorToMap(kvs), merge))
  }

  override def multiput[U: ClassTag](
      kvs: Iterator[(Id, U)], insert: (Id, U) => V, merge: (Id, V, U) => V)
    : PatriciaTreeIndexedRDDPartition[V] = {
    val other = PatriciaTreeIndexedRDDPartition.iteratorToMap(kvs)
    val onlyOther = (other -- map.keys).transform[V](insert)
    val both = map.intersectionWith[U, V](other, merge)
    this.withMap(map ++ onlyOther ++ both)
  }

  override def multiputWithDeletion[U: ClassTag](
      kvs: Iterator[Product2[Id, U]], insert: (Id, U) => Option[V],
      merge: (Id, V, U) => Option[V]): PatriciaTreeIndexedRDDPartition[V] = {
    val other = PatriciaTreeIndexedRDDPartition.iteratorToMap(kvs)
    val insertionsOnlyOther = (other -- map.keys).modifyOrRemove(insert)
    val both = map.intersectionWith[U, Option[V]](other, merge)
    val insertionsBoth = both.modifyOrRemove((id, opt) => opt)

    val insertions = insertionsOnlyOther ++ insertionsBoth
    val deletions = both.filter(_._2.isEmpty).keys
    this.withMap(map ++ insertions -- deletions)
  }

  override def delete(ks: Array[Id]): PatriciaTreeIndexedRDDPartition[V] = {
    this.withMap(map -- ks)
  }

  override def mapValues[V2: ClassTag](f: (Id, V) => V2): PatriciaTreeIndexedRDDPartition[V2] = {
    this.withMap(map.transform(f))
  }

  override def filter(pred: (Id, V) => Boolean): PatriciaTreeIndexedRDDPartition[V] = {
    this.withMap(map.filter(Function.tupled(pred)))
  }

  override def diff(
      other: PatriciaTreeIndexedRDDPartition[V]): PatriciaTreeIndexedRDDPartition[V] = {
    val result = map
      .intersectionWith[V, Option[V]](other.map, (id, a, b) => if (a != b) Some(a) else None)
      .modifyOrRemove((id, opt) => opt)
    this.withMap(result)
  }

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: PatriciaTreeIndexedRDDPartition[V2])
      (f: (Id, Option[V], Option[V2]) => W): PatriciaTreeIndexedRDDPartition[W] = {
    val both = map.intersectionWith[V2, W](other.map, (id, a, b) => f(id, Some(a), Some(b)))
    val onlySelf = (map -- other.map.keys).transform[W]((id, a) => f(id, Some(a), None))
    val onlyOther = (other.map -- map.keys).transform[W]((id, b) => f(id, None, Some(b)))
    this.withMap(both ++ onlySelf ++ onlyOther)
  }

  override def join[U: ClassTag]
      (other: PatriciaTreeIndexedRDDPartition[U])
      (f: (Id, V, U) => V): PatriciaTreeIndexedRDDPartition[V] = {
    val onlySelf = map -- other.map.keys
    val both = map.intersectionWith[U, V](other.map, (id, a, b) => f(id, a, b))
    this.withMap(onlySelf ++ both)
  }

  override def join[U: ClassTag]
      (other: Iterator[(Id, U)])
      (f: (Id, V, U) => V): PatriciaTreeIndexedRDDPartition[V] = {
    join(createUsingIndex(other))(f)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: PatriciaTreeIndexedRDDPartition[V2])
      (f: (Id, V, Option[V2]) => V3): PatriciaTreeIndexedRDDPartition[V3] = {
    val onlySelf = (map -- other.map.keys).transform[V3]((id, a) => f(id, a, None))
    val both = map.intersectionWith[V2, V3](other.map, (id, a, b) => f(id, a, Some(b)))
    this.withMap(onlySelf ++ both)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(Id, V2)])
      (f: (Id, V, Option[V2]) => V3): PatriciaTreeIndexedRDDPartition[V3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: PatriciaTreeIndexedRDDPartition[U])
      (f: (Id, V, U) => V2): PatriciaTreeIndexedRDDPartition[V2] = {
    this.withMap(map.intersectionWith(other.map, f))
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (iter: Iterator[Product2[Id, U]])
      (f: (Id, V, U) => V2): PatriciaTreeIndexedRDDPartition[V2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  override def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): PatriciaTreeIndexedRDDPartition[V] = {
    this.withMap(PatriciaTreeIndexedRDDPartition.iteratorToMap(iter).intersection(map))
  }

  override def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[Id, V2]])
    : PatriciaTreeIndexedRDDPartition[V2] = {
    this.withMap(PatriciaTreeIndexedRDDPartition.iteratorToMap(iter))
  }

  override def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): PatriciaTreeIndexedRDDPartition[V2] = {
    PatriciaTreeIndexedRDDPartition(iter.map(kv => (kv._1, kv._2)), reduceFunc)
  }

  override def reindex(): PatriciaTreeIndexedRDDPartition[V] = {
    this
  }
}

private[spark] object PatriciaTreeIndexedRDDPartition {
  /**
   * Constructs an PatriciaTreeIndexedRDDPartition from an iterator of pairs, merging duplicate keys
   * arbitrarily.
   */
  def apply[V: ClassTag](iter: Iterator[(Id, V)]): PatriciaTreeIndexedRDDPartition[V] = {
    new PatriciaTreeIndexedRDDPartition(LongMap(iter.toSeq: _*))
  }

  /** Constructs an PatriciaTreeIndexedRDDPartition from an iterator of pairs. */
  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : PatriciaTreeIndexedRDDPartition[V] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
    for ((k, v) <- iter) {
      hashMap.setMerge(k, v, mergeFunc)
    }
    new PatriciaTreeIndexedRDDPartition(iteratorToMap(hashMap.iterator))
  }

  /**
   * Constructs an PatriciaTreeIndexedRDDPartition from an iterator of pairs, merging duplicate keys
   * by applying a binary operator to a start value and all values with the same key. The name comes
   * from the similar `foldLeft` operator in the Scala collections library.
   *
   * @param z the start value
   * @param f the binary operator to use for merging
   */
  def createWithFoldLeft[A: ClassTag, B: ClassTag](
      iter: Iterator[(Id, A)], z: => B, f: (B, A) => B): PatriciaTreeIndexedRDDPartition[B] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Id, B]
    iter.foreach { pair =>
      val id = pair._1
      val a = pair._2
      hashMap.changeValue(id, f(z, a), b => f(b, a))
    }
    new PatriciaTreeIndexedRDDPartition(iteratorToMap(hashMap.iterator))
  }

  protected def iteratorToMap[V: ClassTag](iter: Iterator[Product2[Id, V]]): LongMap[V] = {
    iter.foldLeft(LongMap.empty[V])((x, y) => x.updated(y._1, y._2))
  }
}
