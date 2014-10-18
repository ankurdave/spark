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

import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel

import IndexedRDD.Id

@Experimental
class PatriciaTreeIndexedRDD[V: ClassTag]
    (override val partitionsRDD: RDD[PatriciaTreeIndexedRDDPartition[V]])
  extends RDD[(Id, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with IndexedRDD[V, PatriciaTreeIndexedRDDPartition, PatriciaTreeIndexedRDD] {

  override protected def vTag: ClassTag[V] = classTag[V]

  override protected def pTag[V2]: ClassTag[PatriciaTreeIndexedRDDPartition[V2]] =
    classTag[PatriciaTreeIndexedRDDPartition[V2]]

  override protected def self: PatriciaTreeIndexedRDD[V] = this

  override def withPartitionsRDD[V2: ClassTag](
      partitionsRDD: RDD[PatriciaTreeIndexedRDDPartition[V2]]): PatriciaTreeIndexedRDD[V2] = {
    new PatriciaTreeIndexedRDD(partitionsRDD)
  }
}

object PatriciaTreeIndexedRDD {
  /**
   * Constructs an PatriciaTreeIndexedRDD from an RDD of pairs, partitioning keys using a hash partitioner,
   * preserving the number of partitions of `elems`, and merging duplicate keys arbitrarily.
   */
  def apply[V: ClassTag](elems: RDD[(Id, V)]): PatriciaTreeIndexedRDD[V] = {
    PatriciaTreeIndexedRDD(elems, elems.partitioner.getOrElse(new HashPartitioner(elems.partitions.size)))
  }

  /** Constructs an PatriciaTreeIndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily. */
  def apply[V: ClassTag](elems: RDD[(Id, V)], partitioner: Partitioner): PatriciaTreeIndexedRDD[V] = {
    val partitioned: RDD[(Id, V)] = elems.partitionBy(partitioner)
    val partitions = partitioned.mapPartitions(
      iter => Iterator(PatriciaTreeIndexedRDDPartition(iter)),
      preservesPartitioning = true)
    new PatriciaTreeIndexedRDD(partitions)
  }

  /** Constructs an PatriciaTreeIndexedRDD from an RDD of pairs. */
  def apply[V: ClassTag](
      elems: RDD[(Id, V)], partitioner: Partitioner, mergeValues: (V, V) => V): PatriciaTreeIndexedRDD[V] = {
    val partitioned: RDD[(Id, V)] = elems.partitionBy(partitioner)
    val partitions = partitioned.mapPartitions(
      iter => Iterator(PatriciaTreeIndexedRDDPartition(iter, mergeValues)),
      preservesPartitioning = true)
    new PatriciaTreeIndexedRDD(partitions)
  }
}
