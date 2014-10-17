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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.scalatest.FunSuite

class ImmutableHashIndexedRDDPartitionSuite
  extends IndexedRDDPartitionSuite[ImmutableHashIndexedRDDPartition] {

  override def create[V: ClassTag](iter: Iterator[(IndexedRDD.Id, V)]) =
    ImmutableHashIndexedRDDPartition(iter)

  override def pTag[V2]: ClassTag[ImmutableHashIndexedRDDPartition[V2]] =
    classTag[ImmutableHashIndexedRDDPartition[V2]]

  test("index reuse - createUsingIndex") {
    val vp = create(Iterator((0L, 1), (1L, 1), (2L, 1)))

    // No new elements
    val elems1 = List((0L, 2), (2L, 2))
    val vp1 = vp.createUsingIndex(elems1.iterator)
    assert(vp1.iterator.toSet === elems1.toSet)
    assert(vp.index === vp1.index)

    // New elements
    val elems2 = List((0L, 2), (2L, 2), (3L, 2))
    val vp2 = vp.createUsingIndex(elems2.iterator)
    assert(vp2.iterator.toSet === elems2.toSet)
    assert(vp.index != vp2.index)
  }

  test("index reuse - aggregateUsingIndex") {
    val vp = create(Iterator((0L, 1), (1L, 1), (2L, 1)))

    // No new elements
    val messages1 = List((0L, "a"), (2L, "b"), (0L, "c"))
    val vp1 = vp.aggregateUsingIndex[String](messages1.iterator, _ + _)
    assert(vp1.iterator.toSet === Set((0L, "ac"), (2L, "b")))
    assert(vp.index === vp1.index)

    // No new elements
    val messages2 = List((0L, "a"), (2L, "b"), (0L, "c"), (3L, "d"))
    val vp2 = vp.aggregateUsingIndex[String](messages2.iterator, _ + _)
    assert(vp2.iterator.toSet === Set((0L, "ac"), (2L, "b"), (3L, "d")))
    assert(vp.index != vp2.index)
  }

  test("reindex removes elements from index") {
    val vp = create(Iterator((0L, 1), (1L, 1), (2L, 1)))
    val vp2 = vp.filter { (vid, attr) => vid <= 1 }
    val vp3 = vp2.reindex()
    assert(vp2.iterator.toSet === vp3.iterator.toSet)
    assert(vp2(2) === 1)
    assert(vp3.index.getPos(2) === -1)
  }

}
