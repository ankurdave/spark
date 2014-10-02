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

package org.apache.spark.streaming.dstream

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.IndexedRDD.Id
import org.apache.spark.rdd.PatriciaTreeIndexedRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class SparselyUpdatedStateDStream[V: ClassTag, S: ClassTag](
    parent: DStream[(Id, V)],
    updateFunc: (Seq[V], Option[S]) => Option[S],
    partitioner: Partitioner
  ) extends DStream[(Id, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[PatriciaTreeIndexedRDD[S]] = {
    // Try to get the previous state RDD
    getOrCompute(validTime - slideDuration) match {
      case Some(prevStateRDD) =>    // If previous state RDD exists
        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>   // If parent RDD exists, then compute as usual
            val updatesByKey = parentRDD.groupByKey(partitioner)
            val updateFuncLocal = updateFunc
            val stateRDD = prevStateRDD.asInstanceOf[PatriciaTreeIndexedRDD[S]].multiputWithDeletion[Iterable[V]](
              updatesByKey,
              (id, update) => updateFuncLocal(update.toSeq, None),
              (id, oldState, update) => updateFuncLocal(update.toSeq, Some(oldState)))
            Some(stateRDD)

          case None =>    // If parent RDD does not exist
            // State is updated sparsely (i.e., only when updates arrive), so do nothing
            Some(prevStateRDD.asInstanceOf[PatriciaTreeIndexedRDD[S]])
        }

      case None =>    // If previous session RDD does not exist (first input data)

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>   // If parent RDD exists, then compute as usual
            // Create the state RDD from scratch using the parent RDD
            val updatesByKey = parentRDD.groupByKey(partitioner)
            val updateFuncLocal = updateFunc
            val stateRDD = PatriciaTreeIndexedRDD(updatesByKey.flatMapValues(
              vs => updateFuncLocal(vs.toSeq, None)))
            Some(stateRDD)

          case None => // If parent RDD does not exist, then nothing to do!
            // logDebug("Not generating state RDD (no previous state, no parent)")
            None
        }
    }
  }
}
