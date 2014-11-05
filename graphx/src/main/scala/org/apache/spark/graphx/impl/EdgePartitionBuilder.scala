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

import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.UUID

import scala.reflect.ClassTag
import scala.util.Sorting
import org.apache.spark.SparkEnv

import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector, ExternalSorter}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx] trait EdgePartitionBuilder[ED, VD] {
  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED): Unit
  def toEdgePartition: EdgePartition[ED, VD]
}

private[graphx] object EdgePartitionBuilder {
  private[impl] val pairLexicographicOrdering = new Ordering[(VertexId, VertexId)] {
    override def compare(a: (VertexId, VertexId), b: (VertexId, VertexId)): Int = {
      if (a._1 == b._1) {
        if (a._2 == b._2) 0
        else if (a._2 < b._2) -1
        else 1
      } else if (a._1 < b._1) -1
      else 1
    }
  }
}
