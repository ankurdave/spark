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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import breeze.linalg._
import org.jblas.DoubleMatrix

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._

/** Alternating least squares. */
object ALS {
  /** Interface that mimics org.apache.spark.mllib.recommendation.ALS.train. */
  def train(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double = 0.01): MatrixFactorizationModel = {
    // Convert ratings to weighted edges between users and products. User IDs and product IDs will
    // occupy different parts of the VertexId space, which is guaranteed by the productMask.
    val productMask = 1L << 63
    val unsignedIntMask = 0xFFFFFFFFL
    val edges = ratings.map(r => Edge(
      r.user.toLong & unsignedIntMask,
      (r.product.toLong & unsignedIntMask) | productMask,
      r.rating))
    val graph = Graph.fromEdges(edges, 1)
    val model = run(graph, rank, lambda, iterations).vertices.cache()
    val userFeatures = model
      .filter((kv: (VertexId, Array[Double])) => (kv._1 & productMask) == 0)
      .map(kv => (kv._1.toInt, kv._2))
    val productFeatures = model
      .filter((kv: (VertexId, Array[Double])) => (kv._1 & productMask) != 0)
      .map(kv => ((kv._1 & unsignedIntMask).toInt, kv._2))
    new MatrixFactorizationModel(rank, userFeatures, productFeatures)
  }

  /** Implementation that operates on a graph. */
  private def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, Double],
      latentK: Int, lambda: Double, numIter: Int): Graph[Array[Double], Double] = {
    val alsGraph = graph.mapVertices((id, attr) =>
      Array.fill(latentK){ scala.util.Random.nextDouble() }).cache()
    // val maxUser = graph.edges.map(_.srcId).max

    def sendMsg(edge: EdgeTriplet[Array[Double], Double])
      : Iterator[(VertexId, (Array[Double], Array[Double]))] = {
      val y = edge.attr
      // Compute message for dst
      val X = edge.srcAttr
      val Xy = X.map(_ * y)
      val XtX = (for (i <- 0 until latentK; j <- i until latentK) yield X(i) * X(j)).toArray
      val dstMsg = (Xy, XtX)
      // Compute message for src
      val Z = edge.srcAttr
      val Zy = Z.map(_ * y)
      val ZtZ = (for (i <- 0 until latentK; j <- i until latentK) yield Z(i) * Z(j)).toArray
      val srcMsg = (Zy, ZtZ)
      // Send messages
      Iterator((edge.srcId, srcMsg), (edge.dstId, dstMsg))
    }
    def mergeMsg(
        a: (Array[Double], Array[Double]),
        b: (Array[Double], Array[Double]))
      : (Array[Double], Array[Double]) = {
      var i = 0
      while (i < a._1.length) { a._1(i) += b._1(i); i += 1 }
      i = 0
      while (i < a._2.length) { a._2(i) += b._2(i); i += 1 }
      a
    }
    def vprog(
        id: VertexId, attr: Array[Double], msg: (Array[Double], Array[Double])): Array[Double] = {
      val XyArray = msg._1
      val XtXArray = msg._2
      if (XyArray.isEmpty) {
        attr // no neighbors
      } else {
        val XtX = DenseMatrix.tabulate(latentK, latentK) { (i, j) =>
          (if (i < j) XtXArray(i + (j+1)*j/2) else XtXArray(i + (j+1)*j/2)) +
          (if (i == j) lambda else 1.0F) // regularization
        }
        val Xy = DenseMatrix.create(latentK, 1, XyArray)
        val w = XtX \ Xy
        w.data
      }
    }

    val emptyMsg = (Array.empty[Double], Array.empty[Double])

    Pregel(alsGraph, emptyMsg, numIter)(vprog, sendMsg, mergeMsg)
  }
}
