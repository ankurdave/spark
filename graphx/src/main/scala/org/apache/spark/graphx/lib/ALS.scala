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
import scala.util.Random

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
    // Reversibly convert ratings to weighted edges between users and products. User IDs and product
    // IDs will occupy different parts of the VertexId space.
    val edges = ratings.map(r => Edge(
      compose(0, r.user),
      compose(1, r.product),
      r.rating))
    val graph = Graph.fromEdges(edges, 1)
    val model = run(graph, rank, lambda, iterations).vertices.cache()
    val userFeatures = model
      .filter((kv: (VertexId, Array[Double])) => high(kv._1) == 0)
      .map(kv => (low(kv._1), kv._2))
    val productFeatures = model
      .filter((kv: (VertexId, Array[Double])) => high(kv._1) == 1)
      .map(kv => (low(kv._1), kv._2))
    new MatrixFactorizationModel(rank, userFeatures, productFeatures)
  }

  /** Implementation that operates on a graph. */
  private def run[VD](graph: Graph[VD, Double],
      latentK: Int, lambda: Double, numIter: Int): Graph[Array[Double], Double] = {
    // Initialize user and product factors randomly, but use a deterministic seed for each partition
    // so that fault recovery works
    val seed = new Random().nextLong
    val alsGraph = graph.mapVertices((id, attr) =>
      randomFactor(latentK, new Random(seed ^ id))).cache()

    def sendMsg(iteration: Int, edge: EdgeTriplet[PregelVertex[Array[Double]], Double])
      : Iterator[(VertexId, (Array[Double], Array[Double]))] = {
      val sendToDst = iteration % 2 == 0
      val y = edge.attr
      val theX = if (sendToDst) edge.srcAttr.attr else edge.dstAttr.attr
      val theXy = theX.map(_ * y)
      val theXtX = (for (i <- 0 until latentK; j <- i until latentK) yield theX(i) * theX(j)).toArray
      val msg = (theXy, theXtX)
      val recipient = if (sendToDst) edge.dstId else edge.srcId
      Iterator((recipient, msg))
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
        iteration: Int,
        id: VertexId,
        vertex: PregelVertex[Array[Double]],
        msg: Option[(Array[Double], Array[Double])])
      : PregelVertex[Array[Double]] = {
      msg match {
        case Some((theXyArray, theXtXArray)) =>
          val reg = DenseMatrix.eye[Double](latentK) * lambda
          val theXtX = matrixFromTriangular(latentK, theXtXArray) + reg
          val theXy = DenseMatrix.create(latentK, 1, theXyArray)
          val w = theXtX \ theXy
          PregelVertex(w.data)
        case None =>
          vertex
      }
    }

    Pregel.run(alsGraph, numIter)(vprog, sendMsg, mergeMsg)
  }

  /**
   * Make a random factor vector with the given random.
   */
  private def randomFactor(rank: Int, rand: Random): Array[Double] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    val factor = Array.fill(rank)(math.abs(rand.nextGaussian()))
    val norm = math.sqrt(factor.map(x => x * x).sum)
    factor.map(x => x / norm)
  }

  private def matrixFromTriangular(rank: Int, triangular: Array[Double]): DenseMatrix[Double] = {
    val arr = new Array[Double](rank * rank)
    var pos = 0
    for (i <- 0 until rank) {
      for (j <- i until rank) {
        arr(i * rank + j) = triangular(pos)
        arr(j * rank + i) = triangular(pos)
        pos += 1
      }
    }
    DenseMatrix.create(rank, rank, arr)
  }

  // From http://stackoverflow.com/questions/5147738/supressing-sign-extension-when-upcasting-or-shifting-in-java
  private def compose(hi: Int, lo: Int): Long = (hi.toLong << 32) + unsigned(lo)
  private def unsigned(x: Int): Long = x & 0xFFFFFFFFL
  private def high(x: Long): Int = (x >> 32).toInt
  private def low(x: Long): Int = x.toInt
}
