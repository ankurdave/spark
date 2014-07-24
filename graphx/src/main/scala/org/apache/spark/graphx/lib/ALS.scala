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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.byteswap32

import org.jblas.{DoubleMatrix, SimpleBlas, Solve}

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

    def replication(iteration: Int): (Boolean, Boolean) = {
      val sendToDst = iteration % 2 == 0
      (sendToDst, !sendToDst)
    }
    def sendMsg(iteration: Int, edge: EdgeTriplet[PregelVertex[Array[Double]], Double])
      : Iterator[(VertexId, ArrayBuffer[(Double, Array[Double])])] = {
      val sendToDst = iteration % 2 == 0
      val y = edge.attr
      val theX = if (sendToDst) edge.srcAttr.attr else edge.dstAttr.attr
      val msg = ArrayBuffer((y, theX))
      val recipient = if (sendToDst) edge.dstId else edge.srcId
      Iterator((recipient, msg))
    }
    def mergeMsg(
        a: ArrayBuffer[(Double, Array[Double])],
        b: ArrayBuffer[(Double, Array[Double])])
        : ArrayBuffer[(Double, Array[Double])] = a ++ b
    // TODO: send contiguous matrix X with rows x_i and a y vector
    def vprog(
        iteration: Int,
        id: VertexId,
        vertex: PregelVertex[Array[Double]],
        msg: Option[ArrayBuffer[(Double, Array[Double])]])
      : PregelVertex[Array[Double]] = {
      msg match {
        case Some(msgs) =>
          val triangleSize = latentK * (latentK + 1) / 2
          val tempXtX = new Array[Double](triangleSize)
          val theXy = DoubleMatrix.zeros(latentK)
          for ((y, theXArray) <- msgs) {
            // tempXtX += theX * theX.t
            dspr(theXArray, tempXtX)
            // theXy += theX * y
            SimpleBlas.axpy(y, wrapDoubleArray(theXArray), theXy)
          }

          val fullXtX = fillFullMatrix(latentK, tempXtX)
          // Add regularization
          var i = 0
          while (i < latentK) {
            fullXtX.data(i * latentK + i) += lambda * msgs.length
            i += 1
          }

          val w = Solve.solvePositive(fullXtX, theXy)
          PregelVertex(w.data)
        case None =>
          vertex
      }
    }

    // TODO: write custom bipartite Pregel that partitions edges twice (by source and by dest) to
    // avoid sending duplicate factors from the same user to products in the same partition

    // Double the number of iterations because of the alternating behavior of ALS
    Pregel.runWithCustomReplication(alsGraph, numIter * 2)(replication, vprog, sendMsg, mergeMsg)
  }

  /**
   * Adds theX * theX.t to a matrix (theXtX) in-place.
   *
   * @param theXtX the lower triangular part of the matrix packed in an array (row major)
   */
  private def dspr(theX: Array[Double], theXtX: Array[Double]) {
    val n = theX.length
    var i = 0
    var j = 0
    var idx = 0
    var xi = 0.0
    while (i < n) {
      xi = theX(i)
      j = 0
      while (j <= i) {
        theXtX(idx) += xi * theX(j)
        j += 1
        idx += 1
      }
      i += 1
    }
  }

  /**
   * Wrap a double array in a DoubleMatrix without creating garbage.
   * This is a temporary fix for jblas 1.2.3; it should be safe to move back to the
   * DoubleMatrix(double[]) constructor come jblas 1.2.4.
   */
  private def wrapDoubleArray(v: Array[Double]): DoubleMatrix = {
    new DoubleMatrix(v.length, 1, v: _*)
  }

  /**
   * Given a row-major lower-triangular matrix, compute the full symmetric square
   * matrix that it represents, storing it into destMatrix.
   */
  private def fillFullMatrix(rank: Int, triangularMatrix: Array[Double]): DoubleMatrix = {
    val destMatrix = DoubleMatrix.zeros(rank, rank)
    var i = 0
    var pos = 0
    while (i < rank) {
      var j = 0
      while (j <= i) {
        destMatrix.data(i*rank + j) = triangularMatrix(pos)
        destMatrix.data(j*rank + i) = triangularMatrix(pos)
        pos += 1
        j += 1
      }
      i += 1
    }
    destMatrix
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

  // From http://stackoverflow.com/questions/5147738/supressing-sign-extension-when-upcasting-or-shifting-in-java
  private def compose(hi: Int, lo: Int): Long = (hi.toLong << 32) + unsigned(lo)
  private def unsigned(x: Int): Long = x & 0xFFFFFFFFL
  private def high(x: Long): Int = (x >> 32).toInt
  private def low(x: Long): Int = x.toInt
}
