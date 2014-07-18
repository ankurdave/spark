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

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.graphx.Pregel._

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
 *
 * The first implementation uses the [[Pregel]] interface and runs PageRank for a fixed number
 * of iterations:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the standalone [[Graph]] interface and runs PageRank until
 * convergence:
 *
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 0.0 )
 * while( max(abs(PR - oldPr)) > tol ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
 *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
 * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * Note that this is not the "normalized" PageRank and as a consequence pages that have no
 * inlinks will have a PageRank of alpha.
 */
object PageRank extends Logging {

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
  {

    // Initialize the pagerankGraph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => resetProb )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(iter: Int, id: VertexId, oldV: PregelVertex[Double],
                      msgSum: Option[Double]) = {
      PregelVertex(resetProb + (1.0 - resetProb) * msgSum.getOrElse(0.0))
    }

    def sendMessage(iter: Int, edge: EdgeTriplet[PregelVertex[Double], Double]) = {
      Iterator((edge.dstId, edge.srcAttr.attr * edge.attr))
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // Execute pregel for a fixed number of iterations.
    val prGraph = Pregel.run(pagerankGraph, numIter, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner).cache()
    val normalizer: Double = prGraph.vertices.map(x => x._2).reduce(_ + _)
    prGraph.mapVertices((id, pr) => pr / normalizer)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (currentPr, deltaToSend)
      .mapVertices( (id, attr) => (resetProb, (1.0 - resetProb) * resetProb) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(iter: Int, id: VertexId, vertex: PregelVertex[(Double, Double)],
                      msgSum: Option[Double]) = {
      var (oldPR, pendingDelta) = vertex.attr
      val newPR = oldPR + msgSum.getOrElse(0.0)
      // if we were active then we sent the pending delta on the last iteration
      if (vertex.isActive) {
        pendingDelta = 0.0
      }
      pendingDelta += (1.0 - resetProb) * msgSum.getOrElse(0.0)
      val isActive = math.abs(pendingDelta) >= tol
      PregelVertex((newPR, pendingDelta), isActive)
    }

    def sendMessage(iter: Int, edge: EdgeTriplet[PregelVertex[(Double, Double)], Double]) = {
      val PregelVertex((srcPr, srcDelta), srcIsActive) = edge.srcAttr
      assert(srcIsActive)
      Iterator((edge.dstId, srcDelta * edge.attr))
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // Execute a dynamic version of Pregel.
    val prGraph = Pregel.run(pagerankGraph, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
      .cache()
    val normalizer: Double = prGraph.vertices.map(x => x._2).reduce(_ + _)
    prGraph.mapVertices((id, pr) => pr / normalizer)
  } // end of deltaPageRank
}
