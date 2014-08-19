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

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.math._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object DataflowPagerank extends Logging {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: PageRank <master> <file> <number_of_iterations>")
      System.exit(1)
    }
    val host = args(0)
    val fname = args(1)
    val iters = args(2).toInt
    val partitions = args(3).toInt
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      .set("spark.locality.wait", "100000")
    // val sc = new SparkContext(args(0), "PageRank",
    //   System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    // val sc = new SparkContext(host, "DataflowPagerank(" + fname + ")", conf)
    val sc = new SparkContext(host, "PageRank(" + fname + ")", conf)

    val algo = args(4) match {
      case "naive" => naiveVersion(sc, fname, partitions, iters)
      case "optimized" => optimizedSpark(sc, fname, partitions, iters)
      case "cc" => connectedComponents(sc, fname, partitions)
      case "ccOpt" => ccSlightlyOpt(sc, fname, partitions)
      case _ => throw new UnsupportedOperationException
    }

    sc.stop()
    


    System.exit(0)
  }

  def optimizedSpark(sc: SparkContext, fname: String, partitions: Int, iters: Int) {

    val lines = sc.textFile(fname).repartition(partitions)
    val links: RDD[(Long, Seq[Long])] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.groupByKey().cache()
    var ranks: RDD[(Long, Double)] = links.mapValues(v => 1.0)
    logWarning("Graph loaded")

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      ranks.count
      logWarning(s"Pagerank finished iteration $i")
    }

    // val output = ranks.collect()
    // output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    val totalRank = ranks.map{ case(_, r) => r}.reduce(_ + _)
    logWarning(s"Total Pagerank: $totalRank")
  }

  // For now edge data is arbitrarily a string
  def naiveVersion(sc: SparkContext, fname: String, partitions: Int, iters: Int) {

    val lines = sc.textFile(fname).repartition(partitions)
    val edges: RDD[(Long, (Long, Int))] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, (parts(1).toLong, 1))
    }
    val alpha = 0.15
    val initialRank = 1.0
    logWarning("Pagerank entered.")

    // get outdegree of each each and make weight 1/outdegree
    val weightedEdges: RDD[(Long, (Long, Double))] = edges
      .map { case (src: Long, (dst: Long, _: Int)) => (src, 1.0)}
      // .reduceByKey((v1, v2) => v1 + v2)
      .reduceByKey(_ + _)
      .join(edges)
      .map{ case (src: Long, (outDegree: Double, (dst: Long, _: Int))) =>
            (src, (dst, 1.0/outDegree))
      }.cache()

    // initialize ranks
    var ranks: RDD[(Long, Double)] = edges.map{ case (src: Long, (dst: Long, _: Int)) => (src, initialRank)}
      .union(edges.map{ case (src: Long, (dst: Long, _: Int)) => (dst, initialRank)})
      .distinct()

    logWarning("Starting pagerank iterations")
    for (i <- 1 to iters) {
      ranks = weightedEdges.join(ranks)
        .map {case (src: Long, ((dst: Long, weight: Double), rank: Double)) => (dst, weight*rank)}
        .reduceByKey(_ + _)
        .join(ranks)
        .map { case (id: Long, (incomingRanks: Double, myRank: Double)) => (id, alpha*myRank + (1.0-alpha)*incomingRanks)}

        ranks.count
        logWarning("Finished iteration: " + i) 
    }
    val totalRank = ranks.map{ case(_, r) => r}.reduce(_ + _)
    logWarning(s"Total Pagerank: $totalRank")

  }

  def connectedComponents(sc: SparkContext, fname: String, partitions: Int) {

    val lines = sc.textFile(fname).repartition(partitions)
    val edges: RDD[(Long, Long)] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.cache()
    logWarning("CC started")


    // initialize ccIDs to IDs
    var ccs: RDD[(Long, Long)] = edges.map{ case (src: Long, dst: Long) => (src, src)}
      .union(edges.map{ case (src: Long, dst: Long) => (dst, dst)})
      .distinct()
    var numUpdates = Long.MaxValue

    logWarning("Starting CC iterations")
    var i = 0
    while (numUpdates > 0) {

      val newCCs = edges
        // get src property
        .join(ccs)
        // rekey by dst
        .map {case (src: Long, (dst: Long, srcCC: Long)) => (dst, (src, srcCC))}
        // get dst property
        .join(ccs)
        // emit min ccId to src and adst
        .flatMap { case (dst: Long, ((src: Long, srcCC: Long), dstCC)) =>
            Iterator((src, min(srcCC, dstCC)), (dst, min(srcCC, dstCC)))
        }
        .reduceByKey(min(_, _)).cache()
        // .join(ranks)
        // .map { case (id: Long, (incomingRanks: Double, myRank: Double)) => (id, alpha*myRank + (1.0-alpha)*incomingRanks)}

      // check for convergence
      numUpdates = newCCs.join(ccs)
        .filter{case (vid, (newCC, oldCC)) => newCC != oldCC }.count()

      logWarning(s"CC iter $i with $numUpdates updates") 
      ccs = newCCs
      i += 1

    }
    val numCCs = ccs.map{ case(_, id) => id}.distinct().count()
    logWarning(s"Num connected components: $numCCs")

  }

  def ccSlightlyOpt(sc: SparkContext, fname: String, partitions: Int) {

    val lines = sc.textFile(fname).repartition(partitions)
    val edges: RDD[(Long, Long)] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.cache()
    logWarning("CC started")


    // initialize ccIDs to IDs
    var ccs: RDD[(Long, Long)] = edges.map{ case (src: Long, dst: Long) => (src, src)}
      .union(edges.map{ case (src: Long, dst: Long) => (dst, dst)})
      .distinct()
    var numUpdates = Long.MaxValue

    logWarning("Starting CC iterations")
    var i = 0
    while (numUpdates > 0) {

      val newCCs = edges
        // get src property
        .join(ccs)
        // rekey by dst
        .map {case (src: Long, (dst: Long, srcCC: Long)) => (dst, (src, srcCC))}
        // get dst property
        .join(ccs)
        // emit min ccId to src and adst
        .flatMap { case (dst: Long, ((src: Long, srcCC: Long), dstCC)) =>
            if (srcCC < dstCC)  {
              Iterator((dst, srcCC))
            } else if (dstCC < srcCC) {
              Iterator((src, dstCC))
            } else {
              Iterator.empty
            }
            // Iterator((src, min(srcCC, dstCC)), (dst, min(srcCC, dstCC)))
        }
        .reduceByKey(min(_, _))


      ccs = ccs.join(newCCs).map{ case (vid, (oldCC, newCC)) => (vid, min(oldCC, newCC)) }.cache()
        // .join(ranks)
        // .map { case (id: Long, (incomingRanks: Double, myRank: Double)) => (id, alpha*myRank + (1.0-alpha)*incomingRanks)}

      // check for convergence
      // numUpdates = newCCs.join(ccs)
        // .filter{case (vid, (newCC, oldCC)) => newCC != oldCC }.count()
      numUpdates = newCCs.count()

      logWarning(s"CC iter $i with $numUpdates updates") 
      // ccs = newCCs
      i += 1
    }
    val numCCs = ccs.map{ case(_, id) => id}.distinct().count()
    logWarning(s"Num connected components: $numCCs")
  }
}
