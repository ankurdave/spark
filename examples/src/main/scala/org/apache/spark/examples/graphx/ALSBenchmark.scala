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

package org.apache.spark.examples.graphx

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating

object ALSBenchmark {

  /**
   * To run this program use the following:
   *
   * MASTER=spark://foobar bin/run-example graphx.SynthBenchmark -app=pagerank
   *
   * Options:
   *   -app "pagerank" or "cc" for pagerank or connected components. (Default: pagerank)
   *   -niters the number of iterations of pagerank to use (Default: 10)
   *   -numVertices the number of vertices in the graph (Default: 1000000)
   *   -numEPart the number of edge partitions in the graph (Default: number of cores)
   *   -partStrategy the graph partitioning strategy to use
   *   -mu the mean parameter for the log-normal graph (Default: 4.0)
   *   -sigma the stdev parameter for the log-normal graph (Default: 1.3)
   *   -degFile the local file to save the degree information (Default: Empty)
   */
  def main(args: Array[String]) {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    val conf = new SparkConf()
      .setAppName(s"GraphX ALS Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(conf)

    var niter = 10
    var numUsers = 1000000
    var numProducts = 1000000
    var numRatings = 1000000
    var numEPart = sc.defaultParallelism
    var rank = 100

    options.foreach {
      case ("niter", v) => niter = v.toInt
      case ("numUsers", v) => numUsers = v.toInt
      case ("numProducts", v) => numProducts = v.toInt
      case ("numRatings", v) => numRatings = v.toInt
      case ("numEPart", v) => numEPart = v.toInt
      case ("rank", v) => rank = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    // Generate the ratings
    val ratingsPerPartition = numRatings / numEPart
    val ratings = sc.parallelize(0 until numEPart, numEPart).flatMap { i =>
      val r = new util.Random(i)
      Iterator.fill(ratingsPerPartition) {
        Rating(r.nextInt(numUsers), r.nextInt(numProducts), r.nextDouble())
      }
    }.cache()
    val sampleRatings = ratings.take(2).toList
    val testUser = sampleRatings(0).user
    val testProduct = sampleRatings(1).product
    println(s"Created ${ratings.count} ratings: $sampleRatings")

    var startTime = 0L

    // Run MLlib ALS
    startTime = System.currentTimeMillis
    val mllibModel =
      org.apache.spark.mllib.recommendation.ALS.train(ratings, rank, niter, 0.01, numEPart)
    println(s"MLlib predicted rating for ($testUser, $testProduct): ${mllibModel.predict(testUser, testProduct)}")
    println(s"MLlib ran in ${System.currentTimeMillis - startTime} ms")

    // Run GraphX ALS
    startTime = System.currentTimeMillis
    val graphxModel =
      org.apache.spark.graphx.lib.ALS.train(ratings, rank, niter, 0.01)
    println(s"GraphX predicted rating for ($testUser, $testProduct): ${graphxModel.predict(testUser, testProduct)}")
    println(s"GraphX ran in ${System.currentTimeMillis - startTime} ms")

    sc.stop()
  }
}
