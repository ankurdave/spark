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

package org.apache.spark.examples

import scala.language.higherKinds

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.IndexedRDD.Id
import org.apache.spark.util.Utils

object IndexedRDDBenchmark {

  def main(args: Array[String]) {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var numPartitions = 8
    var elemsPerPartition = 1000000
    var trials = 10
    var miniTrials = 1000
    var microTrials = 1000000

    options.foreach {
      case ("numPartitions", v) => numPartitions = v.toInt
      case ("elemsPerPartition", v) => elemsPerPartition = v.toInt
      case ("trials", v) => trials = v.toInt
      case ("miniTrials", v) => miniTrials = v.toInt
      case ("microTrials", v) => microTrials = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    val conf = new SparkConf()
      .setAppName(s"IndexedRDD Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    LogManager.getLogger(classOf[ImmutableHashIndexedRDDPartition[_]]).setLevel(Level.ERROR)

    test[PatriciaTreeIndexedRDDPartition, PatriciaTreeIndexedRDD](
      sc, "PatriciaTreeIndexedRDD", numPartitions, elemsPerPartition, trials, miniTrials,
      microTrials, rdd => PatriciaTreeIndexedRDD(rdd))

    test[ImmutableHashIndexedRDDPartition, ImmutableHashIndexedRDD](
      sc, "ImmutableHashIndexedRDD", numPartitions, elemsPerPartition, trials, miniTrials,
      microTrials, rdd => ImmutableHashIndexedRDD(rdd))

    sc.stop()
  }

  private def time(label: String, trials: Int)(f: => Unit) {
    val time = Utils.timeIt(trials)(f)
    println(f"$label%-30s |\t ${time / trials}%d ms")
  }

  def test[
    P[X] <: IndexedRDDPartition[X, P],
    IndexedRDDType[X] <: IndexedRDD[X, P, IndexedRDDType]](
    sc: SparkContext,
    name: String,
    numPartitions: Int,
    elemsPerPartition: Int,
    trials: Int,
    miniTrials: Int,
    microTrials: Int,
    create: RDD[(Id, Long)] => IndexedRDDType[Long]) {

    val r = new util.Random(0)

    var start = 0L
    var end = 0L

    val vanilla = sc.parallelize(0 until numPartitions, numPartitions).flatMap(p =>
      (p * elemsPerPartition) until ((p + 1) * elemsPerPartition))
      .map(x => (x.toLong, x.toLong))
    val indexed = create(vanilla).cache()
    println(s"$name (n=${indexed.count()})")
    println(s"---")

    val existingKeysSmall = Array.fill(numPartitions) {
      r.nextInt(numPartitions * elemsPerPartition).toLong
    }

    val subsetRDD = sc.parallelize(0 until numPartitions, numPartitions).flatMap { p =>
      val r = new util.Random(p)
      Array.fill(elemsPerPartition / numPartitions) {
        (r.nextInt(numPartitions * elemsPerPartition).toLong, 1L)
      }
    }.partitionBy(indexed.partitioner.get).cache()
    subsetRDD.count()

    val incomparableRDD = sc.parallelize(0 until numPartitions, numPartitions).flatMap { p =>
      val r = new util.Random(p)
      Array.fill(elemsPerPartition / numPartitions) {
        (r.nextInt(numPartitions * elemsPerPartition * 2).toLong, 1L)
      }
    }.partitionBy(indexed.partitioner.get).cache()
    incomparableRDD.count()

    val subsetIndexed = create(subsetRDD).cache()
    subsetIndexed.count()

    val incomparableIndexed = create(incomparableRDD).cache()
    incomparableIndexed.count()

    time("foreach", trials) {
      indexed.foreach(x => {})
    }

    time("count", trials) {
      indexed.count()
    }

    time(s"multiget ${existingKeysSmall.size}", trials) {
      indexed.multiget(existingKeysSmall)
    }

    time(s"delete ${existingKeysSmall.size}", trials) {
      indexed.delete(existingKeysSmall).count()
    }

    time("filter 0.1%", trials) {
      indexed.filter(kv => kv._1 % 1000 == 0).count()
    }
    time("filter 1%", trials) {
      indexed.filter(kv => kv._1 % 100 == 0).count()
    }
    time("filter 10%", trials) {
      indexed.filter(kv => kv._1 % 10 == 0).count()
    }

    time("mapValues", trials) {
      indexed.mapValues(_ * 2).count()
    }

    time("diff - subset", trials) {
      indexed.diff(subsetIndexed).count()
    }
    time("diff - no subset", trials) {
      indexed.diff(incomparableIndexed).count()
    }

    time("fullOuterJoin - subset", trials) {
      indexed.fullOuterJoin(subsetIndexed) {
        (id, aOpt, bOpt) => aOpt.getOrElse(0L) + bOpt.getOrElse(0L)
      }.count()
    }
    time("fullOuterJoin - no subset", trials) {
      indexed.fullOuterJoin(incomparableIndexed) {
        (id, aOpt, bOpt) => aOpt.getOrElse(0L) + bOpt.getOrElse(0L)
      }.count()
    }

    time("join - subset, rdd", trials) {
      indexed.join(subsetRDD) { (id, a, b) => a + b }.count()
    }
    time("join - no subset, rdd", trials) {
      indexed.join(incomparableRDD) { (id, a, b) => a + b }.count()
    }
    time("join - subset, indexed", trials) {
      indexed.join(subsetIndexed) { (id, a, b) => a + b }.count()
    }
    time("join - no subset, indexed", trials) {
      indexed.join(incomparableIndexed) { (id, a, b) => a + b }.count()
    }

    time("leftJoin - subset, rdd", trials) {
      indexed.leftJoin(subsetRDD) { (id, a, bOpt) => a + bOpt.getOrElse(0L) }.count()
    }
    time("leftJoin - no subset, rdd", trials) {
      indexed.leftJoin(incomparableRDD) { (id, a, bOpt) => a + bOpt.getOrElse(0L) }.count()
    }
    time("leftJoin - subset, indexed", trials) {
      indexed.leftJoin(subsetIndexed) { (id, a, bOpt) => a + bOpt.getOrElse(0L) }.count()
    }
    time("leftJoin - no subset, indexed", trials) {
      indexed.leftJoin(incomparableIndexed) { (id, a, bOpt) => a + bOpt.getOrElse(0L) }.count()
    }

    time("innerJoin - subset, rdd", trials) {
      indexed.innerJoin(subsetRDD) { (id, a, b) => a + b }.count()
    }
    time("innerJoin - no subset, rdd", trials) {
      indexed.innerJoin(incomparableRDD) { (id, a, b) => a + b }.count()
    }
    time("innerJoin - subset, indexed", trials) {
      indexed.innerJoin(subsetIndexed) { (id, a, b) => a + b }.count()
    }
    time("innerJoin - no subset, indexed", trials) {
      indexed.innerJoin(incomparableIndexed) { (id, a, b) => a + b }.count()
    }

    time("multiput - subset", trials) {
      indexed.multiput(subsetRDD, (id: Id, a: Long, b: Long) => a + b).count()
    }
    time("multiput - no subset", trials) {
      indexed.multiput(incomparableRDD, (id: Id, a: Long, b: Long) => a + b).count()
    }

    indexed.unpersist()
    subsetRDD.unpersist()
    incomparableRDD.unpersist()
    subsetIndexed.unpersist()
    incomparableIndexed.unpersist()
  }
}
