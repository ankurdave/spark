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

import java.io.DataInputStream
import java.io.DataOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.SizeEstimator
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.ImmutableBitSet
import org.apache.spark.util.collection.ImmutableLongOpenHashSet
import org.apache.spark.util.collection.ImmutableVector
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.TypeSerializable

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

    val numElements = numPartitions * elemsPerPartition

    val conf = new SparkConf()
      .setAppName(s"IndexedRDD Benchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val r = new util.Random(0)

    var start = 0L
    var end = 0L

    println("========== ImmutableVector ==========")
    println("Constructing Array[Long]...")
    val array = new Array[Long](elemsPerPartition)
    println("Constructing ImmutableVector[Long]...")
    val vector = ImmutableVector.fromArray(array)
    println("Constructing ImmutableVector[Long] (serialized)...")
    val sVector = ImmutableVector.fromArray(array, true)
    println(s"Done. Generated ${elemsPerPartition} elements.")

    println(s"Array[Long] is ${SizeEstimator.estimate(array)} bytes.")
    println(s"ImmutableVector[Long] is ${SizeEstimator.estimate(vector)} bytes.")
    println(s"ImmutableVector[Long] (serialized) is ${SizeEstimator.estimate(sVector)} bytes.")

    println("Testing array lookup performance...")
    start = System.nanoTime
    for (i <- 0 until elemsPerPartition) array(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / microTrials / 1000000} ms per lookup.")

    println("Testing vector lookup performance...")
    start = System.nanoTime
    for (i <- 0 until elemsPerPartition) vector(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / microTrials / 1000000} ms per lookup.")

    println("Testing serialized vector lookup performance...")
    start = System.nanoTime
    for (i <- 0 until elemsPerPartition) sVector(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / microTrials / 1000000} ms per lookup.")

    println("Testing array scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      array.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Testing vector scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      vector.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Testing serialized vector scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      sVector.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Constructing Array[Object]...")
    class A(var x: Int)
    val objArray = Array.fill(elemsPerPartition)(new A(1))
    println("Constructing ImmutableVector[Object]...")
    val objVector = ImmutableVector.fromArray(objArray)
    println("Constructing ImmutableVector[Object] (serialized)...")
    implicit val aSerializer = new TypeSerializable[A] {
      def serializeToStream(a: A, s: DataOutputStream) { s.writeInt(a.x) }
      def deserializeFromStream(s: DataInputStream): A = new A(s.readInt())
    }
    val sObjVector = ImmutableVector.fromArray(objArray, true)
    println(s"Done. Generated ${elemsPerPartition} elements.")

    println(s"Array[Object] is ${SizeEstimator.estimate(objArray)} bytes.")
    println(s"ImmutableVector[Object] is ${SizeEstimator.estimate(objVector)} bytes.")
    println(s"ImmutableVector[Object] (serialized) is ${SizeEstimator.estimate(sObjVector)} bytes.")

    println("Testing array scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      objArray.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Testing vector scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      objVector.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Testing serialized vector scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      sObjVector.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("========== ImmutableBitSet ==========")
    println("Constructing BitSet...")
    val bs = new BitSet(elemsPerPartition)
    for (i <- 0L until 100) bs.set(r.nextInt(elemsPerPartition))
    println("Constructing ImmutableBitSet...")
    val ibs = bs.toImmutableBitSet
    println(s"Done. Generated bitsets with capacity ${elemsPerPartition} and 100 set elements.")

    println(s"BitSet is ${SizeEstimator.estimate(bs)} bytes.")
    println(s"ImmutableBitSet is ${SizeEstimator.estimate(ibs)} bytes.")

    println("Testing bitset lookup performance...")
    start = System.nanoTime
    for (i <- 0 until elemsPerPartition) bs.get(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / microTrials / 1000000} ms per lookup.")

    println("Testing immutable bitset lookup performance...")
    start = System.nanoTime
    for (i <- 0 until elemsPerPartition) ibs.get(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / microTrials / 1000000} ms per lookup.")

    println("Testing bitset scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      bs.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("Testing immutable bitset scan performance...")
    start = System.nanoTime
    for (t <- 1 to trials) {
      ibs.iterator.foreach(x => {})
    }
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / trials / 1000000} ms per scan.")

    println("========== ImmutableLongOpenHashSet ==========")
    println("Constructing OpenHashSet...")
    val ohs = new OpenHashSet[Long]
    for (i <- 0L until elemsPerPartition) ohs.add(i)
    println("Constructing ImmutableLongOpenHashSet...")
    var ilohs = ImmutableLongOpenHashSet.fromLongOpenHashSet(ohs)
    println(s"Done. Generated ${elemsPerPartition} elements.")

    println(s"OpenHashSet is ${SizeEstimator.estimate(ohs)} bytes.")
    println(s"ImmutableLongOpenHashSet is ${SizeEstimator.estimate(ilohs)} bytes.")

    println("Testing OpenHashSet lookup performance (OpenHashSet.getPos)...")
    start = System.nanoTime
    for (i <- 0L until elemsPerPartition) ohs.getPos(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / elemsPerPartition / 1000000} ms per lookup.")

    println("Testing ImmutableLongOpenHashSet lookup performance (ILOHS.getPos)...")
    start = System.nanoTime
    for (i <- 0L until elemsPerPartition) ilohs.getPos(i)
    end = System.nanoTime
    println(s"Done. ${(end - start).toDouble / elemsPerPartition / 1000000} ms per lookup.")

    println("========== IndexedRDD ==========")
    println("Constructing vanilla RDD...")
    val vanilla = sc.parallelize(0 until numPartitions, numPartitions).flatMap(p =>
      (p * elemsPerPartition) until ((p + 1) * elemsPerPartition))
      .map(x => (x.toLong, x)).cache()
    println("Constructing indexed RDD...")
    val indexed = IndexedRDD(vanilla).cache()
    println(s"Done. Generated ${vanilla.count}, ${indexed.count} elements.")

    println(s"Scanning vanilla RDD with mapValues ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = vanilla.mapValues(_ * 2)
      doubled.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")

    println(s"Scanning vanilla RDD with mapValues and count ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = vanilla.mapValues(_ * 2)
      doubled.count()
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")

    println(s"Scanning indexed RDD with mapValues ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = indexed.mapValues(_ * 2)
      doubled.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")

    println(s"Scanning indexed RDD with mapValues and count ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val doubled = indexed.mapValues(_ * 2)
      doubled.count()
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per scan.")

    println("Constructing modified version of vanilla RDD...")
    val vanilla2 = vanilla.mapValues(_ * 2).cache()
    vanilla2.foreach(x => {})
    println("Constructing modified version of indexed RDD...")
    val indexed2 = indexed.mapValues(_ * 2).cache()
    indexed2.foreach(x => {})
    println(s"Done.")

    println(s"Zipping vanilla RDD with modified version ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val zipped = vanilla.zip(vanilla2).map(ab => (ab._1._1, ab._1._2 + ab._2._2))
      zipped.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per zip.")

    println(s"Zipping vanilla RDD with modified version and count ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val zipped = vanilla.zip(vanilla2).map(ab => (ab._1._1, ab._1._2 + ab._2._2))
      zipped.count()
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per zip.")

    // println(s"Joining vanilla RDD with modified version ($trials trials)...")
    // start = System.currentTimeMillis
    // for (i <- 1 to trials) {
    //   val joined = vanilla.join(vanilla2)
    //   joined.foreach(x => {})
    // }
    // end = System.currentTimeMillis
    // println(s"Done. ${(end - start) / trials} ms per join.")

    println(s"Joining indexed RDD with modified version ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val joined = indexed.innerJoin(indexed2) { (id, a, b) => a + b }
      joined.foreach(x => {})
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per join.")

    println(s"Joining indexed RDD with modified version and count ($trials trials)...")
    start = System.currentTimeMillis
    for (i <- 1 to trials) {
      val joined = indexed.innerJoin(indexed2) { (id, a, b) => a + b }
      joined.count()
    }
    end = System.currentTimeMillis
    println(s"Done. ${(end - start) / trials} ms per join.")

    vanilla.unpersist()
    vanilla2.unpersist()
    indexed.unpersist()
    indexed2.unpersist()

    println("========== IndexedRDDPartition ==========")
    println(s"Testing scaling for IndexedRDDPartition.get ($microTrials trials)...")
    println("partition size\tget time (ms)")
    for (n <- 1 to elemsPerPartition by elemsPerPartition / 100) {
      val partition = IndexedRDDPartition((0 until n).iterator.map(x => (x.toLong, x)))
      start = System.nanoTime
      for (i <- 1 to microTrials) {
        val elem = r.nextInt(n)
        assert(partition.multiget(Array(elem)).get(elem) == Some(elem))
      }
      end = System.nanoTime
      println(s"$n\t${(end - start).toDouble / microTrials / 1000000}")
    }
    println("Done.")

    println(s"Testing scaling for IndexedRDDPartition.put - update ($microTrials trials)...")
    println("partition size\tupdate time (ms)")
    for (n <- 1 to elemsPerPartition by elemsPerPartition / 100) {
      val partition = IndexedRDDPartition((0 until n).iterator.map(x => (x.toLong, x)))
      start = System.nanoTime
      for (i <- 1 to microTrials) {
        val elem = r.nextInt(n)
        partition.multiput(Array(elem.toLong -> 0), (id, a, b) => b)
      }
      end = System.nanoTime
      println(s"$n\t${(end - start).toDouble / microTrials / 1000000}")
    }
    println("Done.")

    println(s"Testing scaling for IndexedRDDPartition.put - insert ($miniTrials trials)...")
    println("partition size\tinsert time (ms)")
    for (n <- 1 to elemsPerPartition by elemsPerPartition / 100) {
      val partition = IndexedRDDPartition((0 until n).iterator.map(x => (x.toLong, x)))
      start = System.nanoTime
      for (i <- 1 to miniTrials) {
        val elem = r.nextInt(n).toLong + n
        partition.multiput(Array(elem -> 0), (id, a, b) => b)
      }
      end = System.nanoTime
      println(s"$n\t${(end - start).toDouble / miniTrials / 1000000}")
    }
    println("Done.")

    println(s"Testing scaling for IndexedRDDPartition.delete ($microTrials trials)...")
    println("partition size\tdelete time (ms)")
    for (n <- 1 to elemsPerPartition by elemsPerPartition / 100) {
      val partition = IndexedRDDPartition((0 until n).iterator.map(x => (x.toLong, x)))
      start = System.nanoTime
      for (i <- 1 to microTrials) {
        val elem = r.nextInt(n).toLong
        partition.delete(Array(elem))
      }
      end = System.nanoTime
      println(s"$n\t${(end - start).toDouble / microTrials / 1000000}")
    }
    println("Done.")

    println(s"Testing varying read-write mixtures ($microTrials trials)...")
    println("write %\tworkload time (ms)")
    for (writeProb <- 0 to 100) {
      var partition = IndexedRDDPartition((0 until elemsPerPartition).iterator.map(x => (x.toLong, x)))
      start = System.nanoTime
      var numWrites = 0
      for (i <- 1 to microTrials) {
        val elem = r.nextInt(elemsPerPartition)
        val isWrite = r.nextInt(100) < writeProb
        if (isWrite) {
          partition = partition.multiput(Array(elem.toLong -> 0), (id, a, b) => b)
          numWrites += 1
        } else {
          val read = partition.multiget(Array(elem)).get(elem).get
          assert(read == elem || read == 0)
        }
      }
      end = System.nanoTime
      println(s"${numWrites * 100.0 / microTrials}\t${(end - start).toDouble / 1000000}")
    }
    println("Done.")

    println(s"Testing scaling for IndexedRDDPartition creation ($trials trials)...")
    println("partition size\tcreate time (ms)")
    var n = 1
    while (n <= elemsPerPartition) {
      start = System.nanoTime
      for (i <- 1 to trials) {
        val partition = IndexedRDDPartition((0 until n).iterator.map(x => (x.toLong, x)))
      }
      end = System.nanoTime
      println(s"$n\t${(end - start).toDouble / trials / 1000000}")
      n *= 10
    }
    println("Done.")

    sc.stop()
  }
}
