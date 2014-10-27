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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, RDDLineageSerializer}
import org.scalatest.FunSuite

class RDDLineageSerializerSuite extends FunSuite {

  def testSerialization[T: ClassTag](makeRDD: SparkContext => RDD[T]) {
    // Make an RDD in one SparkContext
    val sc1 = new SparkContext("local", "test1", new SparkConf(false))
    val rdd = makeRDD(sc1)
    val expected = rdd.collect.toSet
    sc1.stop()

    // Deserialize it in a different SparkContext
    val sc2 = new SparkContext("local", "test2", new SparkConf(false))
    RDDLineageSerializer.sc = sc2

    // Ensure that RDDLineageSerializer enables it to be successfully computed after deserialization
    val javaSer = new JavaSerializer(new SparkConf())
    val defaultKryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    val lineageKryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.serializer.RDDLineageSerializerRegistrator"))

    for (ser <- List(javaSer, defaultKryoSer, lineageKryoSer); s = ser.newInstance()) {
      val rddSer: RDD[T] = s.deserialize[RDD[T]](s.serialize[RDD[T]](rdd))

      if (ser == lineageKryoSer) {
        assert(rddSer.collect.toSet === expected)
      } else {
        intercept[NullPointerException] {
          rddSer.collect
        }
      }
    }
  }

  test("ParallelCollectionRDD") {
    testSerialization(_.parallelize(List(1, 2, 3)))
  }

  test("MappedRDD") {
    testSerialization(_.parallelize(List(1, 2, 3)).map(_ * 2))
  }

  test("ShuffledRDD") {
    testSerialization(_.parallelize(List(1, 2, 3)).map(x => (x % 2, x)).reduceByKey(_ + _, 2))
  }
}
