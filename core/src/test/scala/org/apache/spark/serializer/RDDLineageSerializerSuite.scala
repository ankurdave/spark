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

import org.apache.spark._
import org.scalatest.FunSuite

class RDDLineageSerializerSuite extends FunSuite with SharedSparkContext {

  def testSerialization[T: ClassTag](rdd: RDD[T]) {
    val expected = rdd.collect.toSet

    val javaSer = new JavaSerializer(new SparkConf())
    val defaultKryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val lineageKryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.serializer.RDDLineageSerializerRegistrator")

    for (ser <- List(javaSer, defaultKryoSer, lineageKryoSer); s = ser.newInstance()) {
      val rddSer: RDD[T] = s.deserialize(s.serialize(rdd))

      if (ser == lineageKryoSer) {
        assert(rddSer.collect.toSet === expected)
      } else {
        intercept[NullPointerException] {
          rddSer.collect
        }
      }
  }

  test("ParallelCollectionRDD") {
    testSerialization(sc.parallelize(List(1, 2, 3)))
  }
}
