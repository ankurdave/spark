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

package org.apache.spark.serializer

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.HttpBroadcast
import org.apache.spark.network.nio.{PutBlock, GotBlock, GetBlock}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage._
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.collection.CompactBuffer

import scala.reflect.ClassTag

private[spark] class RDDLineageSerializerRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    val ser = new FieldSerializer[RDD[_]](kryo, classOf[RDD[_]])
    ser.removeField("sc")
    kryo.register(classOf[RDD[_]], ser)
  }
}
