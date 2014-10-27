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

import java.lang.reflect.{Field, Modifier}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.FieldSerializer

import org.apache.spark._
import org.apache.spark.rdd._

class RDDLineageSerializer[T](k: Kryo) extends com.esotericsoftware.kryo.Serializer[T] {

  private def shouldSerializeField(f: Field) = {
    val m = f.getModifiers
    Modifier.isTransient(m) && !Modifier.isStatic(m) && f.getType != classOf[SparkContext]
  }

  private def getTransientFields(c: Class[_]): Array[Field] =
    if (c == classOf[Object]) Array.empty
    else c.getDeclaredFields.filter(shouldSerializeField) ++ getTransientFields(c.getSuperclass)

  override def write(kryo: Kryo, output: Output, obj: T) {
    val fs = new FieldSerializer[T](kryo, obj.getClass)
    fs.write(kryo, output, obj)
    for (f <- getTransientFields(obj.getClass)) {
      f.setAccessible(true)
      // println("Serializing " + f + " in " + obj)
      kryo.writeClassAndObject(output, f.get(obj))
      f.setAccessible(false)
    }
  }

  override def read(kryo: Kryo, input: Input, c: Class[T]): T = {
    val fs = new FieldSerializer[T](kryo, c)
    val obj = fs.read(kryo, input, c)
    for (f <- getTransientFields(c)) {
      f.setAccessible(true)
      val fVal = kryo.readClassAndObject(input)
      f.set(obj, fVal)
      // println(s"Deserialized $f = $fVal in $obj")
      f.setAccessible(false)
    }

    // If this is an RDD, set the SparkContext
    if (obj.isInstanceOf[RDD[_]]) {
      val scField = classOf[RDD[_]].getDeclaredFields.find(_.getName == "sc").get
      scField.setAccessible(true)
      assert(RDDLineageSerializer.sc != null)
      scField.set(obj, RDDLineageSerializer.sc)
      scField.setAccessible(false)
    }

    obj
  }
}

object RDDLineageSerializer {
  var sc: SparkContext = null
}
