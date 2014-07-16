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

package org.apache.spark.util.collection

import scala.reflect.ClassTag

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

import org.apache.spark.SparkEnv

trait TypeSerializable[A] {
  def serialize(a: A): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    serializeToStream(a, new DataOutputStream(baos))
    baos.toByteArray()
  }
  def serializeToStream(a: A, s: DataOutputStream): Unit
  def deserialize(b: ByteBuffer): A
}

// TODO: Codegen at compile time? This is sure to be slow because of all the method calls
// Also this approach fails with subtypes - ultimately we should maybe use scala/pickling
object TypeSerializable {
  def useSparkSerializer[A: ClassTag] = new TypeSerializable[A] {
    def serializeToStream(a: A, s: DataOutputStream) {
      SparkEnv.get.serializer.newInstance.serializeStream(s).writeObject(a).close()
    }
    def deserialize(b: ByteBuffer): A = {
      SparkEnv.get.serializer.newInstance.deserialize(b)
    }
  }
  implicit object IntSerializable extends TypeSerializable[Int] {
    def serializeToStream(a: Int, s: DataOutputStream) { s.writeInt(a) }
    def deserialize(b: ByteBuffer): Int = b.getInt()
  }

  implicit object LongSerializable extends TypeSerializable[Long] {
    def serializeToStream(a: Long, s: DataOutputStream) { s.writeLong(a) }
    def deserialize(b: ByteBuffer): Long = b.getLong()
  }

  implicit object CharSerializable extends TypeSerializable[Char] {
    def serializeToStream(a: Char, s: DataOutputStream) { s.writeChar(a) }
    def deserialize(b: ByteBuffer): Char = b.getChar()
  }

  implicit object FloatSerializable extends TypeSerializable[Float] {
    def serializeToStream(a: Float, s: DataOutputStream) { s.writeFloat(a) }
    def deserialize(b: ByteBuffer): Float = b.getFloat()
  }

  implicit object DoubleSerializable extends TypeSerializable[Double] {
    def serializeToStream(a: Double, s: DataOutputStream) { s.writeDouble(a) }
    def deserialize(b: ByteBuffer): Double = b.getDouble()
  }

  implicit object ShortSerializable extends TypeSerializable[Short] {
    def serializeToStream(a: Short, s: DataOutputStream) { s.writeShort(a) }
    def deserialize(b: ByteBuffer): Short = b.getShort()
  }

  implicit object BooleanSerializable extends TypeSerializable[Boolean] {
    def serializeToStream(a: Boolean, s: DataOutputStream) { s.writeBoolean(a) }
    def deserialize(b: ByteBuffer): Boolean = b.get() != 0
  }

  implicit object ByteSerializable extends TypeSerializable[Byte] {
    def serializeToStream(a: Byte, s: DataOutputStream) { s.writeByte(a) }
    def deserialize(b: ByteBuffer): Byte = b.get()
  }

  implicit def tuple2Serializable[A: TypeSerializable, B: TypeSerializable] = new TypeSerializable[(A, B)] {
    def serializeToStream(pair: (A, B), s: DataOutputStream) {
      implicitly[TypeSerializable[A]].serializeToStream(pair._1, s)
      implicitly[TypeSerializable[B]].serializeToStream(pair._2, s)
    }
    def deserialize(bb: ByteBuffer): (A, B) = {
      val a = implicitly[TypeSerializable[A]].deserialize(bb)
      val b = implicitly[TypeSerializable[B]].deserialize(bb)
      (a, b)
    }
  }
}
