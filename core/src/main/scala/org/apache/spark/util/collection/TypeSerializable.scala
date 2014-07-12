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
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import org.apache.spark.SparkEnv

trait TypeSerializable[A] {
  def serialize(a: A): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    serializeToStream(a, new DataOutputStream(baos))
    baos.toByteArray()
  }
  def serializeToStream(a: A, s: DataOutputStream): Unit
  def deserialize(b: Array[Byte], off: Int): A = {
    val bais = new DataInputStream(new ByteArrayInputStream(b, off, b.length - off))
    deserializeFromStream(bais)
  }
  def deserializeFromStream(s: DataInputStream): A
}

// TODO: Codegen at compile time? This is sure to be slow because of all the method calls
// Also this approach fails with subtypes - ultimately we should maybe use scala/pickling
object TypeSerializable {
  def useSparkSerializer[A: ClassTag] = new TypeSerializable[A] {
    def serializeToStream(a: A, s: DataOutputStream) {
      SparkEnv.get.serializer.newInstance.serializeStream(s).writeObject(a).close()
    }
    def deserializeFromStream(s: DataInputStream): A = {
      val deserStream = SparkEnv.get.serializer.newInstance.deserializeStream(s)
      val a = deserStream.readObject()
      deserStream.close()
      a
    }
  }
  implicit object IntSerializable extends TypeSerializable[Int] {
    def serializeToStream(a: Int, s: DataOutputStream) { s.writeInt(a) }
    def deserializeFromStream(s: DataInputStream): Int = s.readInt()
  }

  implicit object LongSerializable extends TypeSerializable[Long] {
    def serializeToStream(a: Long, s: DataOutputStream) { s.writeLong(a) }
    def deserializeFromStream(s: DataInputStream): Long = s.readLong()
  }
  implicit object CharSerializable extends TypeSerializable[Char] {
    def serializeToStream(a: Char, s: DataOutputStream) { s.writeChar(a) }
    def deserializeFromStream(s: DataInputStream): Char = s.readChar()
  }

  implicit object FloatSerializable extends TypeSerializable[Float] {
    def serializeToStream(a: Float, s: DataOutputStream) { s.writeFloat(a) }
    def deserializeFromStream(s: DataInputStream): Float = s.readFloat()
  }

  implicit object DoubleSerializable extends TypeSerializable[Double] {
    def serializeToStream(a: Double, s: DataOutputStream) { s.writeDouble(a) }
    def deserializeFromStream(s: DataInputStream): Double = s.readDouble()
  }

  implicit object ShortSerializable extends TypeSerializable[Short] {
    def serializeToStream(a: Short, s: DataOutputStream) { s.writeShort(a) }
    def deserializeFromStream(s: DataInputStream): Short = s.readShort()
  }

  implicit object BooleanSerializable extends TypeSerializable[Boolean] {
    def serializeToStream(a: Boolean, s: DataOutputStream) { s.writeBoolean(a) }
    def deserializeFromStream(s: DataInputStream): Boolean = s.readBoolean()
  }

  implicit object ByteSerializable extends TypeSerializable[Byte] {
    def serializeToStream(a: Byte, s: DataOutputStream) { s.writeByte(a) }
    def deserializeFromStream(s: DataInputStream): Byte = s.readByte()
  }

  implicit def tuple2Serializable[A: TypeSerializable, B: TypeSerializable] = new TypeSerializable[(A, B)] {
    def serializeToStream(pair: (A, B), s: DataOutputStream) {
      implicitly[TypeSerializable[A]].serializeToStream(pair._1, s)
      implicitly[TypeSerializable[B]].serializeToStream(pair._2, s)
    }
    def deserializeFromStream(s: DataInputStream): (A, B) = {
      val a = implicitly[TypeSerializable[A]].deserializeFromStream(s)
      val b = implicitly[TypeSerializable[B]].deserializeFromStream(s)
      (a, b)
    }
  }
}
