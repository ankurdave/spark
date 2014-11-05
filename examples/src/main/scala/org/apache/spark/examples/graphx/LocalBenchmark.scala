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

package org.apache.spark.graphx.impl {
  import org.apache.spark._
  import org.apache.spark.graphx._

  object LocalBenchmarkHelper {

    def time[A](desc: String)(f: => A): A = {
      val start = System.currentTimeMillis
      val result = f
      println(s"$desc: ${System.currentTimeMillis - start} ms")
      result
    }

    def run(sc: SparkContext, path: String, onDisk: Boolean) {
      val builder = EdgePartition.newBuilder[Double, Double](onDisk)
      val edges = time("load edges") {
        scala.io.Source.fromFile(path).getLines.foreach { line =>
          if (!line.isEmpty && line(0) != '#') {
            val lineArray = line.split("\\s+")
            if (lineArray.length < 2) {
              throw new Exception("Invalid line: " + line)
            }
            val srcId = lineArray(0).toLong
            val dstId = lineArray(1).toLong
            builder.add(srcId, dstId, 1.0)
          }
        }
      }

      val edgePartition: EdgePartition[Double, Double] =
        time("finalize edge partition") {
          builder.toEdgePartition
        }
      val tripletPartition: EdgePartition[Double, Double] =
        time("upgrade edge partition") {
          edgePartition.updateVertices(
            (if (onDisk) edgePartition.asInstanceOf[DiskEdgePartition[Double, Double]].global2local
            else edgePartition.asInstanceOf[MemoryEdgePartition[Double, Double]].global2local)
              .iterator.map(kv => (kv._1, 1.0)))
        }

      def sendMsg(ctx: EdgeContext[Double, Double, Double]) {
        ctx.sendToDst(ctx.srcAttr * ctx.attr)
      }
      def mergeMsg(a: Double, b: Double) = a + b

      time("preAgg") {
        tripletPartition.aggregateMessages(sendMsg, mergeMsg, TripletFields.SrcAndEdge,
          (a, b) => true)
      }
    }
  }
}

package org.apache.spark.examples.graphx {
  import org.apache.spark._
  import org.apache.spark.graphx._

  object LocalBenchmark {
    def main(args: Array[String]) {
      val conf = new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .setAppName("LocalBenchmark")
      GraphXUtils.registerKryoClasses(conf)
      val sc = new SparkContext(conf)
      val path = args(0)
      val onDisk = args(1).toBoolean

      org.apache.spark.graphx.impl.LocalBenchmarkHelper.run(sc, path, onDisk)
    }
  }
}
