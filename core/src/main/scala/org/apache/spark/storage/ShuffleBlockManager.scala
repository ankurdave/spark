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

package org.apache.spark.storage

/** A group of writers for a ShuffleMapTask, one writer per reducer. */
private[spark]
trait ShuffleWriterGroup {
  val writers: Array[BlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}


private[spark]
trait ShuffleBlockManager extends Logging {
  type ShuffleId = Int

  /**
   * Register a completed map output for cleanup.
   */
  def addCompletedMap(block: ShuffleBlockId, numBuckets: Int): Unit

  /**
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
   */
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer): ShuffleWriterGroup

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean

  def stop(): Unit
}
