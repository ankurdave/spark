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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}
import org.apache.spark.shuffle.sort.SortShuffleManager

private[spark]
class MemoryShuffleBlockManager(blockManager: BlockManager) extends ShuffleBlockManager with Logging {
  def conf = blockManager.conf

  // Are we using sort-based shuffle?
  val sortBasedShuffle =
    conf.get("spark.shuffle.manager", "") == classOf[SortShuffleManager].getName

  /**
   * Contains all the state related to a particular shuffle.
   */
  private class ShuffleState(val numBuckets: Int) {
    /**
     * The BlockIds of all map tasks completed on this Executor for this shuffle.
     */
    val completedMapTasks = new ConcurrentLinkedQueue[ShuffleBlockId]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Register a completed map without getting a ShuffleWriterGroup. Used by sort-based shuffle
   * because it just writes a single file by itself.
   */
  override def addCompletedMap(blockId: ShuffleBlockId, numBuckets: Int): Unit = {
    shuffleStates.putIfAbsent(blockId.shuffleId, new ShuffleState(numBuckets))
    val shuffleState = shuffleStates(blockId.shuffleId)
    shuffleState.completedMapTasks.add(blockId)
  }

  override def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer) = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private val blockIds = Array.tabulate[ShuffleBlockId](numBuckets) { bucketId =>
        ShuffleBlockId(shuffleId, mapId, bucketId)
      }

      override val writers: Array[BlockObjectWriter] = blockIds.map { blockId =>
        // TODO: delete the block from memory if it already exists because of previous failures
        blockManager.getMemoryWriter(blockId, serializer)
      }

      override def releaseWriters(success: Boolean) {
        for (blockId <- blockIds) shuffleState.completedMapTasks.add(blockId, numBuckets)
      }
    }
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        for (block <- state.completedMapTasks) {
          blockManager.removeBlock(block, tellMaster = false)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  def stop() {
    metadataCleaner.cancel()
  }
}
